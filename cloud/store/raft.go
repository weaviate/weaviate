//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	gproto "google.golang.org/protobuf/proto"
)

const (
	// tcpMaxPool controls how many connections we will pool
	tcpMaxPool = 3

	// tcpTimeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	tcpTimeout = 10 * time.Second

	raftDBName         = "raft.db"
	logCacheCapacity   = 512 // TODO set higher
	nRetainedSnapShots = 1
)

/// TODO: make loadDB an interface

// Open opens this store and marked as such.
func (st *Store) Open(ctx context.Context) (err error) {
	st.log.Info("bootstrapping started")
	if st.open.Load() { // store already opened
		return nil
	}
	defer func() { st.open.Store(err == nil) }()

	if err = os.MkdirAll(st.raftDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", st.raftDir, err)
	}

	// log store
	st.logStore, err = raftbolt.NewBoltStore(filepath.Join(st.raftDir, raftDBName))
	if err != nil {
		return fmt.Errorf("raft: bolt db: %w", err)
	}

	// log cache
	logCache, err := raft.NewLogCache(logCacheCapacity, st.logStore)
	if err != nil {
		return fmt.Errorf("raft: log cache: %w", err)
	}
	// file snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(st.raftDir, nRetainedSnapShots, os.Stdout)
	if err != nil {
		return fmt.Errorf("raft: file snapshot store: %w", err)
	}

	// tcp transport
	address := fmt.Sprintf("%s:%d", st.host, st.raftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return fmt.Errorf("net.ResolveTCPAddr address=%v: %w", address, err)
	}

	transport, err := raft.NewTCPTransport(address, tcpAddr, tcpMaxPool, tcpTimeout, os.Stdout)
	if err != nil {
		return fmt.Errorf("raft.NewTCPTransport  address=%v tcpAddress=%v maxPool=%v timeOut=%v: %w", address, tcpAddr, tcpMaxPool, tcpTimeout, err)
	}

	st.log.Info("raft tcp transport", "address", address, "tcpMaxPool", tcpMaxPool, "tcpTimeout", tcpTimeout)

	rLog := rLog{st.logStore}
	st.initialLastAppliedIndex, err = rLog.LastAppliedCommand()
	if err != nil {
		return fmt.Errorf("read log last command: %w", err)
	}

	if st.initialLastAppliedIndex == snapshotIndex(snapshotStore) {
		st.loadDatabase(ctx)
	}

	// raft node
	st.raft, err = raft.NewRaft(st.configureRaft(), st, logCache, st.logStore, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("raft.NewRaft %v %w", address, err)
	}

	st.log.Info("starting raft", "applied_index", st.raft.AppliedIndex(), "last_index",
		st.raft.LastIndex(), "last_log_index", st.initialLastAppliedIndex)

	go func() {
		lastLeader := "Unknown"
		t := time.NewTicker(time.Second * 5)
		defer t.Stop()
		for range t.C {
			leader := st.Leader()
			if leader != lastLeader {
				lastLeader = leader
				st.log.Info("current Leader", "address", lastLeader)
			}
		}
	}()

	return nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (st *Store) Apply(l *raft.Log) interface{} {
	st.log.Debug("apply command", "type", l.Type, "index", l.Index)
	if l.Type != raft.LogCommand {
		st.log.Info("not a valid command", "type", l.Type, "index", l.Index)
		return nil
	}
	ret := Response{}
	cmd := command.ApplyRequest{}

	if err := gproto.Unmarshal(l.Data, &cmd); err != nil {
		st.log.Error("decode command: ", "error", err.Error())
		panic("error proto unmarshalling log data")
	}
	schemaOnly := l.Index <= st.initialLastAppliedIndex
	defer func() {
		if l.Index >= st.initialLastAppliedIndex {
			st.loadDatabase(context.Background())
		}
	}()
	switch cmd.Type {
	case command.ApplyRequest_TYPE_ADD_CLASS, command.ApplyRequest_TYPE_RESTORE_CLASS:
		req := command.AddClassRequest{}
		if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
			return Response{Error: fmt.Errorf("decode sub command: %w", err)}
		}
		if req.State == nil {
			return fmt.Errorf("nil sharding state")
		}
		if err := st.parser.ParseClass(req.Class); err != nil {
			return Response{Error: err}
		}
		req.State.SetLocalName(st.nodeID)
		if ret.Error = st.schema.addClass(req.Class, req.State); ret.Error == nil && !schemaOnly {
			st.db.AddClass(req)
		}
	case command.ApplyRequest_TYPE_UPDATE_CLASS:
		req := command.UpdateClassRequest{}
		if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
			return Response{Error: err}
		}
		if req.State != nil {
			req.State.SetLocalName(st.nodeID)
		}
		if err := st.parser.ParseClass(req.Class); err != nil {
			return Response{Error: err}
		}
		if ret.Error = st.schema.updateClass(req.Class, req.State); ret.Error == nil && !schemaOnly {
			st.db.UpdateClass(req)
		}
	case command.ApplyRequest_TYPE_DELETE_CLASS:
		st.schema.deleteClass(cmd.Class)
		if !schemaOnly {
			st.db.DeleteClass(cmd.Class)
		}
	case command.ApplyRequest_TYPE_ADD_PROPERTY:
		req := command.AddPropertyRequest{}
		if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
			return Response{Error: err}
		}
		if req.Property == nil {
			return Response{Error: fmt.Errorf("nil property")}
		}
		if ret.Error = st.schema.addProperty(cmd.Class, *req.Property); ret.Error == nil && !schemaOnly {
			st.db.AddProperty(cmd.Class, req)
		}

	case command.ApplyRequest_TYPE_UPDATE_SHARD_STATUS:
		req := command.UpdateShardStatusRequest{}
		if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
			return Response{Error: err}
		}
		if !schemaOnly {
			ret.Error = st.db.UpdateShardStatus(&req)
		}

	case command.ApplyRequest_TYPE_ADD_TENANT:
		req := &command.AddTenantsRequest{}
		if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
			return Response{Error: err}
		}
		if ret.Error = st.schema.addTenants(cmd.Class, req); ret.Error == nil && !schemaOnly {
			st.db.AddTenants(cmd.Class, req)
		}
	case command.ApplyRequest_TYPE_UPDATE_TENANT:
		req := &command.UpdateTenantsRequest{}
		if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
			return Response{Error: err}
		}
		ret.Data, ret.Error = st.schema.updateTenants(cmd.Class, req)
		if ret.Error == nil && !schemaOnly {
			st.db.UpdateTenants(cmd.Class, req)
		}
	case command.ApplyRequest_TYPE_DELETE_TENANT:
		req := &command.DeleteTenantsRequest{}
		if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
			return Response{Error: err}
		}
		if err := st.schema.deleteTenants(cmd.Class, req); err != nil {
			st.log.Error("delete tenants from class", "class", cmd.Class, "error", err)
		}
		if !schemaOnly {
			st.db.DeleteTenants(cmd.Class, req)
		}

	default:
		st.log.Error("unknown command", "type", cmd.Type, "class", cmd.Class)
	}

	return ret
}

func (st *Store) Snapshot() (raft.FSMSnapshot, error) {
	st.log.Info("persisting snapshot")
	return st.schema, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (st *Store) Restore(rc io.ReadCloser) error {
	st.log.Info("restoring snapshot")
	defer func() {
		if err := rc.Close(); err != nil {
			st.log.Error("restore snapshot: close reader", "error", err)
		}
	}()

	if err := st.schema.Restore(rc); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}

	// TODO-RAFT START
	// In some cases, when the follower state is too far behind the leader's log, the leader might decide to send a snapshot.
	// Consequently, the follower needs to update its state accordingly.
	// Task: https://semi-technology.atlassian.net/browse/WVT-42
	//
	return nil
}

// Join adds the given peer to the cluster.
// This operation must be executed on the leader, otherwise, it will fail with ErrNotLeader.
// If the cluster has not been opened yet, it will return ErrNotOpen.
func (st *Store) Join(id, addr string, voter bool) error {
	if !st.open.Load() {
		return ErrNotOpen
	}
	if st.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	cfg, err := st.raftConfig()
	if err != nil {
		return err
	}
	rID, rAddr := raft.ServerID(id), raft.ServerAddress(addr)
	for _, x := range cfg.Servers {
		// prevent from self join
		if x.ID == rID && x.Address == rAddr {
			return nil
		}
		// TODO: investigate the case of same address and different ids
	}

	if !voter {
		return st.assertFuture(st.raft.AddNonvoter(rID, rAddr, 0, 0))
	}
	return st.assertFuture(st.raft.AddVoter(rID, rAddr, 0, 0))
}

// Remove removes this peer from the cluster
func (st *Store) Remove(id string) error {
	if !st.open.Load() {
		return ErrNotOpen
	}
	if st.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	return st.assertFuture(st.raft.RemoveServer(raft.ServerID(id), 0, 0))
}

// Notify signals this Store that a node is ready for bootstrapping at the specified address.
// Bootstrapping will be initiated once the number of known nodes reaches the expected level,
// which includes this node.

func (st *Store) Notify(id, addr string) (err error) {
	if !st.open.Load() {
		return ErrNotOpen
	}
	// peer is not voter or already bootstrapped or belong to an existing cluster
	if st.bootstrapExpect == 0 || st.bootstrapped.Load() || st.Leader() != "" {
		return nil
	}

	st.mutex.Lock()
	defer st.mutex.Unlock()

	st.candidates[id] = addr
	if len(st.candidates) < st.bootstrapExpect {
		st.log.Debug("number of candidates", "expect", st.bootstrapExpect, "got", st.candidates)
		return nil
	}
	candidates := make([]raft.Server, 0, len(st.candidates))
	i := 0
	for id, addr := range st.candidates {
		candidates = append(candidates, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(id),
			Address:  raft.ServerAddress(addr),
		})
		delete(st.candidates, id)
		i++
	}

	st.log.Info("starting bootstrapping", "candidates", candidates)

	fut := st.raft.BootstrapCluster(raft.Configuration{Servers: candidates})
	if err := fut.Error(); err != nil {
		return err
	}
	st.bootstrapped.Store(true)
	return nil
}

func (st *Store) Stats() map[string]string {
	return st.raft.Stats()
}

func (st *Store) Leader() string {
	if st.raft == nil {
		return ""
	}
	add, _ := st.raft.LeaderWithID()
	return string(add)
}

func (st *Store) assertFuture(fut raft.IndexFuture) error {
	if err := fut.Error(); err != nil && errors.Is(err, raft.ErrNotLeader) {
		return ErrNotLeader
	} else {
		return err
	}
}

func (st *Store) raftConfig() (raft.Configuration, error) {
	cfg := st.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return raft.Configuration{}, err
	}
	return cfg.Configuration(), nil
}

func (st *Store) configureRaft() *raft.Config {
	// TODO-RAFT extend configuration
	cfg := raft.DefaultConfig()
	if st.raftHeartbeatTimeout != 0 {
		cfg.HeartbeatTimeout = st.raftHeartbeatTimeout
	}
	if st.raftElectionTimeout != 0 {
		cfg.ElectionTimeout = st.raftElectionTimeout
	}
	cfg.LocalID = raft.ServerID(st.nodeID)
	cfg.SnapshotThreshold = 1024
	cfg.LogLevel = st.logLevel
	return cfg
}

func (st *Store) loadDatabase(ctx context.Context) {
	if st.dbLoaded.Load() {
		return
	}
	for _, cls := range st.schema.Classes {
		st.parser.ParseClass(&cls.Class)
		cls.Sharding.SetLocalName(st.nodeID)
	}

	if err := st.db.Open(ctx); err != nil {
		st.log.Error("cannot restore database", "error", err.Error())
		panic("error restoring database")
	}
	st.dbLoaded.Store(true)
	st.log.Info("database has been successfully loaded")
}
