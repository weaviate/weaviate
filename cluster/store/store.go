//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	cmd "github.com/weaviate/weaviate/cluster/proto/cluster"
	command "github.com/weaviate/weaviate/cluster/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"google.golang.org/protobuf/proto"
	gproto "google.golang.org/protobuf/proto"
)

const (

	// tcpMaxPool controls how many connections we will pool
	tcpMaxPool = 3

	// tcpTimeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	tcpTimeout = 10 * time.Second

	raftDBName = "raft.db"

	// logCacheCapacity is the maximum number of logs to cache in-memory.
	// This is used to reduce disk I/O for the recently committed entries.
	logCacheCapacity = 512

	nRetainedSnapShots = 3
)

var (
	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader      = errors.New("node is not the leader")
	ErrLeaderNotFound = errors.New("leader not found")
	ErrNotOpen        = errors.New("store not open")
)

// Indexer interface updates both the collection and its indices in the filesystem.
// This is distinct from updating metadata, which is handled through a different interface.
type Indexer interface {
	AddClass(cmd.AddClassRequest) error
	UpdateClass(cmd.UpdateClassRequest) error
	DeleteClass(string) error
	AddProperty(class string, req cmd.AddPropertyRequest) error
	AddTenants(class string, req *cmd.AddTenantsRequest) error
	UpdateTenants(class string, req *cmd.UpdateTenantsRequest) error
	DeleteTenants(class string, req *cmd.DeleteTenantsRequest) error
	UpdateShardStatus(*cmd.UpdateShardStatusRequest) error
	GetShardsStatus(class string) (models.ShardStatusList, error)
	Open(context.Context) error
	Close(context.Context) error
}

// Parser parses concrete class fields after deserialization
type Parser interface {
	// ParseClassUpdate parses a class after unmarshaling by setting concrete types for the fields
	ParseClass(class *models.Class) error

	// ParseClass parses new updates by providing the current class data.
	ParseClassUpdate(class, update *models.Class) (*models.Class, error)
}

type Config struct {
	WorkDir  string // raft working directory
	NodeID   string
	Host     string
	RaftPort int
	RPCPort  int

	// ServerName2PortMap maps server names to port numbers
	ServerName2PortMap map[string]int
	BootstrapExpect    int

	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	RecoveryTimeout   time.Duration
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64

	DB           Indexer
	Parser       Parser
	AddrResolver addressResolver
	Logger       *slog.Logger
	LogLevel     string
	Voter        bool
	// IsLocalHost only required when running Weaviate from the console in localhost
	IsLocalHost bool
}

type Store struct {
	raft              *raft.Raft
	open              atomic.Bool
	raftDir           string
	raftPort          int
	voter             bool
	bootstrapExpect   int
	recoveryTimeout   time.Duration
	heartbeatTimeout  time.Duration
	electionTimeout   time.Duration
	snapshotInterval  time.Duration
	applyTimeout      time.Duration
	snapshotThreshold uint64

	nodeID   string
	host     string
	db       *localDB
	log      *slog.Logger
	logLevel string

	bootstrapped  atomic.Bool
	logStore      *raftbolt.BoltStore
	addResolver   *addrResolver
	transport     *raft.NetworkTransport
	snapshotStore *raft.FileSnapshotStore

	mutex      sync.Mutex
	candidates map[string]string

	// initialLastAppliedIndex represents the index of the last applied command when the store is opened.
	initialLastAppliedIndex uint64

	// dbLoaded is set when the DB is loaded at startup
	dbLoaded atomic.Bool
}

func New(cfg Config) Store {
	return Store{
		raftDir:           cfg.WorkDir,
		raftPort:          cfg.RaftPort,
		voter:             cfg.Voter,
		bootstrapExpect:   cfg.BootstrapExpect,
		candidates:        make(map[string]string, cfg.BootstrapExpect),
		recoveryTimeout:   cfg.RecoveryTimeout,
		heartbeatTimeout:  cfg.HeartbeatTimeout,
		electionTimeout:   cfg.ElectionTimeout,
		snapshotInterval:  cfg.SnapshotInterval,
		snapshotThreshold: cfg.SnapshotThreshold,
		applyTimeout:      time.Second * 20,
		nodeID:            cfg.NodeID,
		host:              cfg.Host,
		addResolver:       newAddrResolver(&cfg),
		db:                &localDB{NewSchema(cfg.NodeID, cfg.DB), cfg.DB, cfg.Parser},
		log:               cfg.Logger,
		logLevel:          cfg.LogLevel,
	}
}

func (st *Store) IsVoter() bool { return st.voter }
func (st *Store) ID() string    { return st.nodeID }

// Open opens this store and marked as such.
func (st *Store) Open(ctx context.Context) (err error) {
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
	st.snapshotStore, err = raft.NewFileSnapshotStore(st.raftDir, nRetainedSnapShots, os.Stdout)
	if err != nil {
		return fmt.Errorf("raft: file snapshot store: %w", err)
	}

	// tcp transport
	address := fmt.Sprintf("%s:%d", st.host, st.raftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return fmt.Errorf("net.ResolveTCPAddr address=%v: %w", address, err)
	}

	st.transport, err = st.addResolver.NewTCPTransport(address, tcpAddr, tcpMaxPool, tcpTimeout)
	if err != nil {
		return fmt.Errorf("raft.NewTCPTransport  address=%v tcpAddress=%v maxPool=%v timeOut=%v: %w", address, tcpAddr, tcpMaxPool, tcpTimeout, err)
	}

	st.log.Info("raft tcp transport", "address", address, "tcpMaxPool", tcpMaxPool, "tcpTimeout", tcpTimeout)

	rLog := rLog{st.logStore}
	st.initialLastAppliedIndex, err = rLog.LastAppliedCommand()
	if err != nil {
		return fmt.Errorf("read log last command: %w", err)
	}
	lastSnapshotIndex := snapshotIndex(st.snapshotStore)
	if st.initialLastAppliedIndex == 0 { // empty node
		st.loadDatabase(ctx)
	}

	st.log.Info("construct a new raft node")
	st.raft, err = raft.NewRaft(st.raftConfig(), st, logCache, st.logStore, st.snapshotStore, st.transport)
	if err != nil {
		return fmt.Errorf("raft.NewRaft %v %w", address, err)
	}

	st.log.Info("raft node",
		"raft_applied_index", st.raft.AppliedIndex(),
		"raft_last_index", st.raft.LastIndex(),
		"last_log_applied_index", st.initialLastAppliedIndex,
		"last_snapshot_index", lastSnapshotIndex)

	go func() {
		lastLeader := "Unknown"
		t := time.NewTicker(time.Second * 60)
		defer t.Stop()
		for range t.C {
			if leader := st.Leader(); leader != "" && leader != lastLeader {
				lastLeader = leader
				st.log.Info("current Leader", "address", lastLeader)
			}
		}
	}()

	return nil
}

func (st *Store) Close(ctx context.Context) (err error) {
	if !st.open.Load() {
		return nil
	}

	// transfer leadership: it stops accepting client requests, ensures
	// the target server is up to date and initiates the transfer
	if st.IsLeader() {
		st.log.Info("transferring leadership to another server")
		if err := st.raft.LeadershipTransfer().Error(); err != nil {
			st.log.Error("transferring leadership: " + err.Error())
		} else {
			st.log.Info("successfully transferred leadership to another server")
		}
	}

	if err = st.raft.Shutdown().Error(); err != nil {
		return err
	}

	st.open.Store(false)

	st.transport.Close()

	st.log.Info("closing log store ...")
	err = st.logStore.Close()

	st.log.Info("closing data store ...")
	errDB := st.db.Close(ctx)
	if err != nil || errDB != nil {
		return fmt.Errorf("close log store: %w, close database: %w", err, errDB)
	}

	return
}

func (f *Store) SetDB(db Indexer) { f.db.SetIndexer(db) }

func (f *Store) Ready() bool {
	return f.open.Load() && f.dbLoaded.Load()
}

// WaitToLoadDB waits for the DB to be loaded. The DB might be first loaded
// after RAFT is in a healthy state, which is when the leader has been elected and there
// is consensus on the log.
func (st *Store) WaitToRestoreDB(ctx context.Context, period time.Duration) error {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if st.dbLoaded.Load() {
				return nil
			} else {
				st.log.Info("waiting for database to be restored")
			}
		}
	}
}

// IsLeader returns whether this node is the leader of the cluster
func (st *Store) IsLeader() bool {
	return st.raft != nil && st.raft.State() == raft.Leader
}

func (f *Store) SchemaReader() retrySchema {
	return retrySchema{f.db.Schema}
}

func (st *Store) Stats() map[string]string { return st.raft.Stats() }

func (st *Store) Leader() string {
	if st.raft == nil {
		return ""
	}
	add, _ := st.raft.LeaderWithID()
	return string(add)
}

func (st *Store) Execute(req *cmd.ApplyRequest) error {
	st.log.Debug("server.execute", "type", req.Type, "class", req.Class)

	cmdBytes, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	if req.Type == cmd.ApplyRequest_TYPE_RESTORE_CLASS && st.db.Schema.ClassInfo(req.Class).Exists {
		st.log.Info("class already restored", "class", req.Class)
		return nil
	}

	fut := st.raft.Apply(cmdBytes, st.applyTimeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	resp := fut.Response().(Response)
	return resp.Error
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (st *Store) Apply(l *raft.Log) interface{} {
	ret := Response{}
	st.log.Debug("apply command", "type", l.Type, "index", l.Index)
	if l.Type != raft.LogCommand {
		st.log.Info("not a valid command", "type", l.Type, "index", l.Index)
		return ret
	}
	cmd := command.ApplyRequest{}
	if err := gproto.Unmarshal(l.Data, &cmd); err != nil {
		st.log.Error("decode command: " + err.Error())
		panic("error proto un-marshalling log data")
	}

	schemaOnly := l.Index <= st.initialLastAppliedIndex
	defer func() {
		// If the local db has not been loaded, wait until we reach the state
		// from the local raft log before loading the db.
		// This is necessary because the database operations are not idempotent
		if !st.dbLoaded.Load() && l.Index >= st.initialLastAppliedIndex {
			st.loadDatabase(context.Background())
		}
		if ret.Error != nil {
			st.log.Error("apply command: "+ret.Error.Error(), "type", l.Type, "index", l.Index)
		}
	}()

	switch cmd.Type {

	case command.ApplyRequest_TYPE_ADD_CLASS:
		ret.Error = st.db.AddClass(&cmd, st.nodeID, schemaOnly)

	case command.ApplyRequest_TYPE_RESTORE_CLASS:
		err := st.db.AddClass(&cmd, st.nodeID, schemaOnly)
		if err == nil || !errors.Is(err, errClassExists) {
			ret.Error = err
		}
		st.log.Info("class already restored", "class", cmd.Class, "op", "apply_restore")

	case command.ApplyRequest_TYPE_UPDATE_CLASS:
		ret.Error = st.db.UpdateClass(&cmd, st.nodeID, schemaOnly)

	case command.ApplyRequest_TYPE_DELETE_CLASS:
		ret.Error = st.db.DeleteClass(&cmd, schemaOnly)

	case command.ApplyRequest_TYPE_ADD_PROPERTY:
		ret.Error = st.db.AddProperty(&cmd, schemaOnly)

	case command.ApplyRequest_TYPE_UPDATE_SHARD_STATUS:
		ret.Error = st.db.UpdateShardStatus(&cmd, schemaOnly)

	case command.ApplyRequest_TYPE_ADD_TENANT:
		ret.Error = st.db.AddTenants(&cmd, schemaOnly)

	case command.ApplyRequest_TYPE_UPDATE_TENANT:
		ret.Data, ret.Error = st.db.UpdateTenants(&cmd, schemaOnly)

	case command.ApplyRequest_TYPE_DELETE_TENANT:
		ret.Error = st.db.DeleteTenants(&cmd, schemaOnly)
	default:
		// This could occur when a new command has been introduced in a later app version
		// At this point, we need to panic so that the app undergo an upgrade during restart
		const msg = "consider upgrading to newer version"
		st.log.Error("unknown command", "type", cmd.Type, "class", cmd.Class, "more", msg)
		panic(fmt.Sprintf("unknown command type=%d class=%s more=%s", cmd.Type, cmd.Class, msg))
	}

	return ret
}

func (st *Store) Snapshot() (raft.FSMSnapshot, error) {
	st.log.Info("persisting snapshot")
	return st.db.Schema, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (st *Store) Restore(rc io.ReadCloser) error {
	st.log.Info("restoring schema from snapshot")
	defer func() {
		if err := rc.Close(); err != nil {
			st.log.Error("restore snapshot: close reader: " + err.Error())
		}
	}()

	if err := st.db.Schema.Restore(rc, st.db.parser); err != nil {
		st.log.Error("restoring schema from snapshot: " + err.Error())
		return fmt.Errorf("restore schema from snapshot: %w", err)
	}
	st.log.Info("successfully restored schema from snapshot")

	if st.reloadDB() {
		st.log.Info("successfully reloaded indexes from snapshot", "n", st.db.Schema.len())
	}

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

	rID, rAddr := raft.ServerID(id), raft.ServerAddress(addr)

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
	if !st.voter || st.bootstrapExpect == 0 || st.bootstrapped.Load() || st.Leader() != "" {
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

	st.log.Info("starting cluster bootstrapping", "candidates", candidates)

	fut := st.raft.BootstrapCluster(raft.Configuration{Servers: candidates})
	if err := fut.Error(); err != nil {
		st.log.Error("bootstrapping cluster: " + err.Error())

		return err
	}
	st.bootstrapped.Store(true)
	return nil
}

func (st *Store) assertFuture(fut raft.IndexFuture) error {
	if err := fut.Error(); err != nil && errors.Is(err, raft.ErrNotLeader) {
		return ErrNotLeader
	} else {
		return err
	}
}

func (st *Store) raftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	if st.heartbeatTimeout > 0 {
		cfg.HeartbeatTimeout = st.heartbeatTimeout
	}
	if st.electionTimeout > 0 {
		cfg.ElectionTimeout = st.electionTimeout
	}
	if st.snapshotInterval > 0 {
		cfg.SnapshotInterval = st.snapshotInterval
	}
	if st.snapshotThreshold > 0 {
		cfg.SnapshotThreshold = st.snapshotThreshold
	}

	cfg.LocalID = raft.ServerID(st.nodeID)
	cfg.LogLevel = st.logLevel
	return cfg
}

func (st *Store) loadDatabase(ctx context.Context) {
	if st.dbLoaded.Load() {
		return
	}

	st.log.Info("loading local db")
	if err := st.db.Load(ctx, st.nodeID); err != nil {
		st.log.Error("cannot restore database: " + err.Error())
		panic("error restoring database")
	}

	st.dbLoaded.Store(true)
	st.log.Info("database has been successfully loaded", "n", st.db.Schema.len())
}

// reloadDB reloads the node's local db. If the db is already loaded, it will be reloaded.
// If a snapshot exists and its is up to date with the log, it will be loaded.
// Otherwise, the database will be loaded when the node synchronizes its state with the leader.
// For more details, see apply() -> loadDatabase().
//
// In specific scenarios where the follower's state is too far behind the leader's log,
// the leader may decide to send a snapshot. Consequently, the follower muss update its state accordingly.
func (st *Store) reloadDB() bool {
	ctx := context.Background()

	if !st.dbLoaded.CompareAndSwap(true, false) {
		// the snapshot already includes the state from the raft log
		snapIndex := snapshotIndex(st.snapshotStore)
		st.log.Info("load local db from snapshot", "last_log_applied_index",
			st.initialLastAppliedIndex, "last_snapshot_index", snapIndex)
		if st.initialLastAppliedIndex <= snapIndex {
			st.loadDatabase(ctx)
			return true
		}
		return false
	}

	st.log.Info("reload local db: closing db ...")
	if err := st.db.Close(ctx); err != nil {
		st.log.Error("reload db: close db " + err.Error())
		panic(fmt.Sprintf("reload db from snapshot: close db: %v", err))
	}

	st.log.Info("reload local db: loading indexes ...")
	if err := st.db.Load(ctx, st.nodeID); err != nil {
		st.log.Error("cannot reload database: " + err.Error())
		panic(fmt.Sprintf("cannot reload database: %v", err))
	}

	st.dbLoaded.Store(true)
	st.initialLastAppliedIndex = 0
	return true
}

type Response struct {
	Error error
	Data  interface{}
}

var _ raft.FSM = &Store{}
