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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"golang.org/x/exp/slices"
	gproto "google.golang.org/protobuf/proto"
)

const (
	// tcpMaxPool controls how many connections we will pool
	tcpMaxPool = 3

	// tcpTimeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	tcpTimeout = 10 * time.Second

	raftDBName         = "raft.db"
	logCacheCapacity   = 512
	nRetainedSnapShots = 1
)

type Candidate struct {
	ID       string
	Address  string
	NonVoter bool
}

func (f *Store) Open(isLeader bool, joiners []Candidate) (*raft.Raft, error) {
	fmt.Println("bootstrapping started")

	if err := os.MkdirAll(f.raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", f.raftDir, err)
	}

	// log store
	logStore, err := raftbolt.NewBoltStore(filepath.Join(f.raftDir, raftDBName))
	if err != nil {
		return nil, fmt.Errorf("raft: bolt db: %w", err)
	}
	// log cache
	logCache, err := raft.NewLogCache(logCacheCapacity, logStore)
	if err != nil {
		return nil, fmt.Errorf("raft: log cache: %w", err)
	}
	// file snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(f.raftDir, nRetainedSnapShots, os.Stdout)
	if err != nil {
		return nil, fmt.Errorf("raft: file snapshot store: %w", err)
	}

	// tcp transport
	address := fmt.Sprintf("%s:%d", f.host, f.raftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("net.ResolveTCPAddr address=%v error=%w", address, err)
	}

	transport, err := raft.NewTCPTransport(address, tcpAddr, tcpMaxPool, tcpTimeout, os.Stdout)
	if err != nil {
		return nil, fmt.Errorf("raft.NewTCPTransport  address=%v tcpAddress=%v maxPool=%v timeOut=%v: %w", address, tcpAddr, tcpMaxPool, tcpTimeout, err)
	}
	log.Printf("raft.NewTCPTransport  address=%v tcpAddress=%v maxPool=%v timeOut=%v\n", address, tcpAddr, tcpMaxPool, tcpTimeout)

	// raft node
	raftNode, err := raft.NewRaft(f.configureRaft(), f, logCache, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft %v %w", address, err)
	}

	// Construct clusterConfig based on the expected initial joiners
	clusterConfig := raft.Configuration{Servers: []raft.Server{}}
	for _, j := range joiners {
		voter := raft.Nonvoter
		if !j.NonVoter {
			voter = raft.Voter
		}
		clusterConfig.Servers = append(clusterConfig.Servers, raft.Server{
			ID:       raft.ServerID(j.ID),
			Address:  raft.ServerAddress(j.Address),
			Suffrage: voter,
		})
	}

	log.Println(clusterConfig.Servers)
	raftNode.BootstrapCluster(clusterConfig)
	f.raft = raftNode

	go func() {
		//isLeader = isLeader && raftNode.Leader() == ""
		//if isLeader {
		//
		// servers := raftNode.GetConfiguration().Configuration().Servers
		// fmt.Println(servers)

		// time.Sleep(time.Second * 10)
		// log.Println("---- Wait for leader")
		// isCurLeader := <-raftNode.LeaderCh()
		// log.Println("---- Current leader", isCurLeader)

		// cluster := NewCluster(raftNode, f.host)
		// for _, c := range joiners {
		// 	// clusterConfig.Servers = append(clusterConfig.Servers, raft.Server{
		// 	// 	ID:      raft.ServerID(c.ID),
		// 	// 	Address: raft.ServerAddress(c.Address),
		// 	// },
		// 	// )
		// 	// log.Printf("--- Join(%v,%v,%v): %v\n", c.ID, c.Address, c.NonVoter, err)
		// 	// if err := cluster.Join(c.ID, c.Address, !c.NonVoter); err != nil {
		// 	// 	log.Printf("join(%v,%v,%v): %v", c.ID, c.Address, c.NonVoter, err)
		// 	// }
		// }
		//}
		var lastLeader raft.ServerAddress = "Unknown"
		t := time.NewTicker(time.Second * 5)
		defer t.Stop()
		for range t.C {
			leader := raftNode.Leader()
			if leader != lastLeader {
				lastLeader = leader
				log.Printf("Current Leader: %v\n", lastLeader)
				log.Printf("+%v", raftNode.Stats())
			}
		}
	}()

	log.Printf("bootstrapping done, %v\n", f)

	return raftNode, nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *Store) Apply(l *raft.Log) interface{} {
	if l.Type != raft.LogCommand {
		log.Printf("%v is not a log command\n", l.Type)
		return nil
	}
	ret := Response{}
	cmd := command.Command{}

	if err := gproto.Unmarshal(l.Data, &cmd); err != nil {
		log.Printf("apply: unmarshal command %v\n", err)
		return nil
	}
	// log.Printf("apply: op=%v key=%v value=%v", cmd.Type, cmd.Class, cmd.SubCommand)
	switch cmd.Type {
	case command.Command_TYPE_ADD_CLASS, command.Command_TYPE_RESTORE_CLASS:
		req := command.AddClassRequest{}
		if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
			log.Printf("unmarshal sub command: %v", err)
			return Response{Error: err}
		}
		if req.State == nil {
			return fmt.Errorf("nil sharding state")
		}
		if err := f.parser.ParseClass(req.Class); err != nil {
			return Response{Error: err}
		}
		req.State.SetLocalName(f.nodeID)
		if ret.Error = f.schema.addClass(req.Class, req.State); ret.Error == nil {
			f.db.AddClass(req)
		}
	case command.Command_TYPE_UPDATE_CLASS:
		req := command.UpdateClassRequest{}
		if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
			return Response{Error: err}
		}
		if req.State != nil {
			req.State.SetLocalName(f.nodeID)
		}
		if err := f.parser.ParseClass(req.Class); err != nil {
			return Response{Error: err}
		}
		if ret.Error = f.schema.updateClass(req.Class, req.State); ret.Error == nil {
			f.db.UpdateClass(req)
		}
	case command.Command_TYPE_DELETE_CLASS:
		f.schema.deleteClass(cmd.Class)
		f.db.DeleteClass(cmd.Class)
	case command.Command_TYPE_ADD_PROPERTY:
		req := command.AddPropertyRequest{}
		if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
			return Response{Error: err}
		}
		if req.Property == nil {
			return Response{Error: fmt.Errorf("nil property")}
		}
		if ret.Error = f.schema.addProperty(cmd.Class, *req.Property); ret.Error == nil {
			f.db.AddProperty(cmd.Class, req)
		}

	case command.Command_TYPE_UPDATE_SHARD_STATUS:
		req := command.UpdateShardStatusRequest{}
		if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
			return Response{Error: err}
		}
		ret.Error = f.db.UpdateShardStatus(&req)

	case command.Command_TYPE_ADD_TENANT:
		req := &command.AddTenantsRequest{}
		if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
			return Response{Error: err}
		}
		if ret.Error = f.schema.addTenants(cmd.Class, req); ret.Error == nil {
			f.db.AddTenants(cmd.Class, req)
		}
	case command.Command_TYPE_UPDATE_TENANT:
		req := &command.UpdateTenantsRequest{}
		if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
			return Response{Error: err}
		}
		ret.Data, ret.Error = f.schema.updateTenants(cmd.Class, req)
		if ret.Error == nil {
			f.db.UpdateTenants(cmd.Class, req)
		}
	case command.Command_TYPE_DELETE_TENANT:
		req := &command.DeleteTenantsRequest{}
		if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
			return Response{Error: err}
		}
		if err := f.schema.deleteTenants(cmd.Class, req); err != nil {
			log.Printf("delete tenants from class %q: %v", cmd.Class, err)
		}
		f.db.DeleteTenants(cmd.Class, req)

	default:
		log.Printf("unknown command %v\n", &cmd)
	}
	return ret
}

func (f *Store) Snapshot() (raft.FSMSnapshot, error) {
	log.Println("persisting snapshot")
	return f.schema, nil
}

func (f *Store) Restore(rc io.ReadCloser) error {
	log.Println("restoring snapshot")
	if err := f.schema.Restore(rc); err != nil {
		log.Printf("restore shanpshot: %v", err)
	}

	for k, v := range f.schema.Classes {
		if err := f.parser.ParseClass(&v.Class); err != nil {
			log.Printf("parse class %v", err)
			continue
		}
		v.Sharding.SetLocalName(f.nodeID)
		if err := f.db.AddClass(command.AddClassRequest{
			Class: &v.Class,
			State: &v.Sharding,
		}); err != nil {
			log.Printf("add class %v", err)
			continue
		}

		for _, t := range v.Sharding.Physical {
			if !slices.Contains[string](t.BelongsToNodes, f.nodeID) {
				continue
			}
			f.db.AddTenants(k, &command.AddTenantsRequest{
				Tenants: []*command.Tenant{
					{Name: t.Name, Status: t.Status, Nodes: t.BelongsToNodes},
				},
			})
		}
	}
	return nil
}

func (f *Store) configureRaft() *raft.Config {
	cfg := raft.DefaultConfig()
	if f.raftHeartbeatTimeout != 0 {
		cfg.HeartbeatTimeout = f.raftHeartbeatTimeout
	}
	if f.raftElectionTimeout != 0 {
		cfg.ElectionTimeout = f.raftElectionTimeout
	}
	cfg.LocalID = raft.ServerID(f.nodeID)
	cfg.SnapshotThreshold = 250
	return cfg
}
