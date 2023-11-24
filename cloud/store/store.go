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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader      = errors.New("node is not the leader")
	ErrLeaderNotFound = errors.New("leader not found")
	ErrNotOpen        = errors.New("store not open")
)

type DB interface {
	AddClass(pl cmd.AddClassRequest) error
	UpdateClass(req cmd.UpdateClassRequest) error
	DeleteClass(string) error
	AddProperty(string, cmd.AddPropertyRequest) error
	AddTenants(class string, req *cmd.AddTenantsRequest) error
	UpdateTenants(class string, req *cmd.UpdateTenantsRequest) error
	DeleteTenants(class string, req *cmd.DeleteTenantsRequest) error
	UpdateShardStatus(req *cmd.UpdateShardStatusRequest) error
	GetShardsStatus(class string) (models.ShardStatusList, error)
}

type Parser interface {
	ParseClass(class *models.Class) error
}

type Config struct {
	WorkDir         string // raft working directory
	NodeID          string
	Host            string
	RaftPort        int
	BootstrapExpect int

	RaftHeartbeatTimeout time.Duration
	RaftElectionTimeout  time.Duration

	DB       DB
	Parser   Parser
	Logger   *slog.Logger
	LogLevel string
}

type Store struct {
	raft *raft.Raft

	open                 atomic.Bool
	raftDir              string
	raftPort             int
	bootstrapExpect      int
	raftHeartbeatTimeout time.Duration
	raftElectionTimeout  time.Duration
	raftApplyTimeout     time.Duration

	nodeID   string
	host     string
	schema   *schema
	db       DB
	parser   Parser
	log      *slog.Logger
	logLevel string

	bootstrapped atomic.Bool

	mutex      sync.Mutex
	candidates map[string]string

	// initialLastAppliedIndex represents the index of the last applied command when the store is opened.
	initialLastAppliedIndex uint64

	loadDB func() error
	// dbLoaded is set when the DB is loaded at startup
	dbLoaded atomic.Bool
}

func New(cfg Config) Store {
	return Store{
		raftDir:              cfg.WorkDir,
		raftPort:             cfg.RaftPort,
		bootstrapExpect:      cfg.BootstrapExpect,
		candidates:           make(map[string]string, cfg.BootstrapExpect),
		raftHeartbeatTimeout: cfg.RaftHeartbeatTimeout,
		raftElectionTimeout:  cfg.RaftElectionTimeout,
		raftApplyTimeout:     time.Second * 20,
		nodeID:               cfg.NodeID,
		host:                 cfg.Host,
		schema:               NewSchema(cfg.NodeID, cfg.DB),
		db:                   cfg.DB,
		parser:               cfg.Parser,
		log:                  cfg.Logger,
		logLevel:             cfg.LogLevel,
	}
}

func (f *Store) SetDB(db DB) {
	f.db = db
	f.schema.shardReader = db
}

func (st *Store) Execute(req *cmd.ApplyRequest) error {
	st.log.Debug("server.execute", "type", req.Type, "class", req.Class)

	cmdBytes, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	fut := st.raft.Apply(cmdBytes, st.raftApplyTimeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	return nil
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
	return st.raft.State() == raft.Leader
}

func (f *Store) SchemaReader() *schema {
	return f.schema
}

type Response struct {
	Error error
	Data  interface{}
}

var _ raft.FSM = &Store{}
