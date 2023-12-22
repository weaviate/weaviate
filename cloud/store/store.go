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
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"google.golang.org/protobuf/proto"
)

type DB interface {
	AddClass(pl command.AddClassRequest) error
	UpdateClass(req command.UpdateClassRequest) error
	DeleteClass(string) error
	AddProperty(string, command.AddPropertyRequest) error
	AddTenants(class string, req *command.AddTenantsRequest) error
	UpdateTenants(class string, req *command.UpdateTenantsRequest) error
	DeleteTenants(class string, req *command.DeleteTenantsRequest) error
	UpdateShardStatus(req *command.UpdateShardStatusRequest) error
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

	DB     DB
	Parser Parser
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

	nodeID string
	host   string
	schema *schema
	db     DB
	parser Parser

	bootstrapped atomic.Bool

	mutex      sync.Mutex
	candidates map[string]string
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
	}
}

func (f *Store) SetDB(db DB) {
	f.db = db
}

func (st *Store) Execute(cmd *command.ApplyRequest) error {
	log.Printf("server apply: %s %+v\n", cmd.Type, cmd.Class)

	cmdBytes, err := proto.Marshal(cmd)
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
