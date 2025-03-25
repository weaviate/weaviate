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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/config"
	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/cluster/fsm/schema"
	"github.com/weaviate/weaviate/cluster/log"
	"github.com/weaviate/weaviate/cluster/types"
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

// Store is the implementation of RAFT on this local node. It will handle the local schema and RAFT operations (startup,
// bootstrap, snapshot, etc...). It ensures that a raft cluster is setup with remote node on start (either recovering
// from old state, or bootstrap itself based on the provided configuration).
type Store struct {
	cfg config.RaftConfig
	// log is a shorthand to the logger passed in the config to reduce the amount of indirection when logging in the
	// code
	log *logrus.Logger

	// open is set on opening the store
	open atomic.Bool
	// raft implementation from external library
	raft          *raft.Raft
	raftResolver  types.RaftResolver
	raftTransport *raft.NetworkTransport

	// applyTimeout timeout limit the amount of time raft waits for a command to be applied
	applyTimeout time.Duration

	// raft snapshot store
	snapshotStore *raft.FileSnapshotStore

	// raft log store
	logStore *raftbolt.BoltStore

	// raft log cache
	logCache *raft.LogCache

	// cluster bootstrap related attributes
	bootstrapMutex sync.Mutex
	candidates     map[string]string
	// bootstrapped is set once the node has either bootstrapped or recovered from RAFT log entries
	bootstrapped atomic.Bool

	fsm fsm.FSM
}

func NewFSM(cfg config.RaftConfig, reg prometheus.Registerer) Store {
	return Store{
		cfg:          cfg,
		log:          cfg.Logger,
		candidates:   make(map[string]string, cfg.BootstrapExpect),
		applyTimeout: time.Second * 20,
		raftResolver: cfg.RaftResolver,
		fsm:          cfg.FSM,
	}
}

func (st *Store) IsVoter() bool { return st.cfg.Voter }
func (st *Store) ID() string    { return st.cfg.NodeID }

// lastIndex returns the last index in stable storage,
// either from the last log or from the last snapshot.
// this method work as a protection from applying anything was applied to the db
// by checking either raft or max(snapshot, log store) instead the db will catchup
func (st *Store) lastIndex() uint64 {
	if st.raft != nil {
		return st.raft.AppliedIndex()
	}

	l, err := st.LastAppliedCommand()
	if err != nil {
		panic(fmt.Sprintf("read log last command: %s", err.Error()))
	}
	return max(lastSnapshotIndex(st.snapshotStore), l)
}

// Open opens this store and marked as such.
// It constructs a new Raft node using the provided configuration.
// If there is any old state, such as snapshots, logs, peers, etc., all of those will be restored.
func (st *Store) Open(ctx context.Context) (err error) {
	if st.open.Load() { // store already opened
		return nil
	}
	defer func() { st.open.Store(err == nil) }()

	if err := st.init(); err != nil {
		return fmt.Errorf("initialize raft store: %w", err)
	}

	// we have to open the DB before constructing new raft in case of restore calls
	st.fsm.Open(ctx, st.lastIndex())

	st.log.WithFields(logrus.Fields{
		"name":                 st.cfg.NodeID,
		"metadata_only_voters": st.cfg.MetadataOnlyVoter,
	}).Info("construct a new raft node")
	st.raft, err = raft.NewRaft(st.raftConfig(), st.fsm, st.logCache, st.logStore, st.snapshotStore, st.raftTransport)
	if err != nil {
		return fmt.Errorf("raft.NewRaft %v %w", st.raftTransport.LocalAddr(), err)
	}

	st.log.WithFields(logrus.Fields{
		"raft_applied_index":                st.raft.AppliedIndex(),
		"raft_last_index":                   st.raft.LastIndex(),
		"last_store_applied_index_on_start": st.lastIndex(),
		"last_snapshot_index":               lastSnapshotIndex(st.snapshotStore),
	}).Info("raft node constructed")

	// There's no hard limit on the migration, so it should take as long as necessary.
	// However, we believe that 1 day should be more than sufficient.
	f := func() { st.onLeaderFound(time.Hour * 24) }
	enterrors.GoWrapper(f, st.log)
	return nil
}

func (st *Store) init() error {
	var err error
	if err := os.MkdirAll(st.cfg.WorkDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", st.cfg.WorkDir, err)
	}

	// log store
	st.logStore, err = raftbolt.NewBoltStore(filepath.Join(st.cfg.WorkDir, raftDBName))
	if err != nil {
		return fmt.Errorf("bolt db: %w", err)
	}

	// log cache
	st.logCache, err = raft.NewLogCache(logCacheCapacity, st.logStore)
	if err != nil {
		return fmt.Errorf("log cache: %w", err)
	}

	// file snapshot store
	st.snapshotStore, err = raft.NewFileSnapshotStore(st.cfg.WorkDir, nRetainedSnapShots, os.Stdout)
	if err != nil {
		return fmt.Errorf("file snapshot store: %w", err)
	}

	// tcp transport
	address := fmt.Sprintf("%s:%d", st.cfg.Host, st.cfg.Port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return fmt.Errorf("net.resolve tcp address=%v: %w", address, err)
	}

	st.raftTransport, err = st.raftResolver.NewTCPTransport(address, tcpAddr, tcpMaxPool, tcpTimeout, st.log)
	if err != nil {
		return fmt.Errorf("raft transport address=%v tcpAddress=%v maxPool=%v timeOut=%v: %w", address, tcpAddr, tcpMaxPool, tcpTimeout, err)
	}
	st.log.WithFields(logrus.Fields{
		"address":    address,
		"tcpMaxPool": tcpMaxPool,
		"tcpTimeout": tcpTimeout,
	}).Info("tcp transport")

	return err
}

// onLeaderFound execute specific tasks when the leader is detected
func (st *Store) onLeaderFound(timeout time.Duration) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		if leader := st.Leader(); leader != "" {
			st.log.WithField("address", leader).Info("current Leader")
		} else {
			continue
		}
	}
}

func (st *Store) Close(ctx context.Context) error {
	if !st.open.Load() {
		return nil
	}

	// transfer leadership: it stops accepting client requests, ensures
	// the target server is up to date and initiates the transfer
	if st.IsLeader() {
		st.log.Info("transferring leadership to another server")
		if err := st.raft.LeadershipTransfer().Error(); err != nil {
			st.log.WithError(err).Error("transferring leadership")
		} else {
			st.log.Info("successfully transferred leadership to another server")
		}
	}

	if err := st.raft.Shutdown().Error(); err != nil {
		return err
	}

	st.open.Store(false)

	st.log.Info("closing raft-net ...")
	if err := st.raftTransport.Close(); err != nil {
		// it's not that fatal if we weren't able to close
		// the transport, that's why just warn
		st.log.WithError(err).Warn("close raft-net")
	}

	st.log.Info("closing log store ...")
	if err := st.logStore.Close(); err != nil {
		return fmt.Errorf("close log store: %w", err)
	}

	st.log.Info("closing data store ...")
	if err := st.fsm.Close(ctx); err != nil {
		return fmt.Errorf(" close database: %w", err)
	}

	return nil
}

func (st *Store) SetDB(db schema.Indexer) {
	st.fsm.SetDBHandle(db)
}

func (st *Store) Ready() bool {
	return st.open.Load() && st.fsm.IsDBOpen() && st.Leader() != ""
}

// WaitToLoadDB waits for the DB to be loaded. The DB might be first loaded
// after RAFT is in a healthy state, which is when the leader has been elected and there
// is consensus on the log.
func (st *Store) WaitToRestoreDB(ctx context.Context, period time.Duration, close chan struct{}) error {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-close:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if st.fsm.IsDBOpen() {
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

// SchemaReader returns a SchemaReader from the underlying schema manager using a wait function that will make it wait
// for a raft log entry to be applied in the FSM Store before authorizing the read to continue.
func (st *Store) SchemaReader() schema.SchemaReader {
	return st.fsm.NewSchemaReader()
}

// Stats returns internal statistics from this store, for informational/debugging purposes only.
//
// The statistics directly from raft are nested under the "raft" key. If the raft statistics are
// not yet available, then the "raft" key will not exist.
// See https://pkg.go.dev/github.com/hashicorp/raft#Raft.Stats for the default raft stats.
//
// The values of "leader_address" and "leader_id" are the respective address/ID for the current
// leader of the cluster. They may be empty strings if there is no current leader or the leader is
// unknown.
//
// The value of "ready" indicates whether this store is ready, see Store.Ready.
//
// The value of "is_voter" indicates whether this store is a voter, see Store.IsVoter.
//
// The value of "open" indicates whether this store is open, see Store.open.
//
// The value of "bootstrapped" indicates whether this store has completed bootstrapping,
// see Store.bootstrapped.
//
// The value of "candidates" is a map[string]string of the current candidates IDs/addresses,
// see Store.candidates.
//
// The value of "last_store_log_applied_index" is the index of the last applied command found when
// the store was opened, see Store.lastAppliedIndexToDB.
//
// The value of "last_applied_index" is the index of the latest update to the store,
// see Store.lastAppliedIndex.
//
// The value of "db_loaded" indicates whether the DB has finished loading, see Store.dbLoaded.
//
// Since this is for information/debugging we want to avoid enforcing unnecessary restrictions on
// what can go in these stats, thus we're returning map[string]any. However, any values added to
// this map should be able to be JSON encoded.
func (st *Store) Stats() map[string]any {
	stats := make(map[string]any)

	// Add custom stats for this store
	currentLeaderAddress, currentLeaderID := st.LeaderWithID()
	stats["id"] = st.cfg.NodeID
	stats["leader_address"] = currentLeaderAddress
	stats["leader_id"] = currentLeaderID
	stats["ready"] = st.Ready()
	stats["is_voter"] = st.IsVoter()
	stats["open"] = st.open.Load()
	stats["bootstrapped"] = st.bootstrapped.Load()
	stats["candidates"] = st.candidates
	stats["last_store_log_applied_index"] = st.lastIndex()
	stats["last_applied_index"] = st.lastIndex()
	stats["db_loaded"] = st.fsm.IsDBOpen()

	// If the raft stats exist, add them as a nested map
	if st.raft != nil {
		stats["raft"] = st.raft.Stats()
		// add the servers information
		var servers []map[string]any
		if cf := st.raft.GetConfiguration(); cf.Error() == nil {
			servers = make([]map[string]any, len(cf.Configuration().Servers))
			for i, server := range cf.Configuration().Servers {
				servers[i] = map[string]any{
					"id":       server.ID,
					"address":  server.Address,
					"suffrage": server.Suffrage,
				}
			}
			stats["raft_latest_configuration_servers"] = servers
		}
	}

	return stats
}

// Leader is used to return the current leader address.
// It may return empty strings if there is no current leader or the leader is unknown.
func (st *Store) Leader() string {
	if st.raft == nil {
		return ""
	}
	add, _ := st.raft.LeaderWithID()
	return string(add)
}

func (st *Store) LeaderWithID() (raft.ServerAddress, raft.ServerID) {
	if st.raft == nil {
		return "", ""
	}
	return st.raft.LeaderWithID()
}

func (st *Store) assertFuture(fut raft.IndexFuture) error {
	if err := fut.Error(); err != nil && errors.Is(err, raft.ErrNotLeader) {
		return types.ErrNotLeader
	} else {
		return err
	}
}

func (st *Store) raftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	if st.cfg.HeartbeatTimeout > 0 {
		cfg.HeartbeatTimeout = st.cfg.HeartbeatTimeout
	}
	if st.cfg.ElectionTimeout > 0 {
		cfg.ElectionTimeout = st.cfg.ElectionTimeout
	}
	if st.cfg.SnapshotInterval > 0 {
		cfg.SnapshotInterval = st.cfg.SnapshotInterval
	}
	if st.cfg.SnapshotThreshold > 0 {
		cfg.SnapshotThreshold = st.cfg.SnapshotThreshold
	}

	cfg.LocalID = raft.ServerID(st.cfg.NodeID)
	cfg.LogLevel = st.cfg.Logger.GetLevel().String()

	logger := log.NewHCLogrusLogger("raft", st.log)
	cfg.Logger = logger

	return cfg
}

func lastSnapshotIndex(ss *raft.FileSnapshotStore) uint64 {
	ls, err := ss.List()
	if err != nil || len(ls) == 0 {
		return 0
	}
	return ls[0].Index
}
