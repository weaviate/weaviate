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

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/dynusers"
	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/cluster/log"
	rbacRaft "github.com/weaviate/weaviate/cluster/rbac"
	"github.com/weaviate/weaviate/cluster/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/resolver"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
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

type Config struct {
	// WorkDir is the directory RAFT will use to store config & snapshot
	WorkDir string
	// NodeID is this node id
	NodeID string
	// Host is this node host name
	Host string
	// RaftPort is used by internal RAFT communication
	RaftPort int
	// RPCPort is used by weaviate internal gRPC communication
	RPCPort int
	// RaftRPCMessageMaxSize is the maximum message sized allowed on the internal RPC communication
	// TODO: Remove Raft prefix to avoid confusion between RAFT and RPC.
	RaftRPCMessageMaxSize int

	// NodeNameToPortMap maps server names to port numbers
	NodeNameToPortMap map[string]int

	// Raft leader election related settings

	// HeartbeatTimeout specifies the time in follower state without contact
	// from a leader before we attempt an election.
	HeartbeatTimeout time.Duration
	// ElectionTimeout specifies the time in candidate state without contact
	// from a leader before we attempt an election.
	ElectionTimeout time.Duration
	// LeaderLeaseTimeout specifies the time in leader state without contact
	// from a follower before we attempt an election.
	LeaderLeaseTimeout time.Duration
	// TimeoutsMultiplier is the multiplier for the timeout values for
	// raft election, heartbeat, and leader lease
	TimeoutsMultiplier int

	// Raft snapshot related settings

	// SnapshotThreshold controls how many outstanding logs there must be before
	// we perform a snapshot. This is to prevent excessive snapshotting by
	// replaying a small set of logs instead. The value passed here is the initial
	// setting used. This can be tuned during operation using ReloadConfig.
	SnapshotThreshold uint64

	// SnapshotInterval controls how often we check if we should perform a
	// snapshot. We randomly stagger between this value and 2x this value to avoid
	// the entire cluster from performing a snapshot at once. The value passed
	// here is the initial setting used. This can be tuned during operation using
	// ReloadConfig.
	SnapshotInterval time.Duration

	// TrailingLogs controls how many logs we leave after a snapshot. This is used
	// so that we can quickly replay logs on a follower instead of being forced to
	// send an entire snapshot. The value passed here is the initial setting used.
	// This can be tuned during operation using ReloadConfig.
	TrailingLogs uint64

	// Cluster bootstrap related settings

	// BootstrapTimeout is the time a node will notify other node that it is ready to bootstrap a cluster if it can't
	// find a an existing cluster to join
	BootstrapTimeout time.Duration
	// BootstrapExpect is the number of nodes this cluster expect to receive a notify from to start bootstrapping a
	// cluster
	BootstrapExpect int

	// ConsistencyWaitTimeout is the duration we will wait for a schema version to land on that node
	ConsistencyWaitTimeout time.Duration
	// NodeSelector is the memberlist interface to RAFT
	NodeSelector cluster.NodeSelector
	Logger       *logrus.Logger
	Voter        bool

	// MetadataOnlyVoters configures the voters to store metadata exclusively, without storing any other data
	MetadataOnlyVoters bool

	// DB is the interface to the weaviate database. It is necessary so that schema changes are reflected to the DB
	DB schema.Indexer
	// Parser parses class field after deserialization
	Parser schema.Parser
	// LoadLegacySchema is responsible for loading old schema from boltDB
	LoadLegacySchema schema.LoadLegacySchema
	// SaveLegacySchema is responsible for loading new schema into boltDB
	SaveLegacySchema schema.SaveLegacySchema
	// IsLocalHost only required when running Weaviate from the console in localhost
	IsLocalHost bool

	// SentryEnabled configures the sentry integration to add internal middlewares to rpc client/server to set spans &
	// capture traces
	SentryEnabled bool

	// EnableOneNodeRecovery enables the actually one node recovery logic to avoid it running all the time when
	// unnecessary
	EnableOneNodeRecovery bool
	// ForceOneNodeRecovery will force the single node recovery routine to run. This is useful if the cluster has
	// committed wrong peer configuration entry that makes it unable to obtain a quorum to start.
	// WARNING: This should be run on *actual* one node cluster only.
	ForceOneNodeRecovery bool

	// 	AuthzController to manage RBAC commands and apply it to casbin
	AuthzController authorization.Controller
	AuthNConfig     config.Authentication
	RBAC            *rbac.Manager

	DynamicUserController *apikey.DBUser

	// ReplicaCopier copies shard replicas between nodes
	ReplicaCopier replicationTypes.ReplicaCopier

	// ReplicationEngineMaxWorkers is the maximum number of workers for the replication engine
	ReplicationEngineMaxWorkers int

	// DistributedTasks is the configuration for the distributed task manager.
	DistributedTasks config.DistributedTasksConfig

	ReplicaMovementEnabled bool

	// ReplicaMovementMinimumAsyncWait is the minimum time bound that replica movement operations will wait before
	// async replication can complete.
	ReplicaMovementMinimumAsyncWait *runtime.DynamicValue[time.Duration]
}

// Store is the implementation of RAFT on this local node. It will handle the local schema and RAFT operations (startup,
// bootstrap, snapshot, etc...). It ensures that a raft cluster is setup with remote node on start (either recovering
// from old state, or bootstrap itself based on the provided configuration).
type Store struct {
	cfg Config
	// log is a shorthand to the logger passed in the config to reduce the amount of indirection when logging in the
	// code
	log *logrus.Logger

	// open is set on opening the store
	open atomic.Bool
	// dbLoaded is set when the DB is loaded at startup
	dbLoaded atomic.Bool

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

	// schemaManager is responsible for applying changes committed by RAFT to the schema representation & querying the
	// schema
	schemaManager *schema.SchemaManager

	// authZManager is responsible for applying/querying changes committed by RAFT to the rbac representation
	authZManager *rbacRaft.Manager

	// authZManager is responsible for applying/querying changes committed by RAFT to the rbac representation
	dynUserManager *dynusers.Manager

	// replicationManager is responsible for applying/querying the replication FSM used to handle replication operations
	replicationManager *replication.Manager

	// distributedTaskManager is responsible for applying/querying the distributed task FSM used to handle distributed tasks.
	distributedTasksManager *distributedtask.Manager

	// lastAppliedIndexToDB represents the index of the last applied command when the store is opened.
	lastAppliedIndexToDB atomic.Uint64
	// lastAppliedIndex index of latest update to the store
	lastAppliedIndex atomic.Uint64

	// snapshotter is the snapshotter for the store
	snapshotter fsm.Snapshotter

	// authZController is the authz controller for the store
	authZController authorization.Controller

	metrics *storeMetrics
}

// storeMetrics exposes RAFT store related prometheus metrics
type storeMetrics struct {
	applyDuration prometheus.Histogram
	applyFailures prometheus.Counter

	// raftLastAppliedIndex represents current applied index of a raft cluster in local node.
	// This includes every commands including config changes
	raftLastAppliedIndex prometheus.Gauge

	// fsmLastAppliedIndex represents current applied index of cluster store FSM in local node.
	// This includes commands without config changes
	fsmLastAppliedIndex prometheus.Gauge

	// fsmStartupAppliedIndex represents previous applied index of the cluster store FSM in local node
	// that any restart would try to catch up
	fsmStartupAppliedIndex prometheus.Gauge
}

// newStoreMetrics cretes and registers the store related metrics on
// given prometheus registry.
func newStoreMetrics(nodeID string, reg prometheus.Registerer) *storeMetrics {
	r := promauto.With(reg)
	return &storeMetrics{
		applyDuration: r.NewHistogram(prometheus.HistogramOpts{
			Name:        "weaviate_cluster_store_fsm_apply_duration_seconds",
			Help:        "Time to apply cluster store FSM state in local node",
			ConstLabels: prometheus.Labels{"nodeID": nodeID},
			Buckets:     prometheus.ExponentialBuckets(0.001, 5, 5), // 1ms, 5ms, 25ms, 125ms, 625ms
		}),
		applyFailures: r.NewCounter(prometheus.CounterOpts{
			Name:        "weaviate_cluster_store_fsm_apply_failures_total",
			Help:        "Total failure count of cluster store FSM state apply in local node",
			ConstLabels: prometheus.Labels{"nodeID": nodeID},
		}),
		raftLastAppliedIndex: r.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_cluster_store_raft_last_applied_index",
			Help:        "Current applied index of a raft cluster in local node. This includes every commands including config changes",
			ConstLabels: prometheus.Labels{"nodeID": nodeID},
		}),
		fsmLastAppliedIndex: r.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_cluster_store_fsm_last_applied_index",
			Help:        "Current applied index of cluster store FSM in local node. This includes commands without config changes",
			ConstLabels: prometheus.Labels{"nodeID": nodeID},
		}),
		fsmStartupAppliedIndex: r.NewGauge(prometheus.GaugeOpts{
			Name:        "weaviate_cluster_store_fsm_startup_applied_index",
			Help:        "Previous applied index of the cluster store FSM in local node that any restart would try to catch up",
			ConstLabels: prometheus.Labels{"nodeID": nodeID},
		}),
	}
}

func NewFSM(cfg Config, authZController authorization.Controller, snapshotter fsm.Snapshotter, reg prometheus.Registerer) Store {
	schemaManager := schema.NewSchemaManager(cfg.NodeID, cfg.DB, cfg.Parser, reg, cfg.Logger)
	replicationManager := replication.NewManager(schemaManager.NewSchemaReader(), reg)
	schemaManager.SetReplicationFSM(replicationManager.GetReplicationFSM())

	return Store{
		cfg:          cfg,
		log:          cfg.Logger,
		candidates:   make(map[string]string, cfg.BootstrapExpect),
		applyTimeout: time.Second * 20,
		raftResolver: resolver.NewRaft(resolver.RaftConfig{
			ClusterStateReader: cfg.NodeSelector,
			RaftPort:           cfg.RaftPort,
			IsLocalHost:        cfg.IsLocalHost,
			NodeNameToPortMap:  cfg.NodeNameToPortMap,
		}),
		schemaManager:      schemaManager,
		snapshotter:        snapshotter,
		authZController:    authZController,
		authZManager:       rbacRaft.NewManager(cfg.RBAC, cfg.AuthNConfig, snapshotter, cfg.Logger),
		dynUserManager:     dynusers.NewManager(cfg.DynamicUserController, cfg.Logger),
		replicationManager: replicationManager,
		distributedTasksManager: distributedtask.NewManager(distributedtask.ManagerParameters{
			Clock:            clockwork.NewRealClock(),
			CompletedTaskTTL: cfg.DistributedTasks.CompletedTaskTTL,
		}),
		metrics: newStoreMetrics(cfg.NodeID, reg),
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

	li := st.lastIndex()
	st.lastAppliedIndexToDB.Store(li)
	st.metrics.fsmStartupAppliedIndex.Set(float64(li))

	// we have to open the DB before constructing new raft in case of restore calls
	st.openDatabase(ctx)

	st.log.WithFields(logrus.Fields{
		"name":                 st.cfg.NodeID,
		"metadata_only_voters": st.cfg.MetadataOnlyVoters,
	}).Info("construct a new raft node")
	st.raft, err = raft.NewRaft(st.raftConfig(), st, st.logCache, st.logStore, st.snapshotStore, st.raftTransport)
	if err != nil {
		return fmt.Errorf("raft.NewRaft %v %w", st.raftTransport.LocalAddr(), err)
	}

	// Only if node recovery is enabled will we check if we are either forcing it or automating the detection of a one
	// node cluster
	if st.cfg.EnableOneNodeRecovery && (st.cfg.ForceOneNodeRecovery || (st.cfg.BootstrapExpect == 1 && len(st.candidates) < 2)) {
		if err := st.recoverSingleNode(st.cfg.ForceOneNodeRecovery); err != nil {
			return err
		}
	}

	snapIndex := lastSnapshotIndex(st.snapshotStore)
	if st.lastAppliedIndexToDB.Load() == 0 && snapIndex == 0 {
		// if empty node report ready
		st.dbLoaded.Store(true)
	}

	st.lastAppliedIndex.Store(st.raft.AppliedIndex())

	st.log.WithFields(logrus.Fields{
		"raft_applied_index":                st.raft.AppliedIndex(),
		"raft_last_index":                   st.raft.LastIndex(),
		"last_store_applied_index_on_start": st.lastAppliedIndexToDB.Load(),
		"last_snapshot_index":               snapIndex,
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
	st.snapshotStore, err = raft.NewFileSnapshotStore(st.cfg.WorkDir, nRetainedSnapShots, st.log.Out)
	if err != nil {
		return fmt.Errorf("file snapshot store: %w", err)
	}

	// tcp transport
	address := fmt.Sprintf("%s:%d", st.cfg.Host, st.cfg.RaftPort)
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

		// migrate from old non raft schema to the new schema
		migrate := func() error {
			legacySchema, err := st.cfg.LoadLegacySchema()
			if err != nil {
				return fmt.Errorf("load schema: %w", err)
			}

			// If the legacy schema is empty we can abort early
			if len(legacySchema) == 0 {
				st.log.Info("legacy schema is empty, nothing to migrate")
				return nil
			}

			// serialize snapshot
			b, c, err := schema.LegacySnapshot(st.cfg.NodeID, legacySchema)
			if err != nil {
				return fmt.Errorf("create snapshot: %w", err)
			}
			b.Index = st.raft.LastIndex()
			b.Term = 1
			if err := st.raft.Restore(b, c, timeout); err != nil {
				return fmt.Errorf("raft restore: %w", err)
			}
			return nil
		}

		// Only leader can restore the old schema
		if st.IsLeader() && st.schemaManager.NewSchemaReader().Len() == 0 && st.cfg.LoadLegacySchema != nil {
			st.log.Info("starting migration from old schema")
			if err := migrate(); err != nil {
				st.log.WithError(err).Error("migrate from old schema")
			} else {
				st.log.Info("migration from the old schema has been successfully completed")
			}
		}
		return
	}
}

// StoreSchemaV1() is responsible for saving new schema (RAFT) to boltDB
func (st *Store) StoreSchemaV1() error {
	return st.cfg.SaveLegacySchema(st.schemaManager.NewSchemaReader().States())
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
	if err := st.schemaManager.Close(ctx); err != nil {
		return fmt.Errorf(" close database: %w", err)
	}

	return nil
}

func (st *Store) SetDB(db schema.Indexer) { st.schemaManager.SetIndexer(db) }

func (st *Store) Ready() bool {
	return st.open.Load() && st.dbLoaded.Load() && st.Leader() != ""
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
			if st.dbLoaded.Load() {
				return nil
			} else {
				st.log.Info("waiting for database to be restored")
			}
		}
	}
}

// WaitForAppliedIndex waits until the update with the given version is propagated to this follower node
func (st *Store) WaitForAppliedIndex(ctx context.Context, period time.Duration, version uint64) error {
	if idx := st.lastAppliedIndex.Load(); idx >= version {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, st.cfg.ConsistencyWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	var idx uint64
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: version got=%d  want=%d", types.ErrDeadlineExceeded, idx, version)
		case <-ticker.C:
			if idx = st.lastAppliedIndex.Load(); idx >= version {
				return nil
			} else {
				st.log.WithFields(logrus.Fields{
					"got":  idx,
					"want": version,
				}).Debug("wait for update version")
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
	f := func(ctx context.Context, version uint64) error {
		return st.WaitForAppliedIndex(ctx, time.Millisecond*50, version)
	}
	return st.schemaManager.NewSchemaReaderWithWaitFunc(f)
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
	stats["last_store_log_applied_index"] = st.lastAppliedIndexToDB.Load()
	stats["last_applied_index"] = st.lastIndex()
	stats["db_loaded"] = st.dbLoaded.Load()

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
	// If the TimeoutsMultiplier is set, use it to multiply the timeout values
	// This is used to speed up the raft election, heartbeat, and leader lease
	// in a multi-node cluster.
	// the default value is 1
	// for production requirement,it's recommended to set it to 5
	// this in order to tolerate the network delay and avoid extensive leader election triggered more frequently
	// example : https://developer.hashicorp.com/consul/docs/reference/architecture/server#production-server-requirements
	timeOutMultiplier := 1
	if st.cfg.TimeoutsMultiplier > 1 {
		timeOutMultiplier = st.cfg.TimeoutsMultiplier
	}
	if st.cfg.HeartbeatTimeout > 0 {
		cfg.HeartbeatTimeout = st.cfg.HeartbeatTimeout
	}
	if st.cfg.ElectionTimeout > 0 {
		cfg.ElectionTimeout = st.cfg.ElectionTimeout
	}
	if st.cfg.LeaderLeaseTimeout > 0 {
		cfg.LeaderLeaseTimeout = st.cfg.LeaderLeaseTimeout
	}
	if st.cfg.SnapshotInterval > 0 {
		cfg.SnapshotInterval = st.cfg.SnapshotInterval
	}
	if st.cfg.SnapshotThreshold > 0 {
		cfg.SnapshotThreshold = st.cfg.SnapshotThreshold
	}
	if st.cfg.TrailingLogs > 0 {
		cfg.TrailingLogs = st.cfg.TrailingLogs
	}
	cfg.HeartbeatTimeout *= time.Duration(timeOutMultiplier)
	cfg.ElectionTimeout *= time.Duration(timeOutMultiplier)
	cfg.LeaderLeaseTimeout *= time.Duration(timeOutMultiplier)
	cfg.LocalID = raft.ServerID(st.cfg.NodeID)
	cfg.LogLevel = st.cfg.Logger.GetLevel().String()
	cfg.NoLegacyTelemetry = true

	logger := log.NewHCLogrusLogger("raft", st.log)
	cfg.Logger = logger

	return cfg
}

func (st *Store) openDatabase(ctx context.Context) {
	if st.dbLoaded.Load() {
		return
	}

	if st.cfg.MetadataOnlyVoters {
		st.log.Info("Not loading local DB as the node is metadata only")
	} else {
		st.log.Info("loading local db")
		if err := st.schemaManager.Load(ctx, st.cfg.NodeID); err != nil {
			st.log.WithError(err).Error("cannot restore database")
			panic("error restoring database")
		}
		st.log.Info("local DB successfully loaded")
	}

	st.log.WithField("n", st.schemaManager.NewSchemaReader().Len()).Info("schema manager loaded")
}

// reloadDBFromSchema() it will be called from two places Restore(), Apply()
// on constructing raft.NewRaft(..) the raft lib. will
// call Restore() first to restore from snapshots if there is any and
// then later will call Apply() on any new committed log
func (st *Store) reloadDBFromSchema() {
	if !st.cfg.MetadataOnlyVoters {
		st.schemaManager.ReloadDBFromSchema()
	} else {
		st.log.Info("skipping reload DB from schema as the node is metadata only")
	}
	st.dbLoaded.Store(true)

	// in this path it means it was called from Apply()
	// or forced Restore()
	if st.raft != nil {
		// we don't update lastAppliedIndexToDB if not a restore
		return
	}

	// restore requests from snapshots before init new RAFT node
	lastLogApplied, err := st.LastAppliedCommand()
	if err != nil {
		st.log.WithField("error", err).Warn("can't detect the last applied command, setting the lastLogApplied to 0")
	}

	val := max(lastSnapshotIndex(st.snapshotStore), lastLogApplied)
	st.lastAppliedIndexToDB.Store(val)
	st.metrics.fsmStartupAppliedIndex.Set(float64(val))
}

func (st *Store) FSMHasCaughtUp() bool {
	return st.lastAppliedIndex.Load() >= st.lastAppliedIndexToDB.Load()
}

type Response struct {
	Error   error
	Version uint64
}

var _ raft.FSM = &Store{}

func lastSnapshotIndex(snapshotStore *raft.FileSnapshotStore) uint64 {
	if snapshotStore == nil {
		return 0
	}

	ls, err := snapshotStore.List()
	if err != nil || len(ls) == 0 {
		return 0
	}
	return ls[0].Index
}

// recoverSingleNode is used to manually force a new configuration in order to
// recover from a loss of quorum where the current configuration cannot be
// WARNING! This operation implicitly commits all entries in the Raft log, so
// in general this is an extremely unsafe operation and that's why it's made to be
// used in a single cluster node.
// for more details see : https://github.com/hashicorp/raft/blob/main/api.go#L279
func (st *Store) recoverSingleNode(force bool) error {
	if !force && (st.cfg.BootstrapExpect > 1 || len(st.candidates) > 1) {
		return fmt.Errorf("bootstrap expect %v, candidates %v, "+
			"can't perform auto recovery in multi node cluster", st.cfg.BootstrapExpect, st.candidates)
	}
	servers := st.raft.GetConfiguration().Configuration().Servers
	// nothing to do here, wasn't a single node
	if !force && len(servers) != 1 {
		st.log.WithFields(logrus.Fields{
			"servers_from_previous_configuration": servers,
			"candidates":                          st.candidates,
		}).Warn("didn't perform cluster recovery")
		return nil
	}

	exNode := servers[0]
	newNode := raft.Server{
		ID:       raft.ServerID(st.cfg.NodeID),
		Address:  raft.ServerAddress(fmt.Sprintf("%s:%d", st.cfg.Host, st.cfg.RPCPort)),
		Suffrage: raft.Voter,
	}

	// same node nothing to do here
	if !force && (exNode.ID == newNode.ID && exNode.Address == newNode.Address) {
		return nil
	}

	st.log.WithFields(logrus.Fields{
		"action":                      "raft_cluster_recovery",
		"existed_single_cluster_node": exNode,
		"new_single_cluster_node":     newNode,
	}).Info("perform cluster recovery")

	fut := st.raft.Shutdown()
	if err := fut.Error(); err != nil {
		return err
	}

	recoveryConfig := st.cfg
	// Force the recovery to be metadata only and un-assign the associated DB to ensure no DB operations are made during
	// the restore to avoid any data change.
	recoveryConfig.MetadataOnlyVoters = true
	recoveryConfig.DB = nil
	// we don't use actual registry here, because we don't want to register metrics, it's already registered
	// in actually FSM and this is FSM is temporary for recovery.
	tempFSM := NewFSM(recoveryConfig, st.authZController, st.snapshotter, prometheus.NewPedanticRegistry())
	if err := raft.RecoverCluster(st.raftConfig(),
		&tempFSM,
		st.logCache,
		st.logStore,
		st.snapshotStore,
		st.raftTransport,
		raft.Configuration{Servers: []raft.Server{newNode}}); err != nil {
		return err
	}

	var err error
	st.raft, err = raft.NewRaft(st.raftConfig(), st, st.logCache, st.logStore, st.snapshotStore, st.raftTransport)
	if err != nil {
		return fmt.Errorf("raft.NewRaft %v %w", st.raftTransport.LocalAddr(), err)
	}

	if exNode.ID == newNode.ID {
		// no node name change needed in the state
		return nil
	}

	st.log.WithFields(logrus.Fields{
		"action":                       "replace_states_node_name",
		"old_single_cluster_node_name": exNode.ID,
		"new_single_cluster_node_name": newNode.ID,
	}).Info("perform cluster recovery")
	st.schemaManager.ReplaceStatesNodeName(string(newNode.ID))

	return nil
}
