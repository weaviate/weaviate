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
	"github.com/weaviate/weaviate/cluster/proto/api"
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
	ErrUnknownCommand = errors.New("unknown command")
	// ErrDeadlineExceeded represents an error returned when the deadline for waiting for a specific update is exceeded.
	ErrDeadlineExceeded = errors.New("deadline exceeded for waiting for update")
)

// Indexer interface updates both the collection and its indices in the filesystem.
// This is distinct from updating metadata, which is handled through a different interface.
type Indexer interface {
	AddClass(api.AddClassRequest) error
	UpdateClass(api.UpdateClassRequest) error
	DeleteClass(string) error
	AddProperty(class string, req api.AddPropertyRequest) error
	AddTenants(class string, req *api.AddTenantsRequest) error
	UpdateTenants(class string, req *api.UpdateTenantsRequest) error
	DeleteTenants(class string, req *api.DeleteTenantsRequest) error
	UpdateShardStatus(*api.UpdateShardStatusRequest) error
	GetShardsStatus(class string) (models.ShardStatusList, error)
	UpdateIndex(api.UpdateClassRequest) error

	// ReloadLocalDB reloads the local database using the latest schema.
	ReloadLocalDB(ctx context.Context, all []api.UpdateClassRequest) error

	// RestoreClassDir restores classes on the filesystem directly from the temporary class backup stored on disk.
	RestoreClassDir(class string) error
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

	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	RecoveryTimeout  time.Duration
	SnapshotInterval time.Duration
	BootstrapTimeout time.Duration
	// UpdateWaitTimeout Timeout duration for waiting for the update to be propagated to this follower node.
	UpdateWaitTimeout time.Duration
	SnapshotThreshold uint64

	DB           Indexer
	Parser       Parser
	AddrResolver addressResolver
	Logger       *slog.Logger
	LogLevel     string
	Voter        bool

	// LoadLegacySchema is responsible for loading old schema from boltDB
	LoadLegacySchema LoadLegacySchema
	// SaveLegacySchema is responsible for loading new schema into boltDB
	SaveLegacySchema SaveLegacySchema
	// IsLocalHost only required when running Weaviate from the console in localhost
	IsLocalHost bool
}

type Store struct {
	raft             *raft.Raft
	open             atomic.Bool
	raftDir          string
	raftPort         int
	voter            bool
	bootstrapExpect  int
	recoveryTimeout  time.Duration
	heartbeatTimeout time.Duration
	electionTimeout  time.Duration
	snapshotInterval time.Duration

	// applyTimeout timeout limit the amount of time raft waits for a command to be started
	applyTimeout time.Duration

	// migrationTimeout is the timeout for restoring the legacy schema
	migrationTimeout time.Duration

	// UpdateWaitTimeout Timeout duration for waiting for the update to be propagated to this follower node.
	updateWaitTimeout time.Duration

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

	// lastIndex        atomic.Uint64

	// lastAppliedIndex index of latest update to the store
	lastAppliedIndex atomic.Uint64

	// dbLoaded is set when the DB is loaded at startup
	dbLoaded atomic.Bool

	// LoadLegacySchema is responsible for loading old schema from boltDB
	loadLegacySchema LoadLegacySchema
	saveLegacySchema SaveLegacySchema
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
		migrationTimeout:  cfg.BootstrapTimeout,
		updateWaitTimeout: cfg.UpdateWaitTimeout,
		applyTimeout:      time.Second * 20,
		nodeID:            cfg.NodeID,
		host:              cfg.Host,
		addResolver:       newAddrResolver(&cfg),
		db:                &localDB{NewSchema(cfg.NodeID, cfg.DB), cfg.DB, cfg.Parser, cfg.Logger},
		log:               cfg.Logger,
		logLevel:          cfg.LogLevel,

		// loadLegacySchema is responsible for loading old schema from boltDB
		loadLegacySchema: cfg.LoadLegacySchema,
		saveLegacySchema: cfg.SaveLegacySchema,
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

	// log cache
	logCache, err := st.init()
	if err != nil {
		return fmt.Errorf("initialize raft store: %w", err)
	}

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
		return fmt.Errorf("raft.NewRaft %v %w", st.transport.LocalAddr(), err)
	}
	st.lastAppliedIndex.Store(st.raft.AppliedIndex())
	st.log.Info("raft node",
		"raft_applied_index", st.raft.AppliedIndex(),
		"raft_last_index", st.raft.LastIndex(),
		"last_log_applied_index", st.initialLastAppliedIndex,
		"last_snapshot_index", lastSnapshotIndex)

	go st.onLeaderFound(st.migrationTimeout)

	return nil
}

func (st *Store) init() (logCache *raft.LogCache, err error) {
	if err = os.MkdirAll(st.raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", st.raftDir, err)
	}

	// log store
	st.logStore, err = raftbolt.NewBoltStore(filepath.Join(st.raftDir, raftDBName))
	if err != nil {
		return nil, fmt.Errorf("bolt db: %w", err)
	}

	// log cache
	logCache, err = raft.NewLogCache(logCacheCapacity, st.logStore)
	if err != nil {
		return nil, fmt.Errorf("log cache: %w", err)
	}

	// file snapshot store
	st.snapshotStore, err = raft.NewFileSnapshotStore(st.raftDir, nRetainedSnapShots, os.Stdout)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %w", err)
	}

	// tcp transport
	address := fmt.Sprintf("%s:%d", st.host, st.raftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("net.resolve tcp address=%v: %w", address, err)
	}

	st.transport, err = st.addResolver.NewTCPTransport(address, tcpAddr, tcpMaxPool, tcpTimeout)
	if err != nil {
		return nil, fmt.Errorf("transport address=%v tcpAddress=%v maxPool=%v timeOut=%v: %w",
			address, tcpAddr, tcpMaxPool, tcpTimeout, err)
	}
	st.log.Info("tcp transport", "address", address, "tcpMaxPool", tcpMaxPool, "tcpTimeout", tcpTimeout)

	return
}

// onLeaderFound execute specific tasks when the leader is detected
func (st *Store) onLeaderFound(timeout time.Duration) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {

		if leader := st.Leader(); leader != "" {
			st.log.Info("current Leader", "address", leader)
		} else {
			continue
		}

		// migrate from old non raft schema to the new schema
		migrate := func() error {
			legacySchema, err := st.loadLegacySchema()
			if err != nil {
				return fmt.Errorf("load schema: %w", err)
			}

			// If the legacy schema is empty we can abort early
			if len(legacySchema) == 0 {
				st.log.Info("legacy schema is empty, nothing to migrate")
				return nil
			}

			// serialize snapshot
			b, c, err := LegacySnapshot(st.nodeID, legacySchema)
			if err != nil {
				return fmt.Errorf("create snapshot: %s" + err.Error())
			}
			b.Index = st.raft.LastIndex()
			b.Term = 1
			if err := st.raft.Restore(b, c, timeout); err != nil {
				return fmt.Errorf("raft restore: %w" + err.Error())
			}
			return nil
		}

		// Only leader can restore the old schema
		if st.IsLeader() && st.db.Schema.len() == 0 && st.loadLegacySchema != nil {
			st.log.Info("starting migration from old schema")
			if err := migrate(); err != nil {
				st.log.Error("migrate from old schema: %v" + err.Error())
			} else {
				st.log.Info("migration from the old schema has been successfully completed")
			}
		}
		return
	}
}

// StoreSchemaV1() is responsible for saving new schema (RAFT) to boltDB
func (st *Store) StoreSchemaV1() error {
	return st.saveLegacySchema(st.db.Schema.States())
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

func (st *Store) SetDB(db Indexer) { st.db.SetIndexer(db) }

func (st *Store) Ready() bool {
	return st.open.Load() && st.dbLoaded.Load() && st.Leader() != ""
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

// WaitForUpdate waits until the update with the given version is propagated to this follower node
func (st *Store) WaitForUpdate(ctx context.Context, period time.Duration, version uint64) error {
	if idx := st.lastAppliedIndex.Load(); idx >= version {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, st.updateWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	var idx uint64
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: version got=%d  want=%d", ErrDeadlineExceeded, idx, version)
		case <-ticker.C:
			if idx = st.lastAppliedIndex.Load(); idx >= version {
				return nil
			} else {
				st.log.Debug("wait for update version", "got", idx, "want", version) // TODO-RAFT remove
			}
		}
	}
}

// IsLeader returns whether this node is the leader of the cluster
func (st *Store) IsLeader() bool {
	return st.raft != nil && st.raft.State() == raft.Leader
}

func (st *Store) SchemaReader() retrySchema {
	return retrySchema{st.db.Schema, st.VersionedSchemaReader()}
}

func (st *Store) VersionedSchemaReader() versionedSchema {
	f := func(ctx context.Context, version uint64) error {
		return st.WaitForUpdate(ctx, time.Millisecond*50, version)
	}
	return versionedSchema{st.db.Schema, f}
}

// FindSimilarClass returns the name of an existing class with a similar name, and "" otherwise
func (f *Store) FindSimilarClass(name string) string {
	return f.db.Schema.ClassEqual(name)
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
// The value of "initial_last_applied_index" is the index of the last applied command found when
// the store was opened, see Store.initialLastAppliedIndex.
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
	stats["leader_address"] = currentLeaderAddress
	stats["leader_id"] = currentLeaderID
	stats["ready"] = st.Ready()
	stats["is_voter"] = st.IsVoter()
	stats["open"] = st.open.Load()
	stats["bootstrapped"] = st.bootstrapped.Load()
	stats["candidates"] = st.candidates
	stats["initial_last_applied_index"] = st.initialLastAppliedIndex
	stats["last_applied_index"] = st.lastAppliedIndex.Load()
	stats["db_loaded"] = st.dbLoaded.Load()

	// If the raft stats exist, add them as a nested map
	if st.raft != nil {
		stats["raft"] = st.raft.Stats()
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

func (st *Store) Execute(req *api.ApplyRequest) (uint64, error) {
	st.log.Debug("server.execute", "type", req.Type, "class", req.Class)

	cmdBytes, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal command: %w", err)
	}
	classInfo := st.db.Schema.ClassInfo(req.Class)
	if req.Type == api.ApplyRequest_TYPE_RESTORE_CLASS && classInfo.Exists {
		st.log.Info("class already restored", "class", req.Class)
		return 0, nil
	}
	if req.Type == api.ApplyRequest_TYPE_ADD_CLASS {
		if other := st.FindSimilarClass(req.Class); other == req.Class {
			return 0, errClassExists
		} else if other != "" {
			return 0, fmt.Errorf("%w: found similar class %q", errClassExists, other)
		}
	}

	fut := st.raft.Apply(cmdBytes, st.applyTimeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return 0, ErrNotLeader
		}
		return 0, err
	}
	resp := fut.Response().(Response)
	return resp.Version, resp.Error
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (st *Store) Apply(l *raft.Log) interface{} {
	ret := Response{Version: l.Index}
	st.log.Debug("apply command", "type", l.Type, "index", l.Index)
	if l.Type != raft.LogCommand {
		st.log.Info("not a valid command", "type", l.Type, "index", l.Index)
		return ret
	}
	cmd := api.ApplyRequest{}
	if err := gproto.Unmarshal(l.Data, &cmd); err != nil {
		st.log.Error("decode command: " + err.Error())
		panic("error proto un-marshalling log data")
	}

	schemaOnly := l.Index <= st.initialLastAppliedIndex
	defer func() {
		st.lastAppliedIndex.Store(l.Index)
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

	cmd.Version = l.Index
	switch cmd.Type {

	case api.ApplyRequest_TYPE_ADD_CLASS:
		ret.Error = st.db.AddClass(&cmd, st.nodeID, schemaOnly)

	case api.ApplyRequest_TYPE_RESTORE_CLASS:
		ret.Error = st.db.RestoreClass(&cmd, st.nodeID, schemaOnly)

	case api.ApplyRequest_TYPE_UPDATE_CLASS:
		ret.Error = st.db.UpdateClass(&cmd, st.nodeID, schemaOnly)

	case api.ApplyRequest_TYPE_DELETE_CLASS:
		ret.Error = st.db.DeleteClass(&cmd, schemaOnly)

	case api.ApplyRequest_TYPE_ADD_PROPERTY:
		ret.Error = st.db.AddProperty(&cmd, schemaOnly)

	case api.ApplyRequest_TYPE_UPDATE_SHARD_STATUS:
		ret.Error = st.db.UpdateShardStatus(&cmd, schemaOnly)

	case api.ApplyRequest_TYPE_ADD_TENANT:
		ret.Error = st.db.AddTenants(&cmd, schemaOnly)

	case api.ApplyRequest_TYPE_UPDATE_TENANT:
		ret.Data, ret.Error = st.db.UpdateTenants(&cmd, schemaOnly)

	case api.ApplyRequest_TYPE_DELETE_TENANT:
		ret.Error = st.db.DeleteTenants(&cmd, schemaOnly)

	case api.ApplyRequest_TYPE_STORE_SCHEMA_V1:
		ret.Error = st.StoreSchemaV1()

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

	if st.raft != nil {
		st.lastAppliedIndex.Store(st.raft.AppliedIndex()) // TODO-RAFT: check if raft return the latest applied index
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
		if !errors.Is(err, raft.ErrCantBootstrap) {
			return err
		}
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
// the leader may decide to send a snapshot. Consequently, the follower must update its state accordingly.
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

	st.log.Info("reload local db: loading indexes ...")
	if err := st.db.Reload(); err != nil {
		st.log.Error("cannot reload database: " + err.Error())
		panic(fmt.Sprintf("cannot reload database: %v", err))
	}

	st.dbLoaded.Store(true)
	st.initialLastAppliedIndex = 0
	return true
}

type Response struct {
	Error   error
	Version uint64
	Data    interface{}
}

var _ raft.FSM = &Store{}
