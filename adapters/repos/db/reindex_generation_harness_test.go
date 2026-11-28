//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

// genCluster runs, on every node, the real schema FSM, task FSM, scheduler,
// reindex provider and DB. One in-process log stands in for RAFT: every entry
// is applied on every node in order. Tests submit a run and then only wait.
type genCluster struct {
	t                 testing.TB
	ctx               context.Context
	class             string
	rangeableInMemory bool

	mu     sync.Mutex // serializes the log
	log    []*api.ApplyRequest
	nodes  []*genNode
	runs   int
	shards []string // of a single-tenant class

	gateMu  sync.Mutex
	gates   []genGate
	waiting []*genWaiting
}

// genGate holds entries matching it until released, like a slow leader.
type genGate struct {
	typ   api.ApplyRequest_Type
	match func(raw []byte) bool
	ch    chan struct{}
}

type genWaiting struct {
	typ api.ApplyRequest_Type
	raw []byte
}

type genNode struct {
	c     *genCluster
	name  string
	root  string
	sm    *clusterschema.SchemaManager
	sr    clusterschema.SchemaReader
	dtm   *distributedtask.Manager
	db    *DB
	sched *distributedtask.Scheduler
	start func()
	stop  func()
	logs  *logrustest.Hook
	// lose takes a unit's completion from the provider, which hears it failed; the test may propose it later.
	lose func(req *api.RecordDistributedTaskUnitCompletionRequest) bool
	// panicOnStart, when set, panics the provider's start claim of a unit.
	panicOnStart func(unit string) bool

	applied uint64 // the last entry in this node's log
	down    bool   // crashed: gets no entries and proposes nothing new
	gone    bool   // replaced by a restart: whatever it still proposes is dropped
	lagging bool   // restarted, and its log not caught up yet
}

const (
	genProp  = "title"
	genNum   = "num"
	genValue = "hello world"
)

type genClassOpts struct {
	filterable, searchable, rangeable bool
	tokenization                      string
	mapSearchable                     bool // searchable index on the map strategy, not blockmax
	singleTenantShards                int  // 0 for a multi-tenant class
	rangeableInMemory                 bool
}

func genDefaultOpts() genClassOpts {
	return genClassOpts{filterable: true, searchable: true, tokenization: models.PropertyTokenizationWord}
}

func genClassDef(name string, o genClassOpts) *models.Class {
	off, on := false, true
	return &models.Class{
		Class: name, Vectorizer: "none", VectorIndexType: "hnsw", VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60, Stopwords: &models.StopwordConfig{Preset: "none"},
			IndexNullState: true, IndexPropertyLength: true, UsingBlockMaxWAND: !o.mapSearchable,
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: o.singleTenantShards == 0},
		ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		Properties: []*models.Property{
			{
				Name: genProp, DataType: schema.DataTypeText.PropString(), Tokenization: o.tokenization,
				IndexFilterable: &o.filterable, IndexSearchable: &o.searchable, IndexRangeFilters: &off,
			},
			{
				Name: genNum, DataType: schema.DataTypeInt.PropString(),
				IndexFilterable: &on, IndexSearchable: &off, IndexRangeFilters: &o.rangeable,
			},
		},
	}
}

// newGenCluster starts the nodes and adds the class through the log. Every
// shard has a replica on every node.
func newGenCluster(t testing.TB, o genClassOpts, nodes int) *genCluster {
	c := &genCluster{
		t: t, ctx: context.Background(), class: "Gen" + strings.ReplaceAll(uuid.NewString()[:8], "-", ""),
		rangeableInMemory: o.rangeableInMemory,
	}
	for i := range nodes {
		c.startNode(fmt.Sprintf("node%d", i+1))
	}
	state := &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{}}
	if o.singleTenantShards > 0 {
		cfg, err := shardingConfig.ParseConfig(map[string]interface{}{"desiredCount": float64(o.singleTenantShards)}, nodes)
		require.NoError(t, err)
		state, err = sharding.InitState(c.class, cfg, c.nodes[0].name, c.nodeNames(), int64(nodes), false)
		require.NoError(t, err)
		c.shards = slices.Sorted(maps.Keys(state.Physical))
	}
	class := genClassDef(c.class, o)
	class.ReplicationConfig.Factor = int64(nodes)
	require.NoError(t, c.apply(api.ApplyRequest_TYPE_ADD_CLASS, c.class, api.AddClassRequest{Class: class, State: state}))
	return c
}

func (c *genCluster) nodeNames() []string {
	out := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		out[i] = n.name
	}
	return out
}

type genParser struct{}

func (genParser) ParseClass(*models.Class) error { return nil }
func (genParser) ParseClassUpdate(_, update *models.Class) (*models.Class, error) {
	return update, nil
}

// genSchemaGetter is the DB's schema getter over its node's schema FSM.
type genSchemaGetter struct {
	fakeSchemaGetter
	n *genNode
}

func (g *genSchemaGetter) ReadOnlyClass(class string) *models.Class {
	return g.n.sr.ReadOnlyClass(class)
}

func (g *genSchemaGetter) ClassLive(class string) map[string]map[string]uint64 {
	return g.n.sr.ClassLive(class)
}
func (g *genSchemaGetter) NodeName() string { return g.n.name }
func (g *genSchemaGetter) Nodes() []string  { return g.n.c.nodeNames() }
func (g *genSchemaGetter) ShardReplicas(class, shard string) ([]string, error) {
	return g.n.sr.ShardReplicas(class, shard)
}

func (g *genSchemaGetter) ShardOwner(class, shard string) (string, error) {
	return g.n.name, nil
}

func (g *genSchemaGetter) TenantsShards(_ context.Context, class string, tenants ...string) (map[string]string, error) {
	out := map[string]string{}
	err := g.n.sr.Read(class, true, func(_ *models.Class, st *sharding.State) error {
		for _, t := range tenants {
			if p, ok := st.Physical[t]; ok {
				out[t] = p.Status
			}
		}
		return nil
	})
	return out, err
}

func (g *genSchemaGetter) OptimisticTenantStatus(ctx context.Context, class, tenant string, _ bool) (map[string]string, error) {
	return g.TenantsShards(ctx, class, tenant)
}

func (g *genSchemaGetter) CopyShardingState(class string) *sharding.State {
	var out *sharding.State
	_ = g.n.sr.Read(class, true, func(_ *models.Class, st *sharding.State) error {
		cp := st.DeepCopy()
		out = &cp
		return nil
	})
	return out
}

func (c *genCluster) startNode(name string) {
	n := c.newNode(name, c.t.TempDir())
	n.start()
	c.nodes = append(c.nodes, n)
}

// newNode opens a node's FSMs and DB over root; its scheduler starts with start.
func (c *genCluster) newNode(name, root string) *genNode {
	t := c.t
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	n := &genNode{c: c, name: name, root: root, logs: logrustest.NewLocal(logger)}
	n.sm = clusterschema.NewSchemaManager(name, nil, genParser{}, prometheus.NewPedanticRegistry(), logger)
	n.sr = n.sm.NewSchemaReader()
	n.dtm = distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:                  logger,
		ConflictDetectors:       map[string]distributedtask.ConflictDetector{ReindexNamespace: ReindexDetector{}},
		SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{ReindexNamespace: ReindexDetector{}},
		TaskRules:               map[string]distributedtask.TaskRules{ReindexNamespace: ReindexTaskRules{}},
		ClassLive:               n.sr.ClassLive,
		ReadSharding: func(class string, read func(*sharding.State)) error {
			return n.sr.Read(class, false, func(_ *models.Class, st *sharding.State) error {
				read(st)
				return nil
			})
		},
	})
	n.sm.SetDistributedTaskManager(n.dtm)

	fsm := replicationTypes.NewMockReplicationFSMReader(t)
	fsm.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	fsm.EXPECT().HasActiveTargetReplicationForShard(mock.Anything, mock.Anything, mock.Anything).Return(false).Maybe()
	fsm.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{name}).Maybe()
	fsm.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{name}).Maybe()
	sel := cluster.NewMockNodeSelector(t)
	sel.EXPECT().LocalName().Return(name).Maybe()
	sel.EXPECT().NodeHostname(mock.Anything).Return(name, true).Maybe()
	eager := false
	db, err := New(logger, name, Config{
		MemtablesFlushDirtyAfter: 60, RootPath: root, QueryMaximumResults: 100000, MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards: &eager, IndexRangeableInMemory: c.rangeableInMemory,
	}, &FakeRemoteClient{}, sel, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(), sel, n.sr, fsm, nil)
	require.NoError(t, err)
	n.db = db
	db.SetSchemaGetter(&genSchemaGetter{n: n})
	require.NoError(t, db.WaitForStartup(c.ctx))
	n.sm.SetIndexer(schemaUC.NewExecutor(NewMigrator(db, logger, name), n.sr, logger, func(string) error { return nil }))
	db.SetReindexTaskStateSource(n.dtm)
	n.dtm.SetStateListener(db.ReindexTaskStateListener)

	client := &genClient{c: c, n: n}
	prov := NewReindexProvider(db, logger, name, func() int { return 4 }, c.ctx)
	prov.SetReindexFlipper(client)
	n.sched = distributedtask.NewScheduler(distributedtask.SchedulerParams{
		CompletionRecorder: client, TaskSource: n.dtm, TaskCleaner: client, TaskFinalizer: client,
		AckRecorder: client, Providers: map[string]distributedtask.Provider{ReindexNamespace: prov},
		Logger: logger, LocalNode: name, CompletedTaskTTL: time.Hour, TickInterval: 20 * time.Millisecond,
	})
	n.dtm.SetSchedulerNotifier(n.sched)
	ctx, cancel := context.WithCancel(c.ctx)
	var once sync.Once
	n.stop = func() {
		once.Do(func() {
			n.sched.Close()
			cancel()
		})
	}
	t.Cleanup(func() {
		n.stop()
		require.NoError(t, n.db.Shutdown(context.Background()))
	})
	n.start = func() { require.NoError(t, n.sched.Start(ctx)) }
	return n
}

func genEncode(sub any) ([]byte, error) {
	switch m := sub.(type) {
	case *api.AddTenantsRequest, *api.DeleteTenantsRequest: // the schema's tenant entries are proto, the rest JSON
		return gproto.Marshal(m.(gproto.Message))
	default:
		return json.Marshal(sub)
	}
}

// apply appends one entry and applies it on every node that is up.
func (c *genCluster) apply(typ api.ApplyRequest_Type, class string, sub any) error {
	return c.propose(nil, typ, class, sub)
}

// propose is apply for an entry a node proposed; a node replaced by a restart
// never gets its entry into the log. The first up node's error is the answer.
func (c *genCluster) propose(from *genNode, typ api.ApplyRequest_Type, class string, sub any) error {
	raw, err := genEncode(sub)
	if err != nil {
		return err
	}
	if from != nil && c.isDown(from) {
		return fmt.Errorf("node %s is down", from.name)
	}
	c.waitGates(typ, raw)
	c.mu.Lock()
	defer c.mu.Unlock()
	if from != nil && from.gone {
		return fmt.Errorf("node %s crashed before the entry reached the log", from.name)
	}
	c.log = append(c.log, &api.ApplyRequest{Type: typ, Class: class, SubCommand: raw, Version: uint64(len(c.log) + 1)})
	var first error
	answered := false
	for _, n := range c.nodes {
		if n.down || n.lagging {
			continue
		}
		err := n.applyNext(false)
		if !answered {
			first, answered = err, true
		}
	}
	return first
}

func (n *genNode) applyNext(schemaOnly bool) error {
	n.applied++
	return n.applyEntry(n.c.log[n.applied-1], n.applied, schemaOnly)
}

func (c *genCluster) waitGates(typ api.ApplyRequest_Type, raw []byte) {
	for {
		c.gateMu.Lock()
		i := slices.IndexFunc(c.gates, func(g genGate) bool { return g.typ == typ && (g.match == nil || g.match(raw)) })
		var ch chan struct{}
		w := &genWaiting{typ: typ, raw: raw}
		if i >= 0 {
			ch = c.gates[i].ch
			c.waiting = append(c.waiting, w)
		}
		c.gateMu.Unlock()
		if ch == nil {
			return
		}
		<-ch
		c.gateMu.Lock()
		c.waiting = slices.DeleteFunc(c.waiting, func(o *genWaiting) bool { return o == w })
		c.gateMu.Unlock()
	}
}

func (c *genCluster) awaitHeld(typ api.ApplyRequest_Type, match func(raw []byte) bool, n int) {
	c.t.Helper()
	c.waitFor(fmt.Sprintf("%d entries %s to wait at their gate", n, typ), func() bool {
		c.gateMu.Lock()
		defer c.gateMu.Unlock()
		return n <= len(slices.DeleteFunc(slices.Clone(c.waiting), func(w *genWaiting) bool {
			return w.typ != typ || !match(w.raw)
		}))
	})
}

// aboutUnit matches a unit's start or completion, never its failure.
func aboutUnit(unit string) func(raw []byte) bool {
	return func(raw []byte) bool {
		var r struct {
			UnitID string `json:"unit_id"`
			Error  string `json:"error"`
		}
		return json.Unmarshal(raw, &r) == nil && r.UnitID == unit && r.Error == ""
	}
}

func (c *genCluster) holdUnit(typ api.ApplyRequest_Type, unit string) func() {
	return c.hold(typ, aboutUnit(unit))
}

// hold makes matching entries wait before they are applied; the returned func releases them.
func (c *genCluster) hold(typ api.ApplyRequest_Type, match func(raw []byte) bool) func() {
	ch := make(chan struct{})
	c.gateMu.Lock()
	c.gates = append(c.gates, genGate{typ: typ, match: match, ch: ch})
	c.gateMu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			c.gateMu.Lock()
			c.gates = slices.DeleteFunc(c.gates, func(g genGate) bool { return g.ch == ch })
			c.gateMu.Unlock()
			close(ch)
		})
	}
	c.t.Cleanup(release)
	return release
}

// holdUnitsOf holds every successful unit completion of the shards whose name starts with prefix.
func (c *genCluster) holdUnitsOf(prefix string) func() {
	return c.hold(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_UNIT_COMPLETED, func(raw []byte) bool {
		var r api.RecordDistributedTaskUnitCompletionRequest
		return json.Unmarshal(raw, &r) == nil && r.Error == "" && strings.HasPrefix(r.UnitId, prefix)
	})
}

// failUnitOf fails the run through one of the shard's units.
func (c *genCluster) failUnitOf(id, shard string) error {
	t := c.task(id)
	return c.apply(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_UNIT_COMPLETED, "", &api.RecordDistributedTaskUnitCompletionRequest{
		Namespace: ReindexNamespace, Id: id, Version: t.Version, NodeId: c.nodes[0].name,
		UnitId: MigrationUnitID(shard, c.nodes[0].name), Error: "test", FinishedAtUnixMillis: time.Now().UnixMilli(),
	})
}

// applyEntry with schemaOnly is the replay of a node's own log before its DB loads.
func (n *genNode) applyEntry(cmd *api.ApplyRequest, index uint64, schemaOnly bool) error {
	switch cmd.Type {
	case api.ApplyRequest_TYPE_ADD_CLASS:
		return n.sm.AddClass(cmd, n.name, schemaOnly, false)
	case api.ApplyRequest_TYPE_ADD_TENANT:
		return n.sm.AddTenants(cmd, schemaOnly)
	case api.ApplyRequest_TYPE_DELETE_TENANT:
		return n.sm.DeleteTenants(cmd, schemaOnly)
	case api.ApplyRequest_TYPE_REINDEX_FLIP:
		return n.sm.ReindexFlip(cmd, schemaOnly)
	case api.ApplyRequest_TYPE_UPDATE_PROPERTY:
		return n.sm.UpdateProperty(cmd, schemaOnly, false)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD:
		return n.dtm.AddTask(cmd, index)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_CANCEL:
		return n.dtm.CancelTask(cmd)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_CLEAN_UP:
		return n.dtm.CleanUpTask(cmd)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_UNIT_COMPLETED:
		return n.dtm.RecordUnitCompletion(cmd)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_UPDATE_UNIT_PROGRESS:
		return n.dtm.UpdateUnitProgress(cmd)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_MARK_FINALIZED:
		return n.dtm.MarkTaskFinalized(cmd)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_MARK_FAILED:
		return n.dtm.MarkTaskFailed(cmd)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_POST_COMPLETION_ACK:
		return n.dtm.RecordPostCompletionAck(cmd)
	case api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_PREPARATION_COMPLETE_ACK:
		return n.dtm.RecordPreparationCompleteAck(cmd)
	default:
		return fmt.Errorf("harness: no dispatch for %s", cmd.Type)
	}
}

// genClient is what cluster.Raft gives one node's scheduler and provider.
type genClient struct {
	c *genCluster
	n *genNode
}

func (r *genClient) dt(typ api.ApplyRequest_Type, sub any) error {
	return distributedtask.RehydratePermanentRejection(r.c.propose(r.n, typ, "", sub))
}

func (r *genClient) ListDistributedTaskHeaders(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return r.c.leader().dtm.ListDistributedTasks(ctx)
}

func (r *genClient) RecordDistributedTaskUnitCompletion(_ context.Context, ns, id string, v uint64, node, unit string) error {
	req := &api.RecordDistributedTaskUnitCompletionRequest{
		Namespace: ns, Id: id, Version: v, NodeId: node, UnitId: unit, FinishedAtUnixMillis: time.Now().UnixMilli(),
	}
	if r.n.lose != nil && r.n.lose(req) {
		return errors.New("the reply was lost")
	}
	return r.dt(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_UNIT_COMPLETED, req)
}

func (r *genClient) RecordDistributedTaskUnitFailure(_ context.Context, ns, id string, v uint64, node, unit, msg string) error {
	return r.dt(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_UNIT_COMPLETED, &api.RecordDistributedTaskUnitCompletionRequest{
		Namespace: ns, Id: id, Version: v, NodeId: node, UnitId: unit, Error: msg, FinishedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (r *genClient) UpdateDistributedTaskUnitProgress(_ context.Context, ns, id string, v uint64, node, unit string, p float32) error {
	if r.n.panicOnStart != nil && r.n.panicOnStart(unit) {
		panic("start claim panicked")
	}
	return r.dt(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_UPDATE_UNIT_PROGRESS, &api.UpdateDistributedTaskUnitProgressRequest{
		Namespace: ns, Id: id, Version: v, NodeId: node, UnitId: unit, Progress: p, UpdatedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (r *genClient) CleanUpDistributedTask(_ context.Context, ns, id string, v uint64) error {
	return r.dt(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_CLEAN_UP, &api.CleanUpDistributedTaskRequest{Namespace: ns, Id: id, Version: v})
}

func (r *genClient) MarkDistributedTaskFinalized(_ context.Context, ns, id string, v uint64) error {
	return r.dt(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_MARK_FINALIZED, &api.MarkTaskFinalizedRequest{
		Namespace: ns, Id: id, Version: v, FinalizedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (r *genClient) MarkDistributedTaskFailed(_ context.Context, ns, id string, v uint64, msg string) error {
	return r.dt(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_MARK_FAILED, &api.MarkTaskFailedRequest{
		Namespace: ns, Id: id, Version: v, Error: msg, FailedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (r *genClient) RecordDistributedTaskPostCompletionAck(_ context.Context, ns, id string, v uint64, group, node string, ok bool, msg string) error {
	return r.dt(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_POST_COMPLETION_ACK, &api.RecordDistributedTaskPostCompletionAckRequest{
		Namespace: ns, Id: id, Version: v, GroupId: group, NodeId: node, Success: ok, Error: msg, AckedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (r *genClient) RecordDistributedTaskPreparationCompleteAck(_ context.Context, ns, id string, v uint64, group, node string, ok bool, msg string) error {
	return r.dt(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_RECORD_PREPARATION_COMPLETE_ACK, &api.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace: ns, Id: id, Version: v, GroupId: &group, NodeId: node, Success: ok, Error: msg, AckedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (r *genClient) ReindexFlip(_ context.Context, class string, req api.ReindexFlipRequest) (uint64, error) {
	return 0, r.c.apply(api.ApplyRequest_TYPE_REINDEX_FLIP, class, req)
}

func (c *genCluster) addTenants(names ...string) {
	c.t.Helper()
	req := &api.AddTenantsRequest{ClusterNodes: c.nodeNames()}
	for _, name := range names {
		req.Tenants = append(req.Tenants, &api.Tenant{Name: name, Status: models.TenantActivityStatusHOT})
	}
	require.NoError(c.t, c.apply(api.ApplyRequest_TYPE_ADD_TENANT, c.class, req))
}

func (c *genCluster) deleteTenants(names ...string) error {
	return c.apply(api.ApplyRequest_TYPE_DELETE_TENANT, c.class, &api.DeleteTenantsRequest{Tenants: names})
}

func genPropFor(mt ReindexMigrationType) string {
	if mt == ReindexTypeEnableRangeable || mt == ReindexTypeRepairRangeable {
		return genNum
	}
	return genProp
}

// submit adds a run the way the submit handler does: no units, the barrier on.
func (c *genCluster) submit(mt ReindexMigrationType, tok string, tenants ...string) (string, error) {
	c.t.Helper()
	c.runs++
	id := fmt.Sprintf("%s:%s:%d", c.class, mt, c.runs)
	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: mt, Collection: c.class, Properties: []string{genPropFor(mt)}, TargetTokenization: tok,
		BucketStrategy: "inverted", Tenants: tenants, ViewServed: true,
	})
	require.NoError(c.t, err)
	return id, c.apply(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, "", &api.AddDistributedTaskRequest{
		Namespace: ReindexNamespace, Id: id, Payload: payload, NeedsPreparationBarrier: true,
		SubmittedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (c *genCluster) cancel(id string) error {
	return c.apply(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_CANCEL, "", &api.CancelDistributedTaskRequest{
		Namespace: ReindexNamespace, Id: id, Version: c.task(id).Version, CancelledAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (c *genCluster) fail(id string) error {
	return c.apply(api.ApplyRequest_TYPE_DISTRIBUTED_TASK_MARK_FAILED, "", &api.MarkTaskFailedRequest{
		Namespace: ReindexNamespace, Id: id, Version: c.task(id).Version, Error: "test", FailedAtUnixMillis: time.Now().UnixMilli(),
	})
}

func (c *genCluster) leader() *genNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if !n.down && !n.lagging {
			return n
		}
	}
	return c.nodes[0]
}

func (c *genCluster) isDown(n *genNode) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return n.down
}

// crash stops a node getting entries; what it proposed before still reaches the log.
func (c *genCluster) crash(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[i].down = true
}

type genRestart struct {
	edit   func(root string) // changes the crashed node's files before they open
	loaded func(n *genNode)  // runs once the DB loaded, before the log catches up
	lag    bool              // keeps the log behind until catchUp
}

// restart starts a crashed node over a copy of its files, memtables never flushed. Its own log
// replays before the DB loads, as a RAFT node's does, then the rest of the log catches up.
func (c *genCluster) restart(i int, o genRestart) *genNode {
	c.t.Helper()
	old := c.nodes[i]
	c.mu.Lock()
	old.gone = true
	c.mu.Unlock()
	root := c.t.TempDir()
	require.NoError(c.t, copyTree(old.root, root))
	old.stop()
	if o.edit != nil {
		o.edit(root)
	}
	n := c.newNode(old.name, root)
	c.mu.Lock()
	for n.applied < old.applied {
		_ = n.applyNext(true) // the replay meets refusals the first apply met too
	}
	c.mu.Unlock()
	n.sm.ReloadDBFromSchema()
	if o.loaded != nil {
		o.loaded(n)
	}
	c.mu.Lock()
	c.nodes[i] = n
	n.lagging = true
	c.mu.Unlock()
	if !o.lag {
		c.catchUp(i)
	}
	n.start()
	return n
}

func (c *genCluster) catchUp(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[i]
	for int(n.applied) < len(c.log) {
		_ = n.applyNext(false)
	}
	n.lagging = false
}

// copyTree copies files a running node may be changing; one gone meanwhile is skipped.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), data, 0o644)
	})
}

// awaitStaged waits until the node's view of shard stages a build. It asks for
// no refresh, so before the scheduler starts only the load's own notice stages it.
func (n *genNode) awaitStaged(shard string) {
	n.c.t.Helper()
	n.c.waitFor("a staged build on "+shard, func() bool { return len(n.shard(shard).loadView().staged) > 0 })
}

// commit releases held entries and waits until want entries of typ about unit are in the log.
func (c *genCluster) commit(release func(), typ api.ApplyRequest_Type, unit string, want int) {
	c.t.Helper()
	release()
	c.waitFor(fmt.Sprintf("%d entries %s of %s in the log", want, typ, unit), func() bool { return c.unitEntries(typ, unit) == want })
}

func (c *genCluster) unitEntries(typ api.ApplyRequest_Type, unit string) int {
	about := aboutUnit(unit)
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.log {
		if e.Type == typ && about(e.SubCommand) {
			n++
		}
	}
	return n
}

func (n *genNode) hasError(text string) bool {
	return slices.ContainsFunc(n.logs.AllEntries(), func(e *logrus.Entry) bool {
		return e.Level <= logrus.WarnLevel && strings.Contains(e.Message, text)
	})
}

// task is the leader's copy of a task, nil once it is gone.
func (c *genCluster) task(id string) *distributedtask.Task {
	tasks, _ := c.leader().dtm.ListDistributedTasks(c.ctx)
	for _, t := range tasks[ReindexNamespace] {
		if t.ID == id {
			return t
		}
	}
	return nil
}

func (c *genCluster) generation(id string) uint64 {
	h, _ := ReindexHeaderOf(c.task(id))
	return h.Generation
}

func (c *genCluster) waitFor(what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitGone waits until a full run finished and its end rule deleted it.
func (c *genCluster) waitGone(id string) {
	c.t.Helper()
	c.waitFor("run "+id+" to finish", func() bool {
		t := c.task(id)
		if t != nil && t.Status.IsTerminal() && t.Status != distributedtask.TaskStatusFinished {
			c.t.Fatalf("run %s ended %s: %s", id, t.Status, t.Error)
		}
		return t == nil
	})
}

func (c *genCluster) waitStatus(id string, st distributedtask.TaskStatus) {
	c.t.Helper()
	c.waitFor(fmt.Sprintf("run %s to reach %s", id, st), func() bool {
		t := c.task(id)
		return t != nil && t.Status == st
	})
}

func (c *genCluster) waitSwitched(id string, groups ...string) {
	c.t.Helper()
	c.waitFor(fmt.Sprintf("run %s to switch %v", id, groups), func() bool {
		t := c.task(id)
		return t != nil && !slices.ContainsFunc(groups, func(g string) bool { return !t.GroupSwapDecided(g) })
	})
}

func (c *genCluster) classProp(name string) *models.Property {
	return c.nodes[0].sr.ReadOnlyClass(c.class).Properties[slices.IndexFunc(
		c.nodes[0].sr.ReadOnlyClass(c.class).Properties, func(p *models.Property) bool { return p.Name == name })]
}

func (n *genNode) index() *Index { return n.db.GetIndex(schema.ClassName(n.c.class)) }

func (n *genNode) shard(name string) *Shard {
	n.c.t.Helper()
	s := n.index().shards.Load(name)
	require.NotNil(n.c.t, s, "shard %s on %s", name, n.name)
	sh, err := unwrapShard(n.c.ctx, s)
	require.NoError(n.c.t, err)
	return sh
}

// reload shuts a shard down and loads it again from disk.
func (n *genNode) reload(name string) {
	n.c.t.Helper()
	idx := n.index()
	require.NoError(n.c.t, n.shard(name).Shutdown(n.c.ctx))
	idx.shards.LoadAndDelete(name)
	shd, err := idx.initShard(n.c.ctx, name, n.sr.ReadOnlyClass(n.c.class), nil, true, true)
	require.NoError(n.c.t, err)
	idx.shards.Store(name, shd)
}

func (c *genCluster) reloadAll(shard string) {
	for _, n := range c.nodes {
		n.reload(shard)
	}
}

func genObject(class, tenant, text string, num int64) *storobj.Object {
	now := time.Now().UnixMilli()
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID: strfmt.UUID(uuid.NewString()), Class: class, Tenant: tenant, CreationTimeUnix: now, LastUpdateTimeUnix: now,
			Properties: map[string]interface{}{genProp: text, genNum: float64(num)},
		},
	}
}

// put writes one object to every replica of the shard, as replication would.
func (c *genCluster) put(shard string, obj *storobj.Object) {
	c.t.Helper()
	for _, n := range c.nodes {
		if c.isDown(n) {
			continue
		}
		cp := *obj
		require.NoError(c.t, n.shard(shard).PutObject(c.ctx, &cp))
	}
}

func (c *genCluster) write(shard string, count int) []*storobj.Object {
	c.t.Helper()
	tenant := shard
	if !c.nodes[0].sr.MultiTenancy(c.class).Enabled {
		tenant = ""
	}
	out := make([]*storobj.Object, count)
	for i := range count {
		out[i] = genObject(c.class, tenant, genValue, int64(i))
		c.put(shard, out[i])
	}
	return out
}

func (n *genNode) filter(prop string, value any, dt schema.DataType, op filters.Operator) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: op, On: &filters.Path{Class: schema.ClassName(n.c.class), Property: schema.PropertyName(prop)},
		Value: &filters.Value{Value: value, Type: dt},
	}}
}

func (n *genNode) count(shard string, f *filters.LocalFilter) int {
	n.c.t.Helper()
	s := n.shard(shard)
	v := s.acquireView()
	defer v.release()
	list, err := s.buildAllowList(n.c.ctx, v, f, additional.Properties{})
	require.NoError(n.c.t, err)
	defer list.Close()
	return list.Len()
}

func (n *genNode) bm25(shard, query string) int {
	n.c.t.Helper()
	objs, _, err := n.shard(shard).ObjectSearch(n.c.ctx, 100000, nil,
		&searchparams.KeywordRanking{Type: "bm25", Query: query, Properties: []string{genProp}}, nil, nil,
		additional.Properties{}, nil)
	require.NoError(n.c.t, err)
	return len(objs)
}

// genReads is what every replica of a shard answers.
type genReads struct{ filterHello, filterValue, bm25Hello, numBelow3 int }

func (c *genCluster) reads(shard string, msg string) []genReads {
	c.t.Helper()
	var out []genReads
	for _, n := range c.nodes {
		out = append(out, genReads{
			filterHello: n.count(shard, n.filter(genProp, "hello", schema.DataTypeText, filters.OperatorEqual)),
			filterValue: n.count(shard, n.filter(genProp, genValue, schema.DataTypeText, filters.OperatorEqual)),
			bm25Hello:   n.bm25(shard, "hello"),
			numBelow3:   n.count(shard, n.filter(genNum, 3, schema.DataTypeInt, filters.OperatorLessThan)),
		})
	}
	return out
}

func (c *genCluster) requireReads(shard string, want genReads, msg string) {
	c.t.Helper()
	for i, got := range c.reads(shard, msg) {
		require.Equal(c.t, want, got, "%s: %s on %s", msg, shard, c.nodes[i].name)
	}
}

// servedFolders lists, per node, the folder each index of prop serves on the shard.
func (c *genCluster) servedFolders(shard, prop string, indexType string) []string {
	var out []string
	for _, n := range c.nodes {
		v := n.shard(shard).loadView()
		out = append(out, servedFolder(v, reindexBucketName(prop, indexType, 0)))
	}
	return out
}

// waitServed waits until every node's view of the shard serves gen of prop's
// index types; the refresh loop publishes a decision or a flip shortly after it applies.
func (c *genCluster) waitServed(shard, prop string, gen uint64, indexTypes ...string) {
	c.t.Helper()
	c.waitFor(fmt.Sprintf("%s to serve generation %d", shard, gen), func() bool {
		return !slices.ContainsFunc(indexTypes, func(it string) bool {
			name := reindexBucketName(prop, it, gen)
			return slices.ContainsFunc(c.servedFolders(shard, prop, it), func(got string) bool { return got != name })
		})
	})
}

func (c *genCluster) folderExists(n *genNode, shard, name string) bool {
	_, err := os.Stat(filepath.Join(n.index().path(), shard, "lsm", name))
	return err == nil
}
