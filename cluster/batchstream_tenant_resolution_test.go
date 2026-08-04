//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / _ \/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /  __/ (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ \___|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"context"
	"fmt"
	"io"
	"math"
	"runtime/metrics"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	grpcAuth "github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
	grpcBatch "github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	repoDB "github.com/weaviate/weaviate/adapters/repos/db"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	usecaseCluster "github.com/weaviate/weaviate/usecases/cluster"
	usecaseClusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	batchStreamTenantResolutionClass   = "BatchStreamTenantResolution"
	batchStreamTenantResolutionNodeID  = "node1"
	batchStreamMissingTenant           = "missing-tenant"
	batchStreamInactiveTenant          = "inactive-tenant"
	batchStreamTenantResolutionTenants = 10
)

// TestBatchStreamMultiTenant exercises the public BatchStream path against a
// single-node Raft schema and a real DB. Every request has one write-side
// activation lookup, followed by one resolver lookup containing the unique
// tenant names rather than an individual lookup for every object.
func TestBatchStreamMultiTenant(t *testing.T) {
	for _, objectCount := range []int{100, 200, 1000} {
		t.Run(fmt.Sprintf("objects=%d", objectCount), func(t *testing.T) {
			fixture := newBatchStreamTenantResolutionFixture(t)
			objects := fixture.objects(objectCount)

			fixture.trace.Reset()
			replies, err := fixture.write(objects)
			require.NoError(t, err)
			requireBatchStreamResults(t, replies, len(objects), 0)
			fixture.requirePersisted(t, objects)
			fixture.requireBulkResolution(t, fixture.tenants)
		})
	}
}

func TestBatchStreamMultiTenantMixedValidity(t *testing.T) {
	fixture := newBatchStreamTenantResolutionFixtureWithInactiveTenant(t)
	objects := fixture.objects(5)
	objects[1].Tenant = batchStreamMissingTenant
	objects[2].Tenant = batchStreamInactiveTenant
	objects[3].Tenant = ""

	fixture.trace.Reset()
	replies, err := fixture.write(objects)
	require.NoError(t, err)
	requireBatchStreamResults(t, replies, 2, 3)
	fixture.requireBulkResolution(t, []string{objects[0].Tenant, batchStreamMissingTenant, batchStreamInactiveTenant, objects[4].Tenant})
	fixture.requirePersisted(t, []*pb.BatchObject{objects[0], objects[4]})

	errorsByUUID := batchStreamErrorsByUUID(replies)
	require.Contains(t, errorsByUUID[objects[1].Uuid], "tenant not found")
	require.Contains(t, errorsByUUID[objects[2].Uuid], "tenant not active")
	require.Contains(t, errorsByUUID[objects[3].Uuid], "without tenant")
}

// BenchmarkBatchStreamTenantResolution measures a complete valid BatchStream
// write. The fixture includes gRPC request parsing, the objects batch use case,
// DB batch persistence, schema.Manager, and a one-node Raft query endpoint.
// Tenant-status spans are recorded so the benchmark can report both request
// latency and the number of resolver lookups made per operation.
func BenchmarkBatchStreamTenantResolution(b *testing.B) {
	for _, objectCount := range []int{100, 200, 1000} {
		b.Run(fmt.Sprintf("objects=%d", objectCount), func(b *testing.B) {
			fixture := newBatchStreamTenantResolutionFixture(b)
			objects := fixture.objects(objectCount)

			// Warm shards, the stream worker, and the upsert state outside the
			// timed request path. Callers of BatchStream retain these resources
			// between requests too.
			fixture.trace.Reset()
			replies, err := fixture.write(objects)
			require.NoError(b, err)
			requireBatchStreamResults(b, replies, len(objects), 0)

			latencies := make([]time.Duration, 0, b.N)
			resolutionQueries := 0
			cpuStarted := batchStreamUserCPUTime()
			b.ResetTimer()
			for b.Loop() {
				fixture.trace.Reset()
				started := time.Now()
				replies, err := fixture.write(objects)
				latencies = append(latencies, time.Since(started))
				if err != nil {
					b.Fatal(err)
				}
				if successes, failures := batchStreamResultCounts(replies); successes != len(objects) || failures != 0 {
					b.Fatalf("unexpected BatchStream result: successes=%d failures=%d", successes, failures)
				}
				resolutionQueries += fixture.trace.ResolutionQueries()
			}
			b.StopTimer()
			cpuElapsed := batchStreamUserCPUTime() - cpuStarted

			b.ReportMetric(float64(percentileLatency(latencies, 0.50).Nanoseconds()), "p50-ns/op")
			b.ReportMetric(float64(percentileLatency(latencies, 0.95).Nanoseconds()), "p95-ns/op")
			b.ReportMetric(float64(resolutionQueries)/float64(len(latencies)), "resolution-queries/op")
			b.ReportMetric(float64(cpuElapsed.Nanoseconds())/float64(len(latencies)), "go-user-cpu-ns/op")
		})
	}
}

// batchStreamUserCPUTime returns the Go runtime's cumulative user CPU time.
// It includes user code and runtime work that executes while the benchmarked
// BatchStream request is in flight, making it comparable across the benchmark
// arms without changing the request path.
func batchStreamUserCPUTime() time.Duration {
	samples := [1]metrics.Sample{{Name: "/cpu/classes/user:cpu-seconds"}}
	metrics.Read(samples[:])
	return time.Duration(samples[0].Value.Float64() * float64(time.Second))
}

func percentileLatency(latencies []time.Duration, percentile float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	ordered := append([]time.Duration(nil), latencies...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := max(0, int(math.Ceil(float64(len(ordered))*percentile))-1)
	return ordered[index]
}

type tenantStatusSpan struct {
	tenants  []string
	started  time.Time
	finished time.Time
}

// tracedSchemaManager decorates the production Raft API used by
// schema.Manager. Embedding the full interface keeps every non-query method on
// the real implementation while making the leader-directed tenant-status query
// observable in the fixture.
type tracedSchemaManager struct {
	schemaUC.SchemaManager

	mu    sync.Mutex
	spans []tenantStatusSpan
}

func (m *tracedSchemaManager) QueryTenantsShards(class string, tenants ...string) (map[string]string, uint64, error) {
	started := time.Now()
	statuses, version, err := m.SchemaManager.QueryTenantsShards(class, tenants...)
	finished := time.Now()

	m.mu.Lock()
	m.spans = append(m.spans, tenantStatusSpan{
		tenants:  append([]string(nil), tenants...),
		started:  started,
		finished: finished,
	})
	m.mu.Unlock()

	return statuses, version, err
}

func (m *tracedSchemaManager) Reset() {
	m.mu.Lock()
	m.spans = m.spans[:0]
	m.mu.Unlock()
}

func (m *tracedSchemaManager) Spans() []tenantStatusSpan {
	m.mu.Lock()
	defer m.mu.Unlock()

	spans := make([]tenantStatusSpan, len(m.spans))
	for i, span := range m.spans {
		spans[i] = tenantStatusSpan{
			tenants:  append([]string(nil), span.tenants...),
			started:  span.started,
			finished: span.finished,
		}
	}
	return spans
}

// ResolutionQueries excludes the one write-side activation check performed by
// BatchManager before Index.putObjectBatch resolves a target shard. The fixture
// only calls this for one valid class batch, where that activation check is
// always present and the remaining queries are resolver work.
func (m *tracedSchemaManager) ResolutionQueries() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return max(0, len(m.spans)-1)
}

func (f *batchStreamTenantResolutionFixture) requireBulkResolution(t testing.TB, tenants []string) {
	t.Helper()

	spans := f.trace.Spans()
	require.Len(t, spans, 2, "one activation lookup and one resolver lookup")
	require.ElementsMatch(t, tenants, spans[1].tenants)
	require.LessOrEqual(t, spans[0].finished, spans[1].started, "resolver lookup must follow activation")
}

// batchStreamSchemaReader combines the local Raft schema reader with Raft's
// WaitForUpdate method, matching the production schema.Manager wiring.
type batchStreamSchemaReader struct {
	clusterSchema.SchemaReader
	*Raft
}

func (r batchStreamSchemaReader) WaitForUpdate(ctx context.Context, version uint64) error {
	return r.Raft.WaitForUpdate(ctx, version)
}

type batchStreamStore struct {
	store   *Store
	indexer *fakes.MockSchemaExecutor
	logger  *logrus.Logger
}

// batchStreamClusterState supplies the memberlist methods used by the schema
// handler while keeping the fixture single-node and in-process.
type batchStreamClusterState struct {
	usecaseCluster.NodeSelector
}

func (s batchStreamClusterState) Hostnames() []string { return s.AllHostnames() }

func (s batchStreamClusterState) AllNames() []string { return s.AllHostnames() }

func (batchStreamClusterState) SchemaSyncIgnored() bool { return false }

func (batchStreamClusterState) SkipSchemaRepair() bool { return false }

func newBatchStreamStore(t testing.TB, nodeID string) *batchStreamStore {
	t.Helper()

	indexer := fakes.NewMockSchemaExecutor()
	parser := fakes.NewMockParser()
	logger, _ := test.NewNullLogger()
	replicationFSM := clusterSchema.NewMockreplicationFSM(t)

	indexer.On("Open", mock.Anything).Return(nil)
	indexer.On("Close", mock.Anything).Return(nil)
	indexer.On("AddClass", mock.Anything).Return(nil)
	indexer.On("RestoreClassDir", mock.Anything).Return(nil)
	indexer.On("UpdateClass", mock.Anything).Return(nil)
	indexer.On("DeleteClass", mock.Anything, mock.Anything).Return(nil)
	indexer.On("AddProperty", mock.Anything, mock.Anything).Return(nil)
	indexer.On("UpdateProperty", mock.Anything, mock.Anything).Return(nil)
	indexer.On("UpdateShardStatus", mock.Anything).Return(nil)
	indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
	indexer.On("UpdateTenants", mock.Anything, mock.Anything).Return(nil)
	indexer.On("UpdateTenantsProcess", mock.Anything, mock.Anything).Return(nil)
	indexer.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
	indexer.On("TriggerSchemaUpdateCallbacks").Return()
	indexer.On("AddReplicaToShard", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	indexer.On("DeleteReplicaFromShard", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	indexer.On("ReconcileAsyncReplicationForShard", mock.Anything, mock.Anything).Return(nil)
	indexer.On("LoadShard", mock.Anything, mock.Anything).Return()
	indexer.On("ShutdownShard", mock.Anything, mock.Anything).Return()

	parser.On("ParseClass", mock.Anything).Return(nil)
	parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)

	replicationFSM.EXPECT().HasActiveReplicationForCollection(mock.Anything).Return(false).Maybe()
	replicationFSM.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()

	cfg := Config{
		WorkDir:                t.TempDir(),
		NodeID:                 nodeID,
		Host:                   "localhost",
		RaftPort:               utils.MustGetFreeTCPPort(),
		Voter:                  true,
		BootstrapExpect:        1,
		HeartbeatTimeout:       time.Second,
		ElectionTimeout:        time.Second,
		SnapshotInterval:       2 * time.Second,
		SnapshotThreshold:      125,
		DB:                     indexer,
		Parser:                 parser,
		NodeSelector:           usecaseClusterMocks.NewMockNodeSelector("localhost"),
		Logger:                 logger,
		ConsistencyWaitTimeout: 50 * time.Millisecond,
		NamespacesController:   namespaces.NewController(logger),
		TelemetryEnabled:       true,
	}

	store := NewFSM(cfg, nil, nil, prometheus.NewPedanticRegistry())
	store.schemaManager.SetReplicationFSM(replicationFSM)
	return &batchStreamStore{store: &store, indexer: indexer, logger: logger}
}

type batchStreamTenantResolutionFixture struct {
	className string
	tenants   []string

	trace        *tracedSchemaManager
	database     *repoDB.DB
	stream       *grpcBatch.StreamHandler
	drain        grpcBatch.Drain
	raft         *Raft
	shutdownOnce sync.Once
}

func newBatchStreamTenantResolutionFixture(t testing.TB) *batchStreamTenantResolutionFixture {
	return newBatchStreamTenantResolutionFixtureWithExtraTenant(t, "", "")
}

func newBatchStreamTenantResolutionFixtureWithInactiveTenant(t testing.TB) *batchStreamTenantResolutionFixture {
	return newBatchStreamTenantResolutionFixtureWithExtraTenant(t, batchStreamInactiveTenant, models.TenantActivityStatusCOLD)
}

func newBatchStreamTenantResolutionFixtureWithExtraTenant(t testing.TB, extraTenant, extraTenantStatus string) *batchStreamTenantResolutionFixture {
	t.Helper()

	ctx := context.Background()
	storeFixture := newBatchStreamStore(t, batchStreamTenantResolutionNodeID)
	nodeSelector := usecaseClusterMocks.NewMockNodeSelector(batchStreamTenantResolutionNodeID)
	raftService := NewRaft(nodeSelector, storeFixture.store, nil)

	require.NoError(t, raftService.Open(ctx, storeFixture.indexer))
	require.NoError(t, storeFixture.store.Notify(batchStreamTenantResolutionNodeID,
		fmt.Sprintf("%s:%d", storeFixture.store.cfg.Host, storeFixture.store.cfg.RaftPort)))
	require.Eventually(t, raftService.IsLeader, 10*time.Second, 25*time.Millisecond, "Raft node did not become leader")
	require.Eventually(t, raftService.Ready, 10*time.Second, 25*time.Millisecond, "Raft node did not become ready")

	trace := &tracedSchemaManager{SchemaManager: raftService}
	clusterState := batchStreamClusterState{NodeSelector: nodeSelector}
	collectionRetrievalStrategy := configRuntime.NewFeatureFlag(
		configRuntime.CollectionRetrievalStrategyLDKey,
		string(configRuntime.LocalOnly),
		nil,
		"",
		storeFixture.logger,
	)
	schemaReader := batchStreamSchemaReader{
		SchemaReader: raftService.SchemaReader(),
		Raft:         raftService,
	}
	schemaManager, err := schemaUC.NewManager(
		nil,
		trace,
		schemaReader,
		nil,
		storeFixture.logger,
		&authorization.DummyAuthorizer{},
		nil,
		config.Config{},
		nil,
		nil,
		nil,
		nil,
		clusterState,
		nil,
		schemaUC.Parser{},
		collectionRetrievalStrategy,
		nil,
		nil,
	)
	require.NoError(t, err)

	eagerShards := false
	database, err := repoDB.New(storeFixture.logger, batchStreamTenantResolutionNodeID, repoDB.Config{
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10_000,
		MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards:      &eagerShards,
	}, nil, nodeSelector, nil, nil, nil, memwatch.NewDummyMonitor(), nodeSelector, schemaReader, raftService.ReplicationFsm())
	require.NoError(t, err)
	database.SetSchemaGetter(schemaManager)
	require.NoError(t, database.WaitForStartup(ctx))

	tenants := batchStreamTenantNames()
	class := batchStreamTenantResolutionClassModel()
	state := batchStreamTenantResolutionState(tenants)
	if extraTenant != "" {
		state.Physical[extraTenant] = sharding.Physical{
			Name:           extraTenant,
			OwnsPercentage: 1,
			BelongsToNodes: []string{batchStreamTenantResolutionNodeID},
			Status:         extraTenantStatus,
		}
	}
	_, err = raftService.AddClass(ctx, class, state)
	require.NoError(t, err)
	require.NoError(t, repoDB.NewMigrator(database, storeFixture.logger, batchStreamTenantResolutionNodeID).AddClass(ctx, class))

	weaviateConfig := &config.WeaviateConfig{Config: config.Config{
		AutoSchema: config.AutoSchema{Enabled: configRuntime.NewDynamicValue(false)},
	}}
	authorizer := &authorization.DummyAuthorizer{}
	modules := noOpModulesProvider{}
	autoSchemaManager := objects.NewAutoSchemaManager(schemaManager, database, weaviateConfig, storeFixture.logger, prometheus.NewPedanticRegistry())
	batchManager := objects.NewBatchManager(database, modules, schemaManager, weaviateConfig, storeFixture.logger, authorizer, nil, autoSchemaManager)
	authenticator := grpcAuth.NewHandler(true, nil)
	batchHandler := grpcBatch.NewHandler(authorizer, batchManager, storeFixture.logger, authenticator, schemaManager, false)
	stream, drain := grpcBatch.Start(authenticator, authorizer, batchHandler, schemaManager, prometheus.NewPedanticRegistry(), 1, storeFixture.logger, false)

	fixture := &batchStreamTenantResolutionFixture{
		className: batchStreamTenantResolutionClass,
		tenants:   tenants,
		trace:     trace,
		database:  database,
		stream:    stream,
		drain:     drain,
		raft:      raftService,
	}
	t.Cleanup(fixture.shutdown)
	return fixture
}

func (f *batchStreamTenantResolutionFixture) shutdown() {
	f.shutdownOnce.Do(func() {
		f.drain()
		_ = f.database.Shutdown(context.Background())
		_ = f.raft.Close(context.Background())
	})
}

func batchStreamTenantNames() []string {
	tenants := make([]string, batchStreamTenantResolutionTenants)
	for i := range tenants {
		tenants[i] = fmt.Sprintf("tenant-%02d", i)
	}
	return tenants
}

func batchStreamTenantResolutionClassModel() *models.Class {
	return &models.Class{
		Class: batchStreamTenantResolutionClass,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		ReplicationConfig:   &models.ReplicationConfig{Factor: 1},
		VectorIndexType:     "hnsw",
		VectorIndexConfig:   enthnsw.UserConfig{Skip: true},
		Vectorizer:          "none",
	}
}

func batchStreamTenantResolutionState(tenants []string) *sharding.State {
	state := &sharding.State{
		IndexID:             batchStreamTenantResolutionClass,
		PartitioningEnabled: true,
		ReplicationFactor:   1,
		Physical:            make(map[string]sharding.Physical, len(tenants)),
	}
	for _, tenant := range tenants {
		state.Physical[tenant] = sharding.Physical{
			Name:           tenant,
			OwnsPercentage: 1,
			BelongsToNodes: []string{batchStreamTenantResolutionNodeID},
			Status:         models.TenantActivityStatusHOT,
		}
	}
	return state
}

func (f *batchStreamTenantResolutionFixture) objects(count int) []*pb.BatchObject {
	objects := make([]*pb.BatchObject, count)
	for i := range objects {
		objects[i] = &pb.BatchObject{
			Uuid:       uuid.NewString(),
			Collection: f.className,
			Tenant:     f.tenants[i%len(f.tenants)],
		}
	}
	return objects
}

func (f *batchStreamTenantResolutionFixture) write(objects []*pb.BatchObject) ([]*pb.BatchStreamReply, error) {
	stream := newInMemoryBatchStream(objects)
	if err := f.stream.Handle(stream); err != nil {
		return stream.Replies(), err
	}
	return stream.Replies(), nil
}

func (f *batchStreamTenantResolutionFixture) requirePersisted(t testing.TB, objects []*pb.BatchObject) {
	t.Helper()

	expected := make(map[string]int, len(f.tenants))
	for _, object := range objects {
		expected[object.Tenant]++
	}
	for tenant, count := range expected {
		actual, err := f.database.CountObjects(context.Background(), f.className, tenant)
		require.NoError(t, err)
		require.Equal(t, count, actual, "tenant %q", tenant)
	}
}

// inMemoryBatchStream drives the generated server-side interface without a
// network transport. Its requests and replies follow the same bidi stream
// sequencing as a gRPC client: Start, one Data message, then EOF.
type inMemoryBatchStream struct {
	grpc.ServerStream

	ctx context.Context

	mu       sync.Mutex
	requests []*pb.BatchStreamRequest
	next     int
	replies  []*pb.BatchStreamReply

	resultsSent     chan struct{}
	resultsSentOnce sync.Once
}

func newInMemoryBatchStream(objects []*pb.BatchObject) *inMemoryBatchStream {
	return &inMemoryBatchStream{
		ctx:         context.Background(),
		resultsSent: make(chan struct{}),
		requests: []*pb.BatchStreamRequest{
			{
				Message: &pb.BatchStreamRequest_Start_{
					Start: &pb.BatchStreamRequest_Start{},
				},
			},
			{
				Message: &pb.BatchStreamRequest_Data_{
					Data: &pb.BatchStreamRequest_Data{
						Objects: &pb.BatchStreamRequest_Data_Objects{Values: objects},
					},
				},
			},
		},
	}
}

func (s *inMemoryBatchStream) Context() context.Context { return s.ctx }

func (s *inMemoryBatchStream) Recv() (*pb.BatchStreamRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.next < len(s.requests) {
		request := s.requests[s.next]
		s.next++
		return request, nil
	}

	// A real client normally keeps its receive half open until it has seen
	// the result for submitted data. Do the same here so the server's
	// receiver does not close its reporting queue before the worker replies.
	s.mu.Unlock()
	<-s.resultsSent
	s.mu.Lock()
	return nil, io.EOF
}

func (s *inMemoryBatchStream) Send(reply *pb.BatchStreamReply) error {
	s.mu.Lock()
	s.replies = append(s.replies, reply)
	s.mu.Unlock()
	if reply.GetResults() != nil {
		s.resultsSentOnce.Do(func() { close(s.resultsSent) })
	}
	return nil
}

func (s *inMemoryBatchStream) Replies() []*pb.BatchStreamReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pb.BatchStreamReply(nil), s.replies...)
}

func batchStreamResultCounts(replies []*pb.BatchStreamReply) (successes, failures int) {
	for _, reply := range replies {
		results := reply.GetResults()
		if results == nil {
			continue
		}
		successes += len(results.Successes)
		failures += len(results.Errors)
	}
	return successes, failures
}

func requireBatchStreamResults(t testing.TB, replies []*pb.BatchStreamReply, successes, failures int) {
	t.Helper()
	gotSuccesses, gotFailures := batchStreamResultCounts(replies)
	require.Equal(t, successes, gotSuccesses, "successful objects")
	require.Equal(t, failures, gotFailures, "failed objects")
}

func batchStreamErrorsByUUID(replies []*pb.BatchStreamReply) map[string]string {
	errorsByUUID := make(map[string]string)
	for _, reply := range replies {
		results := reply.GetResults()
		if results == nil {
			continue
		}
		for _, resultErr := range results.Errors {
			errorsByUUID[resultErr.GetUuid()] = resultErr.GetError()
		}
	}
	return errorsByUUID
}

// noOpModulesProvider is sufficient for a collection configured with the
// built-in "none" vectorizer. It makes the fixture exercise the real batch
// use case rather than bypassing it while keeping external modules out of the
// benchmarked path.
type noOpModulesProvider struct{}

func (noOpModulesProvider) GetObjectAdditionalExtend(_ context.Context, in *search.Result, _ map[string]interface{}) (*search.Result, error) {
	return in, nil
}

func (noOpModulesProvider) ListObjectsAdditionalExtend(_ context.Context, in search.Results, _ map[string]interface{}) (search.Results, error) {
	return in, nil
}

func (noOpModulesProvider) UsingRef2Vec(string) bool { return false }

func (noOpModulesProvider) UpdateVector(_ context.Context, _ *models.Object, _ *models.Class, _ modulecapabilities.FindObjectFn, _ logrus.FieldLogger) error {
	return nil
}

func (noOpModulesProvider) BatchUpdateVector(_ context.Context, _ *models.Class, _ []*models.Object, _ modulecapabilities.FindObjectFn, _ logrus.FieldLogger) (map[int]error, error) {
	return nil, nil
}

func (noOpModulesProvider) VectorizerName(string) (string, error) { return "none", nil }
