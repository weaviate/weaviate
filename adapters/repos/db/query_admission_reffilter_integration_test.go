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

//go:build integrationTest

package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/queryadmission"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestQueryAdmissionRefFilterNoWedge is the regression guard for the admission
// wedge caused by dropping the admitted ctx before a nested cross-reference
// search (inverted.Searcher.extractReferenceFilter used context.TODO()).
//
// With the ctx dropped, the nested Author search re-enters Admit as a FRESH
// acquirer (the re-entrancy marker is lost), so a single ref-filter query needs
// two grants. Under a small node budget the parents hold every unit while their
// nested children park on a never-expiring context.TODO() — the parents can
// never release, capacity is stuck forever, and only a restart recovers.
//
// The test bursts many ref-filter queries against a budget-2 node and asserts
// (a) every query completes or sheds within a bounded time (no wedge) and
// (b) the limiter drains back to zero used budget (no grant leak), read from
// the real prometheus gauge.
func TestQueryAdmissionRefFilterNoWedge(t *testing.T) {
	const (
		budget     = 2
		maxQueue   = 128
		numAuthors = 200
		numQueries = 16
	)

	ctx := context.Background()
	repo, shard, reg := setupRefAdmissionRepo(t, budget, maxQueue, numAuthors)
	defer repo.Shutdown(ctx)

	refFilter := articleRefNameLike("*a*")

	// qctx lets us release any still-queued waiters on failure so the test
	// binary is not left with a large pile of parked goroutines.
	qctx, qcancel := context.WithCancel(ctx)
	defer qcancel()

	var (
		wg     sync.WaitGroup
		badErr atomic.Value
	)
	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A generous per-query deadline: in the buggy build the parent that
			// wedges is blocked inside a nested Admit on context.TODO(), which
			// this deadline cannot interrupt — that is exactly the wedge we
			// detect via the outer timeout below.
			cctx, cancel := context.WithTimeout(qctx, 60*time.Second)
			defer cancel()
			_, _, err := shard.ObjectSearch(cctx, 100, refFilter, nil, nil, nil,
				additional.Properties{}, []string{"title"})
			switch {
			case err == nil:
			case errors.Is(err, queryadmission.ErrOverloaded):
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			default:
				badErr.Store(err.Error())
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		qcancel()
		t.Fatalf("admission wedge: %d ref-filter queries did not complete within 30s; "+
			"the nested cross-reference search re-entered Admit as a fresh acquirer and "+
			"parked on a never-expiring ctx while the parent held its grant", numQueries)
	}

	if v := badErr.Load(); v != nil {
		t.Fatalf("unexpected ref-filter search error (want nil / overloaded / deadline): %v", v)
	}

	// (b) No grant leak: once every query has returned, every unit must be back
	// in the pool. Read from the real limiter gauge, not a test-only accessor.
	require.Eventually(t, func() bool {
		return readAdmissionGauge(reg, "query_admission_used_budget") == 0
	}, 5*time.Second, 10*time.Millisecond,
		"used budget did not drain to zero after all ref-filter queries returned (grant leak)")
}

// TestQueryAdmissionRefFilterSingleGrant proves the re-entrancy contract
// directly: a ref-filter query holds exactly ONE top-level admission grant
// because the nested cross-reference search inherits the parent grant instead
// of acquiring a second one.
//
// The node budget is sized so that even the buggy two-grants-per-query build
// never wedges (there is always capacity for a second grant), which lets the
// test observe the peak in-flight grant count. With the fix, in-flight grants
// never exceed the number of concurrent queries; without it each query briefly
// holds two grants (parent + fresh child) and the peak climbs above numQueries.
func TestQueryAdmissionRefFilterSingleGrant(t *testing.T) {
	const (
		maxQueue   = 512
		numAuthors = 3000 // wide nested search so the double-grant window is easy to sample
		numQueries = 16
	)
	// Enough budget for 2*numQueries acquirers to each take a full grant, so the
	// buggy build cannot park (and thus cannot wedge) — it just doubles grants.
	want := concurrency.TimesGOMAXPROCS(2)
	budget := 3 * numQueries * want

	ctx := context.Background()
	repo, shard, reg := setupRefAdmissionRepo(t, budget, maxQueue, numAuthors)
	defer repo.Shutdown(ctx)

	refFilter := articleRefNameLike("*a*")

	var peak atomic.Int64
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(200 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if v := int64(readAdmissionGauge(reg, "query_admission_inflight")); v > peak.Load() {
					peak.Store(v)
				}
			}
		}
	}()

	var (
		wg     sync.WaitGroup
		badErr atomic.Value
	)
	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			_, _, err := shard.ObjectSearch(cctx, 100, refFilter, nil, nil, nil,
				additional.Properties{}, []string{"title"})
			switch {
			case err == nil:
			case errors.Is(err, queryadmission.ErrOverloaded):
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			default:
				badErr.Store(err.Error())
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		close(stop)
		t.Fatalf("ref-filter queries did not complete within 30s under a generous budget")
	}
	close(stop)

	if v := badErr.Load(); v != nil {
		t.Fatalf("unexpected ref-filter search error (want nil / overloaded / deadline): %v", v)
	}

	// (c) One grant per query: peak in-flight grants must not exceed the number
	// of concurrent queries. A value above numQueries means a query held both a
	// parent and a fresh child grant, i.e. the nested search did not inherit.
	require.LessOrEqual(t, peak.Load(), int64(numQueries),
		"a ref-filter query consumed more than one admission grant: peak in-flight grants %d > %d concurrent queries",
		peak.Load(), numQueries)

	require.Eventually(t, func() bool {
		return readAdmissionGauge(reg, "query_admission_used_budget") == 0
	}, 5*time.Second, 10*time.Millisecond,
		"used budget did not drain to zero after all ref-filter queries returned")
}

// readAdmissionGauge reads a single-series gauge from reg by metric name. It is
// goroutine-safe (prometheus registries are concurrent-safe) and never touches
// *testing.T, so it is safe to call from sampler goroutines. Returns 0 when the
// metric is absent or gathering fails.
func readAdmissionGauge(reg *prometheus.Registry, name string) float64 {
	mfs, err := reg.Gather()
	if err != nil {
		return 0
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

// articleRefNameLike builds the ref-filter shape path:[writtenBy,Author,name]
// Like <pattern> on the Article class.
func articleRefNameLike(pattern string) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorLike,
			On: &filters.Path{
				Class:    schema.ClassName("Article"),
				Property: schema.PropertyName("writtenBy"),
				Child: &filters.Path{
					Class:    schema.ClassName("Author"),
					Property: schema.PropertyName("name"),
				},
			},
			Value: &filters.Value{Value: pattern, Type: schema.DataTypeText},
		},
	}
}

// setupRefAdmissionRepo builds a single-node repo with an Article->Author
// cross-reference schema and a query-admission limiter whose metrics register
// to the returned isolated registry, so tests can read the real gauges without
// any test-only accessor on the limiter.
func setupRefAdmissionRepo(t *testing.T, budget, maxQueue, numAuthors int) (*DB, ShardLike, *prometheus.Registry) {
	t.Helper()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel) // keep the burst quiet
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(cn string, retry bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: cn}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	// Copy the global metrics struct (its metric vectors stay valid) but point
	// the Registerer at a fresh registry: only the admission and load limiters
	// register through it, so the admission gauges become readable in isolation.
	reg := prometheus.NewRegistry()
	pm := *monitoring.GetMetrics()
	pm.Registerer = reg
	promMetrics := &pm

	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		QueryAdmissionBudget:      budget,
		QueryAdmissionMaxQueue:    maxQueue,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, promMetrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))

	vTrue := true
	author := &models.Class{
		Class:               "Author",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWhitespace,
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
			},
		},
	}
	article := &models.Class{
		Class:               "Article",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Properties: []*models.Property{
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWhitespace,
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
			},
			{
				Name:     "writtenBy",
				DataType: []string{"Author"},
			},
		},
	}
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{author, article}}}
	migrator := NewMigrator(repo, logger, "node1")
	require.NoError(t, migrator.AddClass(context.Background(), author))
	require.NoError(t, migrator.AddClass(context.Background(), article))

	importRefAdmissionObjects(t, repo, numAuthors)

	idx := repo.GetIndex(schema.ClassName("Article"))
	require.NotNil(t, idx)
	var shard ShardLike
	require.NoError(t, idx.ForEachShard(func(_ string, s ShardLike) error {
		shard = s
		return nil
	}))
	require.NotNil(t, shard)
	return repo, shard, reg
}

// importRefAdmissionObjects writes numAuthors Author objects (names all contain
// "a", so the Like "*a*" ref filter matches every one) plus one Article per
// author referencing it via writtenBy.
func importRefAdmissionObjects(t *testing.T, repo *DB, numAuthors int) {
	t.Helper()
	const chunk = 1000

	authorIDs := make([]strfmt.UUID, numAuthors)
	batch := make(objects.BatchObjects, 0, chunk)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		_, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
		require.NoError(t, err)
		batch = batch[:0]
	}

	for i := 0; i < numAuthors; i++ {
		id := strfmt.UUID(fmt.Sprintf("a%07x-0000-0000-0000-%012d", i, i))
		authorIDs[i] = id
		batch = append(batch, objects.BatchObject{
			OriginalIndex: len(batch),
			UUID:          id,
			Object: &models.Object{
				Class:      "Author",
				ID:         id,
				Properties: map[string]interface{}{"name": fmt.Sprintf("author alpha %d", i)},
				Vector:     []float32{0.1, 0.2, 0.3},
			},
		})
		if len(batch) == chunk {
			flush()
		}
	}
	flush()

	for i := 0; i < numAuthors; i++ {
		id := strfmt.UUID(fmt.Sprintf("c%07x-0000-0000-0000-%012d", i, i))
		batch = append(batch, objects.BatchObject{
			OriginalIndex: len(batch),
			UUID:          id,
			Object: &models.Object{
				Class: "Article",
				ID:    id,
				Properties: map[string]interface{}{
					"title": fmt.Sprintf("article %d", i),
					"writtenBy": models.MultipleRef{
						&models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/Author/%s", authorIDs[i]))},
					},
				},
				Vector: []float32{0.1, 0.2, 0.3},
			},
		})
		if len(batch) == chunk {
			flush()
		}
	}
	flush()
}
