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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

// TestQueryAdmissionRefFilterNoWedge is the regression guard for the admission
// wedge caused by dropping the admitted ctx before a nested cross-reference
// search (inverted.Searcher.extractReferenceFilter used context.TODO()).
//
// With the ctx dropped, the nested Author search re-enters Admit as a FRESH
// acquirer (the re-entrancy marker is lost), so a single ref-filter query needs
// two grants. Under a small node budget the parents hold every unit while their
// nested children park on a never-expiring context.TODO(); the parents can
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

	if !burstRefFilterSearches(t, qctx, shard, refFilter, numQueries) {
		qcancel()
		t.Fatalf("admission wedge: %d ref-filter queries did not complete within 30s; "+
			"the nested cross-reference search re-entered Admit as a fresh acquirer and "+
			"parked on a never-expiring ctx while the parent held its grant", numQueries)
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
	// buggy build cannot park (and thus cannot wedge); it just doubles grants.
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

	if !burstRefFilterSearches(t, ctx, shard, refFilter, numQueries) {
		close(stop)
		t.Fatalf("ref-filter queries did not complete within 30s under a generous budget")
	}
	close(stop)

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

// burstRefFilterSearches fires numQueries concurrent ref-filter searches and
// reports whether all complete within 30s; the per-query 60s deadline alone
// can't interrupt the buggy build's nested-Admit wedge, so callers handle a
// false return with their own wedge diagnostics.
func burstRefFilterSearches(t *testing.T, qctx context.Context, shard ShardLike,
	refFilter *filters.LocalFilter, numQueries int,
) bool {
	t.Helper()
	var (
		wg     sync.WaitGroup
		badErr atomic.Value
	)
	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		return false
	}

	if v := badErr.Load(); v != nil {
		t.Fatalf("unexpected ref-filter search error (want nil / overloaded / deadline): %v", v)
	}
	return true
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

	// Copy the global metrics struct (its metric vectors stay valid) but point
	// the Registerer at a fresh registry: only the admission and load limiters
	// register through it, so the admission gauges become readable in isolation.
	reg := prometheus.NewRegistry()
	pm := *monitoring.GetMetrics()
	pm.Registerer = reg

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

	repo, shard := newAdmissionRepo(t, admissionRepoParams{
		budget:      budget,
		maxQueue:    maxQueue,
		promMetrics: &pm,
	}, "Article", author, article)

	importRefAdmissionObjects(t, repo, numAuthors)

	return repo, shard, reg
}

// importRefAdmissionObjects writes numAuthors Author objects (names all contain
// "a", so the Like "*a*" ref filter matches every one) plus one Article per
// author referencing it via writtenBy.
//
// Vectors must be distinct and non-collinear (see admissionObjectVector):
// identical vectors collapse the HNSW graph into a near-complete graph,
// making construction super-linear and slow enough to hang CI. The watchdog
// below turns any regression into a fast, labelled failure instead of a hang.
func importRefAdmissionObjects(t *testing.T, repo *DB, numAuthors int) {
	t.Helper()

	// Order-of-magnitude above a healthy import but well under the suite
	// timeout, so a regression fails fast here instead of stalling the suite.
	const importDeadline = 5 * time.Minute

	done := make(chan error, 1)
	go func() { done <- writeRefAdmissionObjects(repo, numAuthors) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(importDeadline):
		t.Fatalf("ref-admission import of %d authors + %d articles did not finish within %s; "+
			"the imported vectors are most likely degenerate (identical or collinear), which "+
			"makes HNSW graph construction pathologically slow", numAuthors, numAuthors, importDeadline)
	}
}

// writeRefAdmissionObjects does the batched import and returns the first
// error; it runs on its own goroutine, so it must not touch *testing.T.
func writeRefAdmissionObjects(repo *DB, numAuthors int) error {
	const chunk = 1000

	authorIDs := make([]strfmt.UUID, numAuthors)
	batch := make(objects.BatchObjects, 0, chunk)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, err := repo.BatchPutObjects(context.Background(), batch, nil, 0); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
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
				Vector:     admissionObjectVector(i),
			},
		})
		if len(batch) == chunk {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

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
				Vector: admissionObjectVector(i),
			},
		})
		if len(batch) == chunk {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// admissionObjectVector returns a distinct, non-collinear vector for object i
// (see importRefAdmissionObjects for why that matters).
func admissionObjectVector(i int) []float32 {
	return []float32{
		float32(i) + 1,
		float32((i*3)%251) + 1,
		float32((i*7)%241) + 1,
	}
}

// TestObjectVectorSearchReleasesGrantBeforeVectorPhase pins that the
// allow-list grant is released before the vector-search phase runs, so a
// slow vector search never holds node budget.
func TestObjectVectorSearchReleasesGrantBeforeVectorPhase(t *testing.T) {
	const (
		budget     = 4
		maxQueue   = 8
		numAuthors = 10000 // wide allow-list + enough vectors to make the vector phase samplable
	)

	ctx := context.Background()
	repo, shard, reg := setupRefAdmissionRepo(t, budget, maxQueue, numAuthors)
	defer repo.Shutdown(ctx)

	// Matches every Article, so the allow-list grant is observable and the
	// vector phase after it dominates the query.
	filter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorLike,
			On: &filters.Path{
				Class:    schema.ClassName("Article"),
				Property: schema.PropertyName("title"),
			},
			Value: &filters.Value{Value: "*article*", Type: schema.DataTypeText},
		},
	}
	searchVec := []models.Vector{[]float32{0.1, 0.2, 0.3}}

	var sawPositive, sawZeroWhileRunning atomic.Bool
	searchDone := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if readAdmissionGauge(reg, "query_admission_used_budget") > 0 {
					sawPositive.Store(true)
					continue
				}
				// used==0 only counts once we've seen the grant, before the search returns.
				if sawPositive.Load() {
					select {
					case <-searchDone:
					default:
						sawZeroWhileRunning.Store(true)
					}
				}
			}
		}
	}()

	// limit<0 forces a brute-force distance search over every matched Article
	// vector, so the vector phase runs long enough to be sampled.
	_, _, err := shard.ObjectVectorSearch(ctx, searchVec, []string{""}, 100, -1,
		filter, nil, nil, additional.Properties{}, nil, []string{"title"})
	close(searchDone)
	close(stop)
	require.NoError(t, err)

	require.True(t, sawPositive.Load(),
		"expected the allow-list phase to hold an admission grant (used_budget > 0)")
	require.True(t, sawZeroWhileRunning.Load(),
		"used_budget must return to zero while ObjectVectorSearch is still running: "+
			"the allow-list grant must be released before the vector phase")
	require.Eventually(t, func() bool {
		return readAdmissionGauge(reg, "query_admission_used_budget") == 0
	}, 2*time.Second, 5*time.Millisecond, "used budget did not drain to zero")
}
