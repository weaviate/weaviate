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

package traverser

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

var (
	errSparseLegFailed = errors.New("sparse leg failed fast")
	errDenseLegFailed  = errors.New("dense leg failed fast")
)

// leakFakeSearcher drives hybrid legs into the leak repro shape: SparseObjectSearch
// fails quickly while VectorSearch blocks until ctx cancels.
type leakFakeSearcher struct {
	sparseErr   error
	sparseDelay time.Duration

	denseErr   error
	denseDelay time.Duration

	sparseErrIgnoreCtx bool
	releaseSparse      chan struct{}

	hardStop            chan struct{}
	vectorSearchEntered chan struct{}
	sparseSearchEntered chan struct{}
}

func newLeakFakeSearcher() *leakFakeSearcher {
	return &leakFakeSearcher{
		hardStop:            make(chan struct{}),
		vectorSearchEntered: make(chan struct{}),
		sparseSearchEntered: make(chan struct{}),
		releaseSparse:       make(chan struct{}),
	}
}

func (f *leakFakeSearcher) VectorSearch(ctx context.Context, params dto.GetParams,
	targetVectors []string, searchVectors []models.Vector,
) ([]search.Result, error) {
	f.signalEntered()
	if f.denseErr != nil {
		if f.denseDelay > 0 {
			select {
			case <-time.After(f.denseDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-f.hardStop:
				return nil, errors.New("hard stopped by test cleanup")
			}
		}
		return nil, f.denseErr
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.hardStop:
			return nil, errors.New("hard stopped by test cleanup")
		case <-ticker.C:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
	}
}

func (f *leakFakeSearcher) SparseObjectSearch(ctx context.Context,
	params dto.GetParams,
) ([]*storobj.Object, []float32, error) {
	f.signalSparseEntered()
	if f.sparseErrIgnoreCtx {
		select {
		case <-f.releaseSparse:
		case <-f.hardStop:
			return nil, nil, errors.New("hard stopped by test cleanup")
		}
		return nil, nil, f.sparseErr
	}
	if f.sparseDelay > 0 {
		select {
		case <-time.After(f.sparseDelay):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-f.hardStop:
			return nil, nil, errors.New("hard stopped by test cleanup")
		}
	}
	if f.sparseErr != nil {
		return nil, nil, f.sparseErr
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-f.hardStop:
			return nil, nil, errors.New("hard stopped by test cleanup")
		case <-ticker.C:
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
		}
	}
}

func (f *leakFakeSearcher) signalEntered() {
	select {
	case <-f.vectorSearchEntered:
	default:
		close(f.vectorSearchEntered)
	}
}

func (f *leakFakeSearcher) signalSparseEntered() {
	select {
	case <-f.sparseSearchEntered:
	default:
		close(f.sparseSearchEntered)
	}
}

func (f *leakFakeSearcher) Search(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
	return nil, nil
}

func (f *leakFakeSearcher) CrossClassVectorSearch(ctx context.Context, vector models.Vector,
	targetVector string, offset, limit int, filters *filters.LocalFilter,
) ([]search.Result, error) {
	return nil, nil
}

func (f *leakFakeSearcher) Object(ctx context.Context, className string, id strfmt.UUID,
	props search.SelectProperties, addl additional.Properties,
	repl *additional.ReplicationProperties, tenant string,
) (*search.Result, error) {
	return nil, nil
}

func (f *leakFakeSearcher) ObjectsByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, addl additional.Properties, tenant string,
) (search.Results, error) {
	return nil, nil
}

func (f *leakFakeSearcher) Aggregate(ctx context.Context, params aggregation.Params,
	modules *modules.Provider,
) (*aggregation.Result, error) {
	return nil, nil
}

func (f *leakFakeSearcher) ResolveReferences(ctx context.Context, objs search.Results,
	props search.SelectProperties, groupBy *searchparams.GroupBy,
	addl additional.Properties, tenant string,
) (search.Results, error) {
	return objs, nil
}

var leakTestConfig = config.Config{
	QueryDefaults:             config.QueryDefaults{Limit: 100},
	QueryMaximumResults:       100,
	QueryHybridMaximumResults: 100,
}

func newLeakExplorer(t *testing.T, searcher objectsSearcher) *Explorer {
	t.Helper()
	log, _ := test.NewNullLogger()
	metrics := &fakeMetrics{}
	e := NewExplorer(searcher, log, getFakeModulesProvider(), metrics, leakTestConfig)
	e.SetSchemaGetter(&fakeSchemaGetter{
		schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
			{Class: "BestClass"},
		}}},
	})
	return e
}

func hybridLeakParams() dto.GetParams {
	return hybridLeakParamsAlpha(0.5)
}

func hybridLeakParamsAlpha(alpha float64) dto.GetParams {
	return dto.GetParams{
		ClassName: "BestClass",
		Pagination: &filters.Pagination{
			Limit:   10,
			Offset:  0,
			Autocut: -1,
		},
		HybridSearch: &searchparams.HybridSearch{
			Query: "anything",
			Alpha: alpha,
			NearVectorParams: &searchparams.NearVector{
				Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}},
			},
		},
	}
}

// TestHybridGoroutineLeak_SiblingLegFailure pins that a failing leg cancels the sibling instead of leaking.
func TestHybridGoroutineLeak_SiblingLegFailure(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	searcher := newLeakFakeSearcher()
	searcher.sparseErr = errSparseLegFailed
	searcher.sparseDelay = 0

	t.Cleanup(func() { close(searcher.hardStop) })

	e := newLeakExplorer(t, searcher)
	params := hybridLeakParams()

	done := make(chan error, 1)
	go func() {
		_, err := e.Hybrid(context.Background(), params)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err, "Hybrid must surface the sparse leg failure")
	case <-time.After(5 * time.Second):
		t.Fatal("Hybrid did not return within 5s after the sparse leg failed")
	}

	time.Sleep(50 * time.Millisecond)
}

// TestHybridContextCancellation pins that client cancel returns context.Canceled promptly without leaking.
func TestHybridContextCancellation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	searcher := newLeakFakeSearcher()
	t.Cleanup(func() { close(searcher.hardStop) })

	e := newLeakExplorer(t, searcher)
	params := hybridLeakParams()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := e.Hybrid(ctx, params)
		done <- err
	}()

	<-searcher.vectorSearchEntered
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled,
			"Hybrid must return context.Canceled when the client ctx is cancelled")
	case <-time.After(1 * time.Second):
		t.Fatal("Hybrid did not return within 1s of client cancellation")
	}

	time.Sleep(50 * time.Millisecond)
}

var errLegMustNotRun = errors.New("leg must not run when ctx is already cancelled")

type failOnLegSearcher struct {
	objectsSearcher
	t *testing.T
}

func (f *failOnLegSearcher) VectorSearch(ctx context.Context, params dto.GetParams,
	targetVectors []string, searchVectors []models.Vector,
) ([]search.Result, error) {
	f.t.Error("dense leg (VectorSearch) invoked despite already-cancelled ctx")
	return nil, errLegMustNotRun
}

func (f *failOnLegSearcher) SparseObjectSearch(ctx context.Context,
	params dto.GetParams,
) ([]*storobj.Object, []float32, error) {
	f.t.Error("sparse leg (SparseObjectSearch) invoked despite already-cancelled ctx")
	return nil, nil, errLegMustNotRun
}

// TestHybridEarlyCancelGuard pins that an already-cancelled ctx returns immediately without launching legs.
func TestHybridEarlyCancelGuard(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	e := newLeakExplorer(t, &failOnLegSearcher{t: t})
	params := hybridLeakParams()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := e.Hybrid(ctx, params)
	require.ErrorIs(t, err, context.Canceled,
		"Hybrid must return the ctx error immediately when called with a cancelled ctx")
}

// TestHybridCancelNormalizationContract pins that client-cancel + leg-error normalizes to context.Canceled.
func TestHybridCancelNormalizationContract(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	searcher := newLeakFakeSearcher()
	searcher.sparseErrIgnoreCtx = true
	searcher.sparseErr = errSparseLegFailed
	t.Cleanup(func() { close(searcher.hardStop) })

	e := newLeakExplorer(t, searcher)
	params := hybridLeakParams()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := e.Hybrid(ctx, params)
		done <- err
	}()

	<-searcher.vectorSearchEntered
	<-searcher.sparseSearchEntered
	cancel()
	close(searcher.releaseSparse)

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled,
			"on client-cancel coinciding with a real leg error, Hybrid must normalize to context.Canceled")
		require.NotErrorIs(t, err, errSparseLegFailed,
			"the genuine leg error must be masked by the cancel-normalization contract")
	case <-time.After(1 * time.Second):
		t.Fatal("Hybrid did not return within 1s")
	}

	time.Sleep(50 * time.Millisecond)
}

// TestHybridSingleLegAndBothErrors pins single-leg cancel (alpha=0, alpha=1) and both-legs-error without leaks.
func TestHybridSingleLegAndBothErrors(t *testing.T) {
	t.Run("alpha=0 sparse-only under cancel", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		searcher := newLeakFakeSearcher()
		t.Cleanup(func() { close(searcher.hardStop) })

		e := newLeakExplorer(t, searcher)
		params := hybridLeakParamsAlpha(0)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() {
			_, err := e.Hybrid(ctx, params)
			done <- err
		}()

		<-searcher.sparseSearchEntered
		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("alpha=0: Hybrid did not return within 1s of cancel")
		}
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("alpha=1 dense-only under cancel", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		searcher := newLeakFakeSearcher()
		t.Cleanup(func() { close(searcher.hardStop) })

		e := newLeakExplorer(t, searcher)
		params := hybridLeakParamsAlpha(1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() {
			_, err := e.Hybrid(ctx, params)
			done <- err
		}()

		<-searcher.vectorSearchEntered
		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("alpha=1: Hybrid did not return within 1s of cancel")
		}
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("both legs error", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		searcher := newLeakFakeSearcher()
		searcher.sparseErr = errSparseLegFailed
		searcher.denseErr = errDenseLegFailed
		t.Cleanup(func() { close(searcher.hardStop) })

		e := newLeakExplorer(t, searcher)
		params := hybridLeakParams() // alpha=0.5 -> both legs run

		_, err := e.Hybrid(context.Background(), params)
		require.Error(t, err, "both legs failing must surface a non-nil error")
		require.NotErrorIs(t, err, context.Canceled,
			"with no client cancel, the returned error must be a genuine leg error")
		time.Sleep(50 * time.Millisecond)
	})
}
