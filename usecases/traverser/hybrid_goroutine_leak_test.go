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

// errSparseLegFailed is the deterministic error the BM25 leg returns in the
// leak repro, standing in for any quick leg failure on a large shard.
var errSparseLegFailed = errors.New("sparse leg failed fast")

// errDenseLegFailed is a deterministic dense-leg failure used by the
// both-legs-error case.
var errDenseLegFailed = errors.New("dense leg failed fast")

// leakFakeSearcher is a controllable objectsSearcher for the Hybrid
// coordination-bug repro. It deliberately drives the two hybrid legs into the
// failing shape described in the architect synthesis:
//   - SparseObjectSearch returns an error quickly (the failing BM25 leg).
//   - VectorSearch blocks, polling the ctx it was handed, and only returns once
//     that ctx is cancelled (the sibling dense leg that should be torn down).
//
// It spawns NO goroutines of its own, so any goroutine goleak reports as leaked
// is the Hybrid leg coordination goroutine, never test scaffolding.
type leakFakeSearcher struct {
	// sparseErr, when non-nil, is returned by SparseObjectSearch after sparseDelay.
	sparseErr   error
	sparseDelay time.Duration

	// denseErr, when non-nil, is returned by VectorSearch after denseDelay
	// (used by the both-legs-error case where the dense leg also fails fast).
	denseErr   error
	denseDelay time.Duration

	// sparseErrIgnoreCtx, when true, makes SparseObjectSearch wait on
	// releaseSparse (NOT on ctx) and then return sparseErr. This lets a test
	// deterministically order a genuine non-cancel leg error against a client
	// cancel: cancel the client ctx first, THEN release the sentinel leg, so
	// eg.Wait() surfaces sparseErr while ctx.Err() is already non-nil.
	sparseErrIgnoreCtx bool
	releaseSparse      chan struct{}

	// hardStop is closed by the test during cleanup to reap any goroutine that
	// the bug leaks, so a RED test does not poison sibling tests in the same
	// binary. goleak.VerifyNone runs (via defer) before t.Cleanup fires, so the
	// leak is still observed while the goroutine is alive.
	hardStop chan struct{}

	// vectorSearchEntered is closed the first time VectorSearch is entered, so a
	// test can wait until the dense leg is actually mid-flight before acting.
	vectorSearchEntered chan struct{}

	// sparseSearchEntered is closed the first time SparseObjectSearch is entered,
	// so a test can assert the sparse leg launched (or, conversely, that it never
	// launched in the early-guard case).
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

// VectorSearch is the dense leg. It blocks until either the ctx it was handed is
// cancelled (the correct teardown signal) or hardStop is closed (test cleanup
// safety net). On unmodified main, Hybrid hands this the original un-cancelled
// ctx, so a sibling-leg failure never cancels it and this goroutine leaks.
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

// SparseObjectSearch is the BM25 leg. It returns an error quickly so that, in
// the leak repro, the errgroup observes a failure while VectorSearch is still
// mid-flight.
func (f *leakFakeSearcher) SparseObjectSearch(ctx context.Context,
	params dto.GetParams,
) ([]*storobj.Object, []float32, error) {
	f.signalSparseEntered()
	if f.sparseErrIgnoreCtx {
		// Wait for the test to release us (deliberately NOT selecting on ctx), so
		// the test can cancel the client ctx first and then make this leg return a
		// genuine non-cancel error. This pins the cancel-normalization contract.
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
	// No error configured: behave like the blocking dense leg (used by the
	// cancellation test where BOTH legs must be in-flight when the client
	// cancels).
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
		// already closed
	default:
		close(f.vectorSearchEntered)
	}
}

func (f *leakFakeSearcher) signalSparseEntered() {
	select {
	case <-f.sparseSearchEntered:
		// already closed
	default:
		close(f.sparseSearchEntered)
	}
}

// --- remaining objectsSearcher surface: unused by the hybrid leg paths under
// test, present only to satisfy the interface. ---

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

// leakTestConfig sets QueryHybridMaximumResults so the hybrid leg sizing logic
// has a non-zero bound without needing real data.
var leakTestConfig = config.Config{
	QueryDefaults:             config.QueryDefaults{Limit: 100},
	QueryMaximumResults:       100,
	QueryHybridMaximumResults: 100,
}

// newLeakExplorer wires an Explorer around the controllable fake searcher with a
// single-class schema that has a legacy (default) vector index, so the dense leg
// resolves to the default target vector "" and routes through VectorSearch with
// no module/vectorizer dependency.
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

// hybridLeakParams builds GetParams for a hybrid query with 0 < alpha < 1 so both
// legs launch. NearVectorParams carries a pre-populated vector so the dense leg
// needs no vectorizer; targetVectors resolves to the single default "" target.
func hybridLeakParams() dto.GetParams {
	return hybridLeakParamsAlpha(0.5)
}

// hybridLeakParamsAlpha is hybridLeakParams with a caller-chosen alpha so a test
// can exercise single-leg shapes: alpha=1 runs only the dense leg, alpha=0 only
// the sparse leg, 0<alpha<1 runs both concurrently.
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

// TestHybridGoroutineLeak_SiblingLegFailure is the RED-first repro for the
// goroutine leak in Explorer.Hybrid.
//
// Causal link: this test catches the leaked-goroutine bug because the sparse leg
// returns errSparseLegFailed almost immediately while the dense leg (VectorSearch)
// is blocked polling the ctx it was handed. On unmodified main, Hybrid runs the
// legs under enterrors.NewErrorGroupWrapper (a bare errgroup with no derived,
// cancellable context), so the first leg's error is surfaced by eg.Wait() and
// Hybrid returns, but nothing ever cancels the ctx the dense leg holds. The dense
// leg therefore never observes a cancel and its goroutine outlives Hybrid's
// return -> goleak.VerifyNone reports it as leaked -> RED. After the fix (Child 2:
// errgroup-derived cancellable ctx threaded into both legs), the sparse leg's
// error cancels egCtx, the dense leg's ctx.Err() poll trips, it returns, and
// goleak is clean -> GREEN.
//
// A plain return-value assertion would go red->green on the fix without proving
// the leak is gone; goleak is what observes the goroutine that would be absent
// only once the cancel actually propagates.
func TestHybridGoroutineLeak_SiblingLegFailure(t *testing.T) {
	// Baseline ignores goroutines already running when the test starts (logger
	// flushers, test framework, etc.), so VerifyNone flags only NEW leaks.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	searcher := newLeakFakeSearcher()
	searcher.sparseErr = errSparseLegFailed
	searcher.sparseDelay = 0 // fail immediately

	// Safety net: reap any leaked dense-leg goroutine AFTER goleak has observed
	// it. t.Cleanup runs after deferred VerifyNone, so RED is still seen first.
	t.Cleanup(func() { close(searcher.hardStop) })

	e := newLeakExplorer(t, searcher)
	params := hybridLeakParams()

	// Bound the call so a hung dense leg cannot wedge the test indefinitely; we
	// assert the error came back, then goleak (deferred) inspects goroutines.
	// This driver goroutine exits as soon as Hybrid returns (fast, once the
	// sparse leg errors), so goleak's retry window does not flag it.
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

	// Give the (buggy) dense-leg goroutine a moment to still be parked, so
	// goleak observes the leak deterministically rather than racing the runtime.
	time.Sleep(50 * time.Millisecond)
}

// TestHybridContextCancellation is the cancellation contract: when the client
// cancels mid-flight, Hybrid returns context.Canceled promptly (<= 1s) and no
// goroutine is leaked.
//
// Causal link: both legs block polling their ctx until cancelled. Cancelling the
// client ctx ~10ms in must (a) return context.Canceled within the deadline and
// (b) leave goleak clean. On main, the legs are handed the client's own
// cancellable ctx, so their internal polls trip on client-cancel and this case
// is the narrower one; the after-fix contract is that BOTH legs stop together and
// nothing lingers.
func TestHybridContextCancellation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	searcher := newLeakFakeSearcher()
	// No sparseErr: both legs block until ctx cancel, so both are genuinely
	// in-flight when the client cancels.
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

	// Cancel ~10ms in, once both legs are mid-flight.
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

// errLegMustNotRun is returned by failOnLegSearcher's legs so that, if a
// regression ever launches a leg despite the early guard, Hybrid stops cleanly at
// eg.Wait() (rather than proceeding into fusion with empty results); the t.Error
// is what actually fails the test.
var errLegMustNotRun = errors.New("leg must not run when ctx is already cancelled")

// failOnLegSearcher fails the test if either hybrid leg is ever invoked. It is
// used by the early-ctx.Err()-guard test to assert that an already-cancelled
// request returns before any leg launches. The embedded objectsSearcher is nil;
// only the two leg methods are reachable on the guarded path, and both are
// overridden here.
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

// TestHybridEarlyCancelGuard pins the early ctx.Err() guard: a request whose ctx
// is already cancelled before Hybrid is called must return the ctx error
// immediately and launch NEITHER leg.
//
// Causal link: the fake fails the test if either leg searcher is invoked. The
// guard added by this fix returns ctx.Err() before eg.Go, so neither method is
// reached. A future edit moving the guard below leg-launch would invoke a leg and
// trip f.t.Error here, turning this test RED.
func TestHybridEarlyCancelGuard(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	e := newLeakExplorer(t, &failOnLegSearcher{t: t})
	params := hybridLeakParams()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the call

	_, err := e.Hybrid(ctx, params)
	require.ErrorIs(t, err, context.Canceled,
		"Hybrid must return the ctx error immediately when called with a cancelled ctx")
}

// TestHybridCancelNormalizationContract pins the deliberate cancel-normalization
// trade-off (the Level C edge): when the client ctx is cancelled AND a leg
// returns a genuine NON-cancel error, Hybrid returns context.Canceled, NOT the
// leg's real error. This masking is intentional so upstream gRPC/REST handlers
// can detect cancellation via errors.Is(err, context.Canceled). Pinning it means
// a future refactor that "fixes" the masking to surface the real error will turn
// this test RED instead of silently changing the contract.
//
// Determinism: the sparse leg ignores ctx and blocks on releaseSparse, so the
// test orders the events exactly: wait until both legs are mid-flight, cancel the
// client ctx, THEN release the sparse leg to return errSparseLegFailed. eg.Wait()
// surfaces that genuine error while ctx.Err() is already non-nil, so the
// normalization branch fires.
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

	// Ensure both legs are mid-flight, then cancel the client ctx and only AFTER
	// that release the sparse leg so it returns a genuine non-cancel error.
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

// TestHybridSingleLegAndBothErrors covers the single-leg-under-cancel shapes and
// the both-legs-error shape, all goleak-clean:
//   - alpha=0: only the sparse leg runs; client cancel mid-flight returns promptly.
//   - alpha=1: only the dense leg runs; client cancel mid-flight returns promptly.
//   - both-error: both legs fail fast with genuine (non-cancel) errors; Hybrid
//     returns a non-nil error, both legs return, nothing leaks.
//
// Causal link: the fix threads egCtx through the single-leg paths too. A
// regression where only the 2-leg path received egCtx would pass the existing
// 2-leg cancel test but leak (or hang) on alpha=0/alpha=1; these subtests would
// then time out / flag goleak.
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
		// errgroup surfaces the first error; either genuine leg error is
		// acceptable. The contract is: a non-nil, non-cancel error comes back
		// and (via goleak) both legs returned without leaking.
		require.Error(t, err, "both legs failing must surface a non-nil error")
		require.NotErrorIs(t, err, context.Canceled,
			"with no client cancel, the returned error must be a genuine leg error")
		time.Sleep(50 * time.Millisecond)
	})
}
