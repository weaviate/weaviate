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

package lsmkv

// Gates for the Addendum-2 chunked-pipeline resolver (gh#309): the per-key concurrent
// chunked driver over the single-key primitive getBySecondaryCore. The close-blocking
// correctness gate is a differential-equality oracle run with the pipeline toggled ON,
// under -race, over the randomized multi-segment corpus (buildRandomizedSecondaryBucket):
// the concurrent chunked resolution must be byte-identical, positionally, to the per-key
// loop that is the shipped, assumed-correct resolver. The worker-concurrency gate proves
// the driver actually runs W-wide (>= 8 workers at W=16) and reddens when forced serial.
// The ctx-cancel and slow-log-annotate-once gates pin the two shared-view hazards the
// audit named: cancellation mid-batch, and per-key slow-log contention.

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// withPipelineOn returns the bucket options that enable the chunked pipeline at worker
// count w for a batch test bucket.
func withPipelineOn(w int) []BucketOption {
	return []BucketOption{
		WithSecondaryBatchPipeline(configRuntime.NewDynamicValue(true)),
		WithSecondaryBatchReadConcurrency(configRuntime.NewDynamicValue(w)),
	}
}

// TestBucketGetBySecondaryBatchPipelineDifferentialRace is the AC1/AC2 close-blocking gate:
// concurrent chunked resolution (W=16) vs the per-key loop over the same frozen randomized
// corpus and seeds, asserting exact positional equality across the full value-mode x bloom
// x seed matrix. Run under -race, the concurrent W-wide chunked driver over one shared view
// is the exact configuration the getBySecondaryCore shared-mutable-state audit must clear:
// a data race on any bucket field, metric, or scratch buffer the single-key primitive
// touches would trip the race detector here even when the returned bytes happen to match.
//
// This catches a batch-only divergence (a chunk-boundary bug, a lost origIdx, a shared
// buffer clobber, a newest-wins slip) because the per-key loop resolves each key
// independently through the shipped path; any place the chunked driver perturbs per-key
// resolution shows up as a positional inequality that would be absent if the driver were
// a faithful chunking of the same primitive.
func TestBucketGetBySecondaryBatchPipelineDifferentialRace(t *testing.T) {
	for _, mode := range secondaryValueModes {
		for _, useBloom := range []bool{false, true} {
			mode, useBloom := mode, useBloom
			bloomName := "bloom-off"
			if useBloom {
				bloomName = "bloom-on"
			}
			t.Run(mode.name+"/"+bloomName, func(t *testing.T) {
				for _, seed := range []int64{1, 7, 42, 1337, 90210} {
					seed := seed
					t.Run("seed", func(t *testing.T) {
						b, universe := buildRandomizedSecondaryBucket(t, seed, useBloom, mode.pread,
							withPipelineOn(16)...)
						rng := rand.New(rand.NewSource(seed * 31))

						keys := make([][]byte, 0, len(universe)+40)
						for _, d := range universe {
							keys = append(keys, encodeDocID(d))
						}
						for i := 0; i < 20; i++ { // absent keys -> interior nils (WithEmpty semantics)
							keys = append(keys, encodeDocID(1_000_000+uint64(i)))
						}
						for i := 0; i < 20; i++ { // duplicates of real keys
							keys = append(keys, encodeDocID(universe[rng.Intn(len(universe))]))
						}
						rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

						// pipeline is ON via the bucket option, so GetBySecondaryBatch routes
						// through the concurrent chunked driver; resolveViaLoop is the serial
						// per-key reference.
						require.True(t, b.secondaryBatchPipelineEnabled(), "pipeline must be on")
						want := resolveViaLoop(t, b, secondaryPos, keys)
						got, err := b.GetBySecondaryBatch(context.Background(), secondaryPos, keys)
						require.NoError(t, err)
						require.Len(t, got, len(keys))
						for i := range keys {
							require.Equalf(t, want[i], got[i],
								"pipeline vs per-key-loop divergence at position %d (key %x)", i, keys[i])
						}
					})
				}
			})
		}
	}
}

// TestBucketGetBySecondaryBatchPipelineWorkerConcurrency is the worker-concurrency gate:
// on the oldest-segment-hit shape (every key descends every newer segment before
// confirming), the driver must run >= 8 workers concurrently at W=16, and the forced W=1
// variant must NOT (peak == 1). Repurposes the phase-2 concurrencyProbe: the driver hook
// fires onReadStart/onReadDone around each per-key getBySecondaryCore, so peak in-flight ==
// peak concurrent workers. A lost fan-out, a W=1 misconfig, or a refactor that re-serializes
// the driver would drop peak below 8 and fail (i); pinning the forced-serial variant to
// peak < 8 proves the gate has teeth rather than passing vacuously.
func TestBucketGetBySecondaryBatchPipelineWorkerConcurrency(t *testing.T) {
	ctx := context.Background()

	runPeak := func(w int) int {
		b, keys := buildOldestSegmentHitBucket(t, 500, 6, withPipelineOn(w)...)
		view := b.GetConsistentView()
		defer view.ReleaseView()
		out := make([][]byte, len(keys))
		probe := newConcurrencyProbe(8, 2*time.Second)
		hook := &secondaryBatchReadHook{onReadStart: probe.onReadStart, onReadDone: probe.onReadDone}
		res, err := b.getBySecondaryBatchPipelined(ctx, secondaryPos, keys, view, out, 0, hook)
		require.NoError(t, err)
		require.Len(t, res, len(keys))
		return probe.peakInflight()
	}

	peakConcurrent := runPeak(16)
	require.GreaterOrEqualf(t, peakConcurrent, 8,
		"chunked driver peaked at %d concurrent workers at W=16; < 8 means it serialized "+
			"(W=1 misconfig, lost fan-out, or a serializing refactor)", peakConcurrent)

	peakSerial := runPeak(1)
	require.Lessf(t, peakSerial, 8,
		"forced-serial (W=1) must NOT reach 8 concurrent workers; got peak %d (the gate would "+
			"be vacuous if this passed)", peakSerial)
}

// TestBucketGetBySecondaryBatchPipelineCtxCancel confirms clean cancellation: a ctx
// cancelled mid-batch surfaces context.Canceled (the driver checks egctx.Err() between
// keys and the errgroup drains) rather than hanging or leaking a worker goroutine (the
// -race run flags a leak/deadlock).
func TestBucketGetBySecondaryBatchPipelineCtxCancel(t *testing.T) {
	b, keys := buildOldestSegmentHitBucket(t, 500, 6, withPipelineOn(16)...)
	view := b.GetConsistentView()
	defer view.ReleaseView()
	out := make([][]byte, len(keys))

	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	hook := &secondaryBatchReadHook{onReadStart: func() { once.Do(cancel) }}

	_, err := b.getBySecondaryBatchPipelined(ctx, secondaryPos, keys, view, out, 0, hook)
	require.ErrorIs(t, err, context.Canceled)
}

// TestBucketGetBySecondaryBatchPipelineSlowLogAnnotateOnce pins the shared-accumulator
// hazard the audit named: the driver must annotate the slow log EXACTLY once per batch, not
// once per key across W workers. Over a 100-key multi-chunk batch the pipeline masks the
// per-key getBySecondaryCore annotations and appends a single lsm_get_by_secondary_batch
// entry after Wait; a naive per-key annotation would leave 100 entries (and contend the
// accumulator mutex W-wide on the cold path).
func TestBucketGetBySecondaryBatchPipelineSlowLogAnnotateOnce(t *testing.T) {
	b, keys := buildAllHitSecondaryBucket(t, 100, 4, withPipelineOn(16)...)
	require.True(t, b.secondaryBatchPipelineEnabled())

	ctx := helpers.InitSlowQueryDetails(context.Background())
	_, err := b.GetBySecondaryBatch(ctx, secondaryPos, keys)
	require.NoError(t, err)

	details := helpers.ExtractSlowQueryDetails(ctx)
	entries, ok := details["lsm_get_by_secondary_batch"].([]BucketSlowLogEntry)
	require.Truef(t, ok, "expected a []BucketSlowLogEntry under lsm_get_by_secondary_batch, got %T",
		details["lsm_get_by_secondary_batch"])
	require.Lenf(t, entries, 1,
		"chunked pipeline must annotate the slow log exactly once per batch, not per key; got %d entries",
		len(entries))
}
