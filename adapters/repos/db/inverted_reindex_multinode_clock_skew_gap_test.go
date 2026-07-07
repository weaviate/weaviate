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
	"encoding/binary"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// gapFixed toggles the characterization assertions between "gap still open"
// (false, the state of current main) and "gap closed" (true, what a
// skew-immune fix must deliver).
//
// This is an INVERTED characterization pin, not a red repro. With gapFixed
// == false every case below asserts the outcome current main actually
// produces, so the whole test is GREEN on main and stays mergeable — an RFC
// needs green CI. Gap cases (the ones whose behavior a fix must change) log a
// loud KNOWN-GAP line when they pass so the reproduction is never silent.
//
// The moment a fix makes a lost write survive (add side) or prunes a
// resurrected posting (delete side), the corresponding assertion flips and
// this test goes RED against gapFixed == false — forcing whoever lands the
// fix to flip this const to true (which then asserts the skew-immune
// outcome). Flip it ONLY once every gap case delivers the skew-immune result;
// a partial fix must keep the pin red.
const gapFixed = false

// readRangeableDocIDs returns the docIDs indexed under a single int64 value in
// a RoaringSetRange (rangeable) bucket. Same read path as
// [filterableToRangeableFingerprint], narrowed to one value so a test can
// assert presence/absence of one specific write.
func readRangeableDocIDs(t *testing.T, b *lsmkv.Bucket, value int64) []uint64 {
	t.Helper()
	require.NotNil(t, b, "rangeable bucket must exist")
	lex, err := entinverted.LexicographicallySortableInt64(value)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	bm, release, err := reader.Read(context.Background(), key, filters.OperatorEqual)
	require.NoError(t, err)
	var ids []uint64
	if bm != nil {
		ids = bm.ToArray()
	}
	if release != nil {
		release()
	}
	return ids
}

// skewGapHarness is the shared scaffolding every case in
// [TestReindex_MultiNodeClockSkew_ReopensDoubleWriteGap] drives: one shard,
// one FilterableToRangeable migration task, and the helpers a case needs to
// stage a write or a delete relative to the double-write callback
// registration boundary (OnAfterLsmInit).
type skewGapHarness struct {
	ctx       context.Context
	shard     *Shard
	task      *ShardReindexTaskGeneric
	className string
	propName  string
}

// putWithTimestamp writes an object carrying an explicit coordinator
// timestamp. A replica preserves whatever LastUpdateTimeUnix the coordinator
// stamped, so a coordinator whose clock is ahead is modeled exactly by an
// object with a future LastUpdateTimeUnix.
func (h *skewGapHarness) putWithTimestamp(t *testing.T, value, tsMillis int64) strfmt.UUID {
	t.Helper()
	id := strfmt.UUID(uuid.NewString())
	require.NoError(t, h.shard.PutObject(h.ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              h.className,
			Properties:         map[string]interface{}{h.propName: value},
			CreationTimeUnix:   tsMillis,
			LastUpdateTimeUnix: tsMillis,
		},
	}))
	return id
}

// bypassDeleteTee drops every delete callback so a subsequent DeleteObject
// does NOT mirror a tombstone into the ingest bucket. It models a delete that
// never reaches the double-write tee (the delete-side analog of an unmirrored
// write); the add tee is left armed, so only the delete escapes the mirror.
func (h *skewGapHarness) bypassDeleteTee() {
	h.shard.callbacksRemoveFromPropertyValueIndex.Store([]onDeleteFromPropertyValueIndex{})
}

// TestReindex_MultiNodeClockSkew_ReopensDoubleWriteGap pins the multi-node
// residual of weaviate/weaviate#11692 — the gap the single-clock fix
// (weaviate/weaviate#11688, capture reindexStarted after callback
// registration, ms-ceiled) does NOT close.
//
// # The invariant and why cross-node skew breaks it
//
// The backfill iterator ([uuidObjectsIteratorAsync]) classifies every object
// it scans by one comparison:
//
//	obj.LastUpdateTimeUnix() < reindexStarted.UnixMilli()  → backfill (analyze)
//	obj.LastUpdateTimeUnix() >= reindexStarted.UnixMilli() → skip (assume mirrored)
//
// The "skip" branch is only sound if every object timestamped at/after
// reindexStarted was in fact captured by the double-write callbacks. That
// holds under a SINGLE clock: reindexStarted is captured after callback
// registration, so any write timestamped >= reindexStarted physically
// happened after the callbacks were live and was mirrored. It does NOT hold
// across nodes. LastUpdateTimeUnix is stamped on the coordinator that received
// the write (usecases/objects/*.go: LastUpdateTimeUnix = m.timeSource.Now())
// and the replica preserves that stamp verbatim; reindexStarted is stamped on
// the replica's own clock (markStarted(time.Now())). With the coordinator's
// clock ahead of the replica's, a write can arrive at the replica BEFORE
// callback registration (so it is NOT mirrored) yet carry a coordinator
// timestamp >= reindexStarted (so the backfill SKIPS it, assuming mirrored) —
// permanently missing from the migrated index after the migration reports
// FINISHED.
//
// # What this pin does and does NOT demonstrate
//
// It reproduces the add-path CLASSIFICATION bug deterministically: the skip
// decision is a pure local timestamp comparison and the replica preserves the
// coordinator stamp, so a crafted future timestamp is a faithful stand-in for
// a coordinator clock that is ahead. It does NOT (and cannot) demonstrate
// reachability of the write-arrives-before-registration interleaving under
// real replication timing — that ordering is hard-coded here (writes are
// staged before OnAfterLsmInit), and its reachability rests on the code
// reading of the markStarted-after-registration ordering
// (weaviate/weaviate#11985), not on this test.
//
// # The cases
//
// Every case runs the production migration path on its own shard and asserts
// whether one sentinel value survives the migration. Add-side cases stage
// unmirrored pre-registration writes; delete-side cases seed the ingest
// bucket through the live double-write mirror and then vary whether the delete
// reaches the tee. gapFixed selects the current (gap-open) or skew-immune
// (gap-closed) expectation; see the const doc.
func TestReindex_MultiNodeClockSkew_ReopensDoubleWriteGap(t *testing.T) {
	const (
		numCorpus    = 25         // baseline population, all timestamp 0 → all backfilled
		controlValue = int64(100) // add-side reverse-skew control (honest past timestamp)
		skewValue    = int64(999) // add-side forward-skew gap (future coordinator timestamp)
		delConverge  = int64(888) // delete-side control (delete reaches the tee)
		delResurrect = int64(777) // delete-side gap (delete bypasses the tee)
	)

	pastTs := time.Now().Add(-time.Hour).UnixMilli()       // coordinator behind → below cutoff
	futureTs := time.Now().Add(24 * time.Hour).UnixMilli() // coordinator ahead → above cutoff

	cases := []struct {
		name     string
		sentinel int64
		// arrangeUnmirrored runs BEFORE OnAfterLsmInit: the double-write
		// callbacks are not armed yet, so these writes are visible to the
		// backfill only via the objects snapshot, never mirrored.
		arrangeUnmirrored func(t *testing.T, h *skewGapHarness)
		// arrangeMirrored runs AFTER OnAfterLsmInit (callbacks armed) but
		// before the async backfill loop: delete-side cases use it to seed
		// the ingest bucket through the live mirror.
		arrangeMirrored func(t *testing.T, h *skewGapHarness)
		// wantPresentWithGap is the outcome current main produces.
		// wantPresentWhenFixed is the skew-immune outcome a fix must deliver.
		// When they differ the case is a GAP case (logs KNOWN-GAP); when they
		// match it is a live control that must hold in both worlds.
		wantPresentWithGap   bool
		wantPresentWhenFixed bool
		// gapDoc is the KNOWN-GAP explanation logged while the gap is open.
		gapDoc string
	}{
		{
			// Directional negative control: coordinator BEHIND the replica.
			// An unmirrored pre-registration write with an honest past
			// timestamp is below the cutoff, analyzed, and backfilled — it
			// survives. Documents that the gap is directional: only a
			// coordinator AHEAD of the replica loses writes.
			name:     "add_reverse_skew_below_cutoff_control_survives",
			sentinel: controlValue,
			arrangeUnmirrored: func(t *testing.T, h *skewGapHarness) {
				h.putWithTimestamp(t, controlValue, pastTs)
			},
			wantPresentWithGap:   true,
			wantPresentWhenFixed: true,
		},
		{
			// THE add-side gap: coordinator AHEAD of the replica. An
			// unmirrored pre-registration write whose future coordinator
			// timestamp lands at/above the cutoff is skipped by the backfill
			// as "already mirrored" while the mirror never saw it — lost.
			// A skew-immune backfill must index it anyway.
			name:     "add_forward_skew_above_cutoff_gap_lost",
			sentinel: skewValue,
			arrangeUnmirrored: func(t *testing.T, h *skewGapHarness) {
				h.putWithTimestamp(t, skewValue, futureTs)
			},
			wantPresentWithGap:   false, // skipped-and-unmirrored ⇒ absent (known-bad)
			wantPresentWhenFixed: true,  // skew-immune ⇒ backfilled ⇒ present
			gapDoc: "add-side: an unmirrored pre-registration write whose coordinator " +
				"timestamp is ahead of the replica's reindexStarted is skipped by the " +
				"backfill AND missed by the double-write callbacks — permanently lost " +
				"(missing-row symptom)",
		},
		{
			// Delete-side negative control: the delete reaches the tee. A
			// live write mirrors delConverge into the ingest bucket, then a
			// future-timestamped delete mirrors a tombstone into the same
			// bucket. The tombstone shadows the mirrored add (ingest wins per
			// key at merge), so the posting converges to absent — even though
			// the delete carries a future coordinator timestamp. Documents
			// that skew alone does NOT break the delete side; the tombstone
			// is position-based, not timestamp-based.
			name:     "delete_mirrored_future_skew_converges_control",
			sentinel: delConverge,
			arrangeMirrored: func(t *testing.T, h *skewGapHarness) {
				id := h.putWithTimestamp(t, delConverge, pastTs)
				require.NoError(t, h.shard.DeleteObject(h.ctx, id, time.Now().Add(24*time.Hour)))
			},
			wantPresentWithGap:   false,
			wantPresentWhenFixed: false,
		},
		{
			// THE delete-side gap: a resurrected stale posting (false-positive
			// filter match), the distinct failure mode from the missing row
			// above. A live write mirrors delResurrect into the ingest
			// bucket; the delete then BYPASSES the tee (models a delete that
			// never reaches the double-write mirror). No compensating
			// tombstone is written, so the mirrored posting survives the
			// migration and a filter on delResurrect matches a deleted
			// object.
			//
			// NOTE: unlike the add-side gap this is not closed by an
			// always-backfill skip-predicate change — the deleted object is
			// gone from the objects snapshot, so no backfill decision touches
			// it. Closing it requires the double-write tee to cover the
			// delete path. The batch-references / nested-property direct-write
			// bypass is the concrete production instance of a delete that
			// escapes the tee; it is tracked separately (it blocks any
			// "closes #11692 for ref-property migrations" claim).
			name:     "delete_tee_bypass_resurrection_gap",
			sentinel: delResurrect,
			arrangeMirrored: func(t *testing.T, h *skewGapHarness) {
				id := h.putWithTimestamp(t, delResurrect, pastTs)
				h.bypassDeleteTee()
				require.NoError(t, h.shard.DeleteObject(h.ctx, id, time.Now().Add(24*time.Hour)))
			},
			wantPresentWithGap:   true,  // stale posting resurrected (known-bad)
			wantPresentWhenFixed: false, // tee covers the delete ⇒ pruned
			gapDoc: "delete-side: a posting the double-write mirror added survives a " +
				"delete that never reached the tee — a resurrected stale posting / " +
				"false-positive filter match on a deleted object",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "MultiNodeClockSkewGap_" + uuid.NewString()[:8]
			class := newFilterableToRangeableTestClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			// Positive-control corpus: all timestamp 0, strictly below any
			// reindexStarted, so the backfill indexes every one. Guards
			// against a vacuous pass (if the bucket ended up empty the
			// sentinel assertion would prove nothing).
			for _, obj := range makeFilterableToRangeableTestObjects(t, numCorpus, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			h := &skewGapHarness{ctx: ctx, className: className, propName: filterableToRangeablePropName}
			h.shard = shard

			if tc.arrangeUnmirrored != nil {
				tc.arrangeUnmirrored(t, h)
			}

			task, wrapped := newFilterableToRangeableTask(t, idx, className, filterableToRangeablePropName)
			h.task = task
			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			if tc.arrangeMirrored != nil {
				tc.arrangeMirrored(t, h)
			}

			for {
				rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				require.NoError(t, err)
				if rerunAt.IsZero() {
					break
				}
			}
			require.True(t, wrapped.migrationCompleted, "migration must complete")

			rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName))
			require.NotNil(t, rangeBucket, "post-migration rangeable bucket must exist")

			// Vacuity guard: a corpus value must be backfilled, else the
			// migration populated nothing and the sentinel assertion below
			// would prove nothing.
			require.NotEmpty(t, readRangeableDocIDs(t, rangeBucket, 0),
				"positive control: backfilled corpus value 0 must be present")

			isGap := tc.wantPresentWithGap != tc.wantPresentWhenFixed
			wantPresent := tc.wantPresentWhenFixed
			if !gapFixed {
				wantPresent = tc.wantPresentWithGap
				if isGap {
					t.Logf("KNOWN-GAP (weaviate/weaviate#11692) %s: %s. Asserting the "+
						"current (gap-open) outcome; this line disappears and the "+
						"assertion flips when the fix lands.", tc.name, tc.gapDoc)
				}
			}

			got := readRangeableDocIDs(t, rangeBucket, tc.sentinel)
			if wantPresent {
				assert.Lenf(t, got, 1,
					"sentinel value %d must be present (case %q)", tc.sentinel, tc.name)
			} else {
				assert.Emptyf(t, got,
					"sentinel value %d must be absent (case %q)", tc.sentinel, tc.name)
			}
		})
	}
}
