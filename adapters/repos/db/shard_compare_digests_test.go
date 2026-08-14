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
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/replica"
)

// withDeletionStrategy installs a DeletionStrategy on the index. Shard.CompareDigests
// does NOT branch on it — the strategy is varied across the Tombstoned_* subtests
// purely to prove that the comparator's verdicts are invariant under it (tombstone
// resolution happens later, on the source, via resolveObjectConflict).
func withDeletionStrategy(strategy string) func(*Index) {
	return func(idx *Index) {
		idx.Config.DeletionStrategy = strategy
	}
}

// TestShardCompareDigestsStrategies covers the per-entry verdict logic in
// Shard.CompareDigests across all classifications (live match, missing, stale,
// tombstoned). The comparator is DeletionStrategy-agnostic, so the tombstoned
// cases are run under every DeletionStrategy value to assert the verdict does
// not change. The cursor merge-join itself is not the focus here; tests use a
// single source UUID per case to keep the merge mechanics out of the way.
func TestShardCompareDigestsStrategies(t *testing.T) {
	ctx := context.Background()
	const class = "CompareDigestsStrategiesTest"

	const (
		tsOlder  int64 = 1_000
		tsMiddle int64 = 2_000
		tsNewer  int64 = 3_000
	)

	t.Run("EmptyInput", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)

		out, err := s.CompareDigests(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, out)

		out, err = s.CompareDigests(ctx, []routerTypes.RepairResponse{})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("LiveExactMatch_NotReturned", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsMiddle)))

		s := concreteShard(t, sl)
		out, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: tsMiddle},
		})
		require.NoError(t, err)
		assert.Empty(t, out, "equal-timestamp entries are intentionally not returned")
	})

	t.Run("LiveSourceNewer_ReturnsLocalTime", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsOlder)))

		s := concreteShard(t, sl)
		out, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: tsNewer},
		})
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, string(uuidLow), out[0].ID)
		assert.Equal(t, tsOlder, out[0].UpdateTime, "must report local UpdateTime so caller treats source as stale-on-target")
		assert.False(t, out[0].Deleted)
	})

	t.Run("LiveLocalNewer_NotReturned", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsNewer)))

		s := concreteShard(t, sl)
		out, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: tsOlder},
		})
		require.NoError(t, err)
		assert.Empty(t, out, "local has the newer object — source must not propagate")
	})

	t.Run("MissingOnTarget_ReturnsZero", func(t *testing.T) {
		sl, _ := testShard(t, ctx, class)
		// no PutObject — uuidLow is missing

		s := concreteShard(t, sl)
		out, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(uuidLow), UpdateTime: tsMiddle},
		})
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, string(uuidLow), out[0].ID)
		assert.Equal(t, int64(0), out[0].UpdateTime, "missing entries are signalled with UpdateTime=0")
		assert.False(t, out[0].Deleted)
	})

	// TombstonedTargetReturnedAsMissing exercises the new contract: the
	// target's CompareDigests is strategy-agnostic. A tombstoned UUID is not
	// visible to Bucket.Cursor() (cursor skips tombstones), so the cursor walk
	// emits it as missing — UpdateTime=0, Deleted=false — regardless of the
	// configured DeletionStrategy. Tombstone resolution happens later, on the
	// source side, via the existing post-Overwrite resolveObjectConflict path.
	t.Run("TombstonedTargetReturnedAsMissing", func(t *testing.T) {
		strategies := []string{
			models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			models.ReplicationConfigDeletionStrategyDeleteOnConflict,
			models.ReplicationConfigDeletionStrategyTimeBasedResolution,
		}
		for _, strategy := range strategies {
			t.Run(strategy, func(t *testing.T) {
				sl, _ := testShard(t, ctx, class, withDeletionStrategy(strategy))
				require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsMiddle)))
				require.NoError(t, sl.DeleteObject(ctx, uuidLow, time.UnixMilli(tsNewer)))

				s := concreteShard(t, sl)
				out, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
					{ID: string(uuidLow), UpdateTime: tsOlder},
				})
				require.NoError(t, err)
				require.Len(t, out, 1)
				assert.Equal(t, string(uuidLow), out[0].ID)
				assert.False(t, out[0].Deleted,
					"target must never set Deleted=true; tombstone resolution is the source's job")
				assert.Equal(t, int64(0), out[0].UpdateTime,
					"tombstoned key must be reported as missing (UpdateTime=0)")
			})
		}
	})
}

// TestShardCompareDigestsCursorMergeJoin exercises the merge-join scenarios
// where the cursor and source-digest stream interact: partial overlap, cursor
// exhaustion mid-batch, and many objects in lex order. Source UUIDs absent
// from the cursor are emitted as missing (UpdateTime=0) — the cursor already
// skips tombstones, so missing-vs-tombstoned is intentionally indistinguishable.
func TestShardCompareDigestsCursorMergeJoin(t *testing.T) {
	ctx := context.Background()
	const class = "CompareDigestsCursorMergeJoinTest"

	const ts int64 = 1_000

	// orderedUUIDs returns n UUIDs in strict lex order (00000000-..., 00000001-..., …).
	orderedUUIDs := func(n int) []strfmt.UUID {
		out := make([]strfmt.UUID, n)
		for i := range n {
			var u uuid.UUID
			u[15] = byte(i + 1) // last byte
			u[14] = byte((i + 1) >> 8)
			out[i] = strfmt.UUID(u.String())
		}
		// defensive sort in case of carry boundaries
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}

	t.Run("PartialOverlap_MissingReportedAsZero", func(t *testing.T) {
		// Local has [A, B, C]; source sends [B, D]. B matches via cursor; D
		// (which sorts after C) is not in the cursor and is emitted as missing.
		ids := orderedUUIDs(4)
		a, b, c, d := ids[0], ids[1], ids[2], ids[3]

		sl, _ := testShard(t, ctx, class)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, a, ts)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, b, ts)))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, c, ts)))

		s := concreteShard(t, sl)
		out, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(b), UpdateTime: ts},     // exact match — must NOT be returned
			{ID: string(d), UpdateTime: ts + 1}, // missing — must be returned with UpdateTime=0
		})
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, string(d), out[0].ID)
		assert.Equal(t, int64(0), out[0].UpdateTime)
		assert.False(t, out[0].Deleted, "target must never set Deleted=true; tombstone resolution is the source's job")
	})

	t.Run("CursorExhaustsBeforeSource", func(t *testing.T) {
		// Local has only [A]; source sends [A, B, C]. After matching A the
		// cursor is exhausted; B and C must be reported as missing
		// (UpdateTime=0).
		ids := orderedUUIDs(3)
		a, b, c := ids[0], ids[1], ids[2]

		sl, _ := testShard(t, ctx, class)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, a, ts)))

		s := concreteShard(t, sl)
		out, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(a), UpdateTime: ts + 1},
			{ID: string(b), UpdateTime: ts + 1},
			{ID: string(c), UpdateTime: ts + 1},
		})
		require.NoError(t, err)
		require.Len(t, out, 3)
		// Sort the output for stable assertions; CompareDigests preserves source order.
		assert.Equal(t, string(a), out[0].ID)
		assert.Equal(t, ts, out[0].UpdateTime, "A is stale — must report local UpdateTime")
		assert.False(t, out[0].Deleted)
		assert.Equal(t, string(b), out[1].ID)
		assert.Equal(t, int64(0), out[1].UpdateTime, "B is missing")
		assert.False(t, out[1].Deleted)
		assert.Equal(t, string(c), out[2].ID)
		assert.Equal(t, int64(0), out[2].UpdateTime, "C is missing")
		assert.False(t, out[2].Deleted)
	})

	t.Run("UnsortedInputRejected", func(t *testing.T) {
		ids := orderedUUIDs(2)
		sl, _ := testShard(t, ctx, class)
		s := concreteShard(t, sl)

		_, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(ids[1]), UpdateTime: ts},
			{ID: string(ids[0]), UpdateTime: ts},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not in strict lex order")

		_, err = s.CompareDigests(ctx, []routerTypes.RepairResponse{
			{ID: string(ids[0]), UpdateTime: ts},
			{ID: string(ids[0]), UpdateTime: ts},
		})
		require.Error(t, err, "duplicates violate strict order and must also be rejected")
	})

	t.Run("ManyObjects_LexOrderingHonoured", func(t *testing.T) {
		// Local holds 50 sequential UUIDs at tsLocal; source sends every other
		// one at tsSource > tsLocal. Verify the merge-join correctly identifies
		// each as stale (source newer), with no off-by-one.
		const n = 50
		ids := orderedUUIDs(n)
		const (
			tsLocal  int64 = 100
			tsSource int64 = 200
		)

		sl, _ := testShard(t, ctx, class)
		for _, id := range ids {
			require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsLocal)))
		}

		// Take every other UUID — exercises cursor advancement past unmatched keys.
		probe := make([]routerTypes.RepairResponse, 0, n/2)
		for i := 0; i < n; i += 2 {
			probe = append(probe, routerTypes.RepairResponse{
				ID: string(ids[i]), UpdateTime: tsSource,
			})
		}

		s := concreteShard(t, sl)
		out, err := s.CompareDigests(ctx, probe)
		require.NoError(t, err)
		require.Len(t, out, len(probe), "every probed UUID is stale — all must be returned")
		for i, r := range out {
			assert.Equal(t, string(ids[2*i]), r.ID, "result order must mirror source order")
			assert.Equal(t, tsLocal, r.UpdateTime)
			assert.False(t, r.Deleted)
		}
	})
}

// TestCompareDigestsBatchSizeFitsBodyCap guards that maxDiffBatchSize batches
// always fit under the REST body cap.
func TestCompareDigestsBatchSizeFitsBodyCap(t *testing.T) {
	maxBytes := maxDiffBatchSize * replica.CompareDigestsRecordLength
	assert.Less(t, maxBytes, replica.CompareDigestsMaxBodyBytes,
		"maxDiffBatchSize (%d) × CompareDigestsRecordLength (%d) = %d bytes must stay under the REST body cap (%d)",
		maxDiffBatchSize, replica.CompareDigestsRecordLength, maxBytes, replica.CompareDigestsMaxBodyBytes)
}
