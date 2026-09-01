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
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The crash-safe delete tests cover the delete ordering required for docID
// reuse: inverted-index posting removals must happen (and be made durable)
// BEFORE the object row delete, because an orphaned posting (docID in a
// posting, no object row) is unrepairable — the row's bytes are the only
// source of which postings to remove — and, once docIDs are reused, resolves
// to a DIFFERENT object.

const (
	crashSafeTextProp = "description"
	crashSafeIntProp  = "points"
)

func crashSafeDeleteClass(className string) *models.Class {
	vTrue := true
	return &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:            crashSafeTextProp,
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWhitespace,
				IndexFilterable: &vTrue,
				IndexSearchable: &vTrue,
			},
			{
				Name:              crashSafeIntProp,
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vTrue,
				IndexRangeFilters: &vTrue,
			},
		},
		InvertedIndexConfig: crashSafeInvertedConfig(),
	}
}

func crashSafeInvertedConfig() *models.InvertedIndexConfig {
	cfg := invertedConfig()
	cfg.IndexTimestamps = true
	return cfg
}

// crashSafeConcreteShard unwraps the ShardLike returned by the fixtures into
// the concrete *Shard the phase-level tests drive.
func crashSafeConcreteShard(t *testing.T, ctx context.Context, s ShardLike) *Shard {
	t.Helper()
	switch sh := s.(type) {
	case *Shard:
		return sh
	case *LazyLoadShard:
		require.NoError(t, sh.Load(ctx))
		return sh.shard
	default:
		t.Fatalf("unexpected shard type %T", s)
		return nil
	}
}

func crashSafeTestShard(t *testing.T, ctx context.Context, className string) *Shard {
	t.Helper()
	shd, _ := testShardWithSettings(t, ctx, crashSafeDeleteClass(className),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	return crashSafeConcreteShard(t, ctx, shd)
}

func crashSafeTestObject(className string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				crashSafeTextProp: "alpha bravo",
				crashSafeIntProp:  float64(42),
			},
		},
	}
}

// crashSafeObjectState reads everything the assertions below need about a
// stored object: its row bytes, docID and the analyzed inverted properties.
type crashSafeObjectState struct {
	idBytes  []byte
	row      []byte
	docID    uint64
	props    []inverted.Property
	nilProps []inverted.NilProperty
}

func crashSafeReadObjectState(t *testing.T, s *Shard, id strfmt.UUID) *crashSafeObjectState {
	t.Helper()

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	require.NoError(t, err)

	bucket, err := s.objectsBucket()
	require.NoError(t, err)

	row, err := bucket.Get(idBytes)
	require.NoError(t, err)
	require.NotNil(t, row, "object row must exist")

	docID, _, err := storobj.DocIDAndTimeFromBinary(row)
	require.NoError(t, err)

	className, err := bucket.ClassName()
	require.NoError(t, err)
	obj, err := storobj.FromBinaryDisk(row, className)
	require.NoError(t, err)

	props, nilProps, _, err := s.AnalyzeObject(obj)
	require.NoError(t, err)

	return &crashSafeObjectState{
		idBytes:  idBytes,
		row:      row,
		docID:    docID,
		props:    props,
		nilProps: nilProps,
	}
}

func crashSafeRowPresent(t *testing.T, s *Shard, idBytes []byte) bool {
	t.Helper()
	bucket, err := s.objectsBucket()
	require.NoError(t, err)
	row, err := bucket.Get(idBytes)
	require.NoError(t, err)
	return row != nil
}

// crashSafeSetBucketContains reports whether docID is a member of key's
// posting in a Set/RoaringSet-strategy bucket.
func crashSafeSetBucketContains(t *testing.T, ctx context.Context, b *lsmkv.Bucket, key []byte, docID uint64) bool {
	t.Helper()
	switch b.Strategy() {
	case lsmkv.StrategySetCollection:
		list, err := b.SetList(key)
		require.NoError(t, err)
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		for _, v := range list {
			if bytes.Equal(v, docIDBytes) {
				return true
			}
		}
		return false
	case lsmkv.StrategyRoaringSet:
		bm, release, err := b.RoaringSetGet(ctx, key)
		require.NoError(t, err)
		defer release()
		return bm.Contains(docID)
	default:
		t.Fatalf("unexpected set bucket strategy %q", b.Strategy())
		return false
	}
}

// crashSafePostingsForDocID reports which inverted structures still contain a
// posting for docID, keyed by a human-readable structure descriptor. It
// covers filterable, searchable, rangeable, null and property-length indexes
// for every analyzed property (timestamp props included via IndexTimestamps).
func crashSafePostingsForDocID(t *testing.T, ctx context.Context, s *Shard,
	st *crashSafeObjectState,
) map[string]bool {
	t.Helper()
	found := map[string]bool{}

	for _, prop := range st.props {
		if prop.HasFilterableIndex {
			b := s.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
			require.NotNil(t, b, "filterable bucket for %q", prop.Name)
			for _, item := range prop.Items {
				if crashSafeSetBucketContains(t, ctx, b, item.Data, st.docID) {
					found["filterable/"+prop.Name] = true
				}
			}
		}

		if prop.HasSearchableIndex {
			b := s.store.Bucket(helpers.BucketSearchableFromPropNameLSM(prop.Name))
			require.NotNil(t, b, "searchable bucket for %q", prop.Name)
			for _, item := range prop.Items {
				pointers, err := b.DocPointerWithScoreList(ctx, item.Data, 1)
				require.NoError(t, err)
				for _, ptr := range pointers {
					if ptr.Id == st.docID && ptr.Frequency > 0 {
						found["searchable/"+prop.Name] = true
					}
				}
			}
		}

		if prop.HasRangeableIndex {
			b := s.store.Bucket(helpers.BucketRangeableFromPropNameLSM(prop.Name))
			require.NotNil(t, b, "rangeable bucket for %q", prop.Name)
			for _, item := range prop.Items {
				reader := b.ReaderRoaringSetRange()
				bm, release, err := reader.Read(ctx, binary.BigEndian.Uint64(item.Data), filters.OperatorEqual)
				require.NoError(t, err)
				if bm.Contains(st.docID) {
					found["rangeable/"+prop.Name] = true
				}
				release()
				reader.Close()
			}
		}

		if isMetaCountProperty(prop) || isInternalProperty(prop) {
			continue
		}

		if s.index.invertedIndexConfig.IndexPropertyLength && prop.Length >= 0 {
			b := s.store.Bucket(helpers.BucketFromPropNameLengthLSM(prop.Name))
			require.NotNil(t, b, "length bucket for %q", prop.Name)
			key, err := bucketKeyPropertyLength(prop.Length)
			require.NoError(t, err)
			if crashSafeSetBucketContains(t, ctx, b, key, st.docID) {
				found["length/"+prop.Name] = true
			}
		}

		if s.index.invertedIndexConfig.IndexNullState {
			b := s.store.Bucket(helpers.BucketFromPropNameNullLSM(prop.Name))
			require.NotNil(t, b, "null bucket for %q", prop.Name)
			key, err := bucketKeyPropertyNull(prop.Length == 0)
			require.NoError(t, err)
			if crashSafeSetBucketContains(t, ctx, b, key, st.docID) {
				found["null/"+prop.Name] = true
			}
		}
	}

	return found
}

func crashSafeAnyPostingPresent(t *testing.T, ctx context.Context, s *Shard,
	st *crashSafeObjectState,
) bool {
	t.Helper()
	return len(crashSafePostingsForDocID(t, ctx, s, st)) > 0
}

// TestDeleteCrashSafe_NoOrphanPostingAtAnyPhase walks every phase of the
// single-object delete and asserts THE invariant that makes deletes
// crash-safe for docID reuse: at no intermediate state may an inverted
// posting reference a docID whose object row is gone. The old ordering (row
// delete first, inverted cleanup after) violates this between the row delete
// and the cleanup.
func TestDeleteCrashSafe_NoOrphanPostingAtAnyPhase(t *testing.T) {
	ctx := testCtx()
	s := crashSafeTestShard(t, ctx, "DeleteCrashSafeInvariant")

	obj := crashSafeTestObject("DeleteCrashSafeInvariant")
	require.NoError(t, s.PutObject(ctx, obj))
	st := crashSafeReadObjectState(t, s, obj.ID())

	// Sanity: postings exist while the row exists.
	require.True(t, crashSafeAnyPostingPresent(t, ctx, s, st))

	var phases []string
	s.testDeletePhaseHook = func(phase string) {
		phases = append(phases, phase)
		if crashSafeAnyPostingPresent(t, ctx, s, st) {
			require.True(t, crashSafeRowPresent(t, s, st.idBytes),
				"after phase %q: inverted posting present for docID %d but object row is gone (orphan posting)",
				phase, st.docID)
		}
	}
	defer func() { s.testDeletePhaseHook = nil }()

	require.NoError(t, s.DeleteObject(ctx, obj.ID(), time.Time{}))

	require.Equal(t,
		[]string{deletePhasePrepared, deletePhaseCleanedUp, deletePhaseBarrier, deletePhaseRowDeleted},
		phases, "delete phases must run in the crash-safe order")

	require.False(t, crashSafeRowPresent(t, s, st.idBytes))
	require.False(t, crashSafeAnyPostingPresent(t, ctx, s, st))
}

// TestDeleteCrashSafe_RetryAfterCrashBeforeRowDelete simulates a crash in the
// window the new ordering leaves open: the inverted cleanup ran and was made
// durable, but the process died before the row delete. The state is
// row-without-postings, and retrying the delete through the public API must
// converge (row gone, postings gone) with no error.
func TestDeleteCrashSafe_RetryAfterCrashBeforeRowDelete(t *testing.T) {
	ctx := testCtx()
	s := crashSafeTestShard(t, ctx, "DeleteCrashSafeRetry")

	obj := crashSafeTestObject("DeleteCrashSafeRetry")
	require.NoError(t, s.PutObject(ctx, obj))
	st := crashSafeReadObjectState(t, s, obj.ID())

	// Run the delete only up to (and including) the barrier — the row delete
	// never happens, simulating a crash right before it.
	touched, err := s.cleanupInvertedIndexOnDelete(st.row, st.docID)
	require.NoError(t, err)
	require.NoError(t, s.invertedDeleteBarrier(ctx, touched))

	require.True(t, crashSafeRowPresent(t, s, st.idBytes),
		"simulated crash: row must still exist")
	require.Empty(t, crashSafePostingsForDocID(t, ctx, s, st),
		"simulated crash: postings must already be removed")

	// Retry through the public API: must converge with no error.
	require.NoError(t, s.DeleteObject(ctx, obj.ID(), time.Time{}))

	require.False(t, crashSafeRowPresent(t, s, st.idBytes))
	require.Empty(t, crashSafePostingsForDocID(t, ctx, s, st))
}

// TestDeleteCrashSafe_CleanupIdempotentPerStructure pins that a partially
// completed cleanup (crash mid-cleanup) does not break the retry: for each
// inverted structure, remove the object's posting once at the lsm level, then
// run the full delete — it must succeed and converge.
func TestDeleteCrashSafe_CleanupIdempotentPerStructure(t *testing.T) {
	ctx := testCtx()
	s := crashSafeTestShard(t, ctx, "DeleteCrashSafeIdempotent")

	propByName := func(st *crashSafeObjectState, name string) *inverted.Property {
		for i := range st.props {
			if st.props[i].Name == name {
				return &st.props[i]
			}
		}
		return nil
	}

	tests := []struct {
		name string
		// preDelete removes exactly one structure's posting(s), simulating a
		// crash that happened right after that removal.
		preDelete func(t *testing.T, st *crashSafeObjectState)
	}{
		{
			name: "filterable (roaringset/set)",
			preDelete: func(t *testing.T, st *crashSafeObjectState) {
				prop := propByName(st, crashSafeTextProp)
				require.NotNil(t, prop)
				b := s.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
				require.NotNil(t, b)
				for _, item := range prop.Items {
					require.NoError(t, s.deleteFromPropertySetBucket(b, st.docID, item.Data))
				}
			},
		},
		{
			name: "searchable (map/inverted)",
			preDelete: func(t *testing.T, st *crashSafeObjectState) {
				prop := propByName(st, crashSafeTextProp)
				require.NotNil(t, prop)
				b := s.store.Bucket(helpers.BucketSearchableFromPropNameLSM(prop.Name))
				require.NotNil(t, b)
				for _, item := range prop.Items {
					require.NoError(t, s.deleteInvertedIndexItemWithFrequencyLSM(b, item, st.docID))
				}
			},
		},
		{
			name: "rangeable (roaringsetrange)",
			preDelete: func(t *testing.T, st *crashSafeObjectState) {
				prop := propByName(st, crashSafeIntProp)
				require.NotNil(t, prop)
				b := s.store.Bucket(helpers.BucketRangeableFromPropNameLSM(prop.Name))
				require.NotNil(t, b)
				for _, item := range prop.Items {
					require.NoError(t, s.deleteFromPropertyRangeBucket(b, st.docID, item.Data))
				}
			},
		},
		{
			name: "null index",
			preDelete: func(t *testing.T, st *crashSafeObjectState) {
				prop := propByName(st, crashSafeTextProp)
				require.NotNil(t, prop)
				require.NoError(t, s.deleteFromPropertyNullIndex(prop.Name, st.docID, prop.Length == 0, nil))
			},
		},
		{
			name: "prop-length index",
			preDelete: func(t *testing.T, st *crashSafeObjectState) {
				prop := propByName(st, crashSafeTextProp)
				require.NotNil(t, prop)
				require.NoError(t, s.deleteFromPropertyLengthIndex(prop.Name, st.docID, prop.Length, nil))
			},
		},
		{
			name: "timestamp buckets",
			preDelete: func(t *testing.T, st *crashSafeObjectState) {
				prop := propByName(st, filters.InternalPropCreationTimeUnix)
				require.NotNil(t, prop, "timestamp prop must be analyzed (IndexTimestamps on)")
				b := s.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
				require.NotNil(t, b)
				for _, item := range prop.Items {
					require.NoError(t, s.deleteFromPropertySetBucket(b, st.docID, item.Data))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := crashSafeTestObject("DeleteCrashSafeIdempotent")
			require.NoError(t, s.PutObject(ctx, obj))
			st := crashSafeReadObjectState(t, s, obj.ID())

			tt.preDelete(t, st)

			// The full delete re-runs the cleanup including the structure
			// already emptied above; it must not error and must converge.
			require.NoError(t, s.DeleteObject(ctx, obj.ID(), time.Time{}))

			require.False(t, crashSafeRowPresent(t, s, st.idBytes))
			require.Empty(t, crashSafePostingsForDocID(t, ctx, s, st))
		})
	}
}

// TestDeleteCrashSafe_BarrierRespectsDocIDReuseFlag pins the barrier
// dispatch: with DOCID_REUSE_ENABLED the barrier syncs (fsync) exactly the
// touched buckets via Store.SyncWALs — observable because SyncWALs errors on
// an unknown bucket name — and without the flag it stays on the WriteWALs
// page-cache flush, which never looks at the names.
func TestDeleteCrashSafe_BarrierRespectsDocIDReuseFlag(t *testing.T) {
	ctx := testCtx()
	s := crashSafeTestShard(t, ctx, "DeleteCrashSafeFlag")

	obj := crashSafeTestObject("DeleteCrashSafeFlag")
	require.NoError(t, s.PutObject(ctx, obj))
	st := crashSafeReadObjectState(t, s, obj.ID())

	t.Run("cleanup reports every touched bucket name", func(t *testing.T) {
		touched, err := s.cleanupInvertedIndexOnDelete(st.row, st.docID)
		require.NoError(t, err)

		expected := []string{
			helpers.BucketFromPropNameLSM(crashSafeTextProp),
			helpers.BucketSearchableFromPropNameLSM(crashSafeTextProp),
			helpers.BucketFromPropNameLSM(crashSafeIntProp),
			helpers.BucketRangeableFromPropNameLSM(crashSafeIntProp),
			helpers.BucketFromPropNameNullLSM(crashSafeTextProp),
			helpers.BucketFromPropNameLengthLSM(crashSafeTextProp),
			helpers.BucketFromPropNameLSM(filters.InternalPropCreationTimeUnix),
			helpers.BucketFromPropNameLSM(filters.InternalPropLastUpdateTimeUnix),
		}
		require.Subset(t, touched.list(), expected)
		require.False(t, touched.all)
	})

	t.Run("flag on routes the touched names into SyncWALs", func(t *testing.T) {
		t.Setenv("DOCID_REUSE_ENABLED", "true")

		bogus := newTouchedBuckets()
		bogus.add("no_such_bucket")
		err := s.invertedDeleteBarrier(ctx, bogus)
		require.ErrorIs(t, err, lsmkv.ErrBucketNotFound,
			"an unknown touched bucket must surface SyncWALs' error — proving the names reach SyncWALs")

		real := newTouchedBuckets()
		real.add(helpers.BucketFromPropNameLSM(crashSafeTextProp))
		require.NoError(t, s.invertedDeleteBarrier(ctx, real))
	})

	t.Run("flag off stays on the WriteWALs path", func(t *testing.T) {
		t.Setenv("DOCID_REUSE_ENABLED", "false")

		bogus := newTouchedBuckets()
		bogus.add("no_such_bucket")
		require.NoError(t, s.invertedDeleteBarrier(ctx, bogus),
			"without the flag the barrier must not consult bucket names (WriteWALs path)")
	})

	t.Run("flag on, full delete end to end", func(t *testing.T) {
		t.Setenv("DOCID_REUSE_ENABLED", "true")

		obj2 := crashSafeTestObject("DeleteCrashSafeFlag")
		require.NoError(t, s.PutObject(ctx, obj2))
		st2 := crashSafeReadObjectState(t, s, obj2.ID())

		require.NoError(t, s.DeleteObject(ctx, obj2.ID(), time.Time{}))
		require.False(t, crashSafeRowPresent(t, s, st2.idBytes))
		require.Empty(t, crashSafePostingsForDocID(t, ctx, s, st2))
	})
}

// TestDeleteCrashSafe_DeleteByFilterViaBatch covers the delete-by-filter
// funnel: filter-matched uuids are resolved (FindUUIDs) and deleted through
// DeleteObjectBatch — the same crash-safe batch path — leaving rows AND
// postings gone for matches and intact for non-matches.
func TestDeleteCrashSafe_DeleteByFilterViaBatch(t *testing.T) {
	ctx := testCtx()
	className := "DeleteCrashSafeByFilter"
	s := crashSafeTestShard(t, ctx, className)

	matching1 := crashSafeTestObject(className)
	matching2 := crashSafeTestObject(className)
	nonMatching := crashSafeTestObject(className)
	nonMatching.Object.Properties = map[string]interface{}{
		crashSafeTextProp: "charlie delta",
		crashSafeIntProp:  float64(7),
	}

	for _, obj := range []*storobj.Object{matching1, matching2, nonMatching} {
		require.NoError(t, s.PutObject(ctx, obj))
	}

	states := map[strfmt.UUID]*crashSafeObjectState{}
	for _, obj := range []*storobj.Object{matching1, matching2, nonMatching} {
		states[obj.ID()] = crashSafeReadObjectState(t, s, obj.ID())
	}

	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On: &filters.Path{
			Class:    schema.ClassName(className),
			Property: schema.PropertyName(crashSafeIntProp),
		},
		Value: &filters.Value{Value: 42, Type: schema.DataTypeInt},
	}}

	uuids, err := s.FindUUIDs(ctx, filter, 100)
	require.NoError(t, err)
	require.ElementsMatch(t, []strfmt.UUID{matching1.ID(), matching2.ID()}, uuids)

	result := s.DeleteObjectBatch(ctx, uuids, time.Time{}, false)
	for _, r := range result {
		require.NoError(t, r.Err)
	}

	for _, obj := range []*storobj.Object{matching1, matching2} {
		st := states[obj.ID()]
		require.False(t, crashSafeRowPresent(t, s, st.idBytes), "matched object row must be gone")
		require.Empty(t, crashSafePostingsForDocID(t, ctx, s, st), "matched object postings must be gone")
	}

	st := states[nonMatching.ID()]
	require.True(t, crashSafeRowPresent(t, s, st.idBytes), "non-matching object must survive")
	require.NotEmpty(t, crashSafePostingsForDocID(t, ctx, s, st))
}
