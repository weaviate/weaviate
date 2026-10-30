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
	"encoding/binary"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type reproObj struct {
	id    strfmt.UUID
	vec   []float32
	docID uint64
}

type reproLoss struct {
	secondaryNil    int
	secondaryWrong  int
	byDocIDMissing  int
	searchMissing   int
	rowCheckDefeats int
}

// reproShard is reuseTestShard with a defaulted HNSW config, so nearVector
// reliably finds an object by its own vector.
func reproShard(t *testing.T, ctx context.Context) *Shard {
	t.Helper()
	class := &models.Class{
		Class: reuseTestClass,
		Properties: []*models.Property{{
			Name:         "category",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}},
	}
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.NewDefaultUserConfig(), false, false)
	s := shd.(*Shard)
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	return s
}

func reproObjectsBucketSegments(s *Shard) int {
	view := s.store.Bucket(helpers.ObjectsBucketLSM).GetConsistentView()
	defer view.ReleaseView()
	return len(view.Disk)
}

func reproFlushObjects(t *testing.T, s *Shard) {
	t.Helper()
	require.NoError(t, s.store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())
}

func reproCompactToOneSegment(t *testing.T, s *Shard) {
	t.Helper()
	never := cyclemanager.ShouldAbortCallback(func() bool { return false })
	for i := 0; i < 50 && reproObjectsBucketSegments(s) > 1; i++ {
		s.cycleCallbacks.compactionCallbacks.CycleCallback(never)
		if s.cycleCallbacks.compactionAuxCallbacks != nil {
			s.cycleCallbacks.compactionAuxCallbacks.CycleCallback(never)
		}
	}
	require.Equal(t, 1, reproObjectsBucketSegments(s), "objects bucket must be compacted into one segment")
}

func reproMeasureLoss(t *testing.T, s *Shard, objs []reproObj) reproLoss {
	t.Helper()
	ctx := context.Background()
	var loss reproLoss

	bucket, release, err := s.objectsBucket()
	require.NoError(t, err)
	docIDs := make([]uint64, len(objs))
	var unresolvable []uint64
	for i, o := range objs {
		docIDs[i] = o.docID
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, o.docID)
		row, err := bucket.GetBySecondary(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, key)
		require.NoError(t, err)
		if row == nil {
			loss.secondaryNil++
			unresolvable = append(unresolvable, o.docID)
			continue
		}
		got, err := storobj.FromBinaryUUIDOnlyDisk(row, reuseTestClass)
		require.NoError(t, err)
		if got.ID() != o.id {
			loss.secondaryWrong++
		}
	}
	resolved, err := storobj.ObjectsByDocIDWithEmpty(bucket, docIDs, additional.Properties{}, nil, s.index.logger)
	release()
	require.NoError(t, err)
	for i, o := range resolved {
		if o == nil || o.ID() != objs[i].id {
			loss.byDocIDMissing++
		}
	}

	for _, o := range objs {
		found, _, err := s.ObjectVectorSearch(ctx, []models.Vector{o.vec}, []string{""}, 0, 1,
			nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		if len(found) == 0 || found[0].ID() != o.id {
			loss.searchMissing++
		}
	}

	for _, id := range unresolvable {
		err := s.freeList.assertNoTraces(id)
		if err == nil || !strings.Contains(err.Error(), "an object row still references it") {
			loss.rowCheckDefeats++
		}
	}
	return loss
}

// TestDocIDReuseRepro_DuplicateSecondaryKeyHidesReusedObject pins that a live
// object on a reused docID stays resolvable once the old life's tombstone
// (which keeps the docID secondary key) and the new life's row share a segment.
func TestDocIDReuseRepro_DuplicateSecondaryKeyHidesReusedObject(t *testing.T) {
	// ADAPTATION (only change from Jero's verbatim appendix in PR #13036's
	// review, vendored at audit/pr13036-docid-reuse-review.md on
	// tests/docid-reuse-regression): skipped per the regression-suite banner
	// convention. Verified RED at this branch head (f03e86a0fd): 44-60/100 live
	// objects on reused docIDs lost (audit recorded 41-55/100). Finding C1.
	t.Skip("pins finding C1 (duplicate secondary key hides a reused object); no fix yet — see docid-reuse-jero-audit. Jero's verbatim repro, verified RED at f03e86a0fd.")

	const (
		anchors = 5
		n       = 100
		dims    = 16
	)

	tests := []struct {
		name             string
		flushAfterPut    bool
		flushAfterDelete bool
	}{
		{name: "put, delete and reuse in one memtable, single flush"},
		{name: "put in own segment, delete and reuse in the next, then compaction", flushAfterPut: true},
		{name: "delete and reuse in separate segments, then compaction", flushAfterDelete: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DOCID_REUSE_ENABLED", "true")
			ctx := context.Background()
			s := reproShard(t, ctx)
			require.NoError(t, s.cycleCallbacks.compactionCallbacksCtrl.Deactivate(ctx))
			require.NoError(t, s.cycleCallbacks.compactionAuxCallbacksCtrl.Deactivate(ctx))
			require.NoError(t, s.cycleCallbacks.flushCallbacksCtrl.Deactivate(ctx))

			r := rand.New(rand.NewSource(42))
			randVec := func() []float32 {
				v := make([]float32, dims)
				for i := range v {
					v[i] = r.Float32()
				}
				return v
			}

			for i := 0; i < anchors; i++ {
				reusePutObject(t, s, randVec())
			}
			oldDocIDs := map[uint64]struct{}{}
			old := make([]strfmt.UUID, n)
			for i := range old {
				old[i] = reusePutObject(t, s, randVec())
				oldDocIDs[reuseDocIDOf(t, s, old[i])] = struct{}{}
			}
			if tt.flushAfterPut {
				reproFlushObjects(t, s)
			}

			for _, id := range old {
				require.NoError(t, s.DeleteObject(ctx, id, time.Time{}))
			}
			runTombstoneCleanups(t, s)
			require.True(t, runHarvest(s))
			if tt.flushAfterDelete {
				reproFlushObjects(t, s)
			}

			fresh := make([]reproObj, n)
			for i := range fresh {
				vec := randVec()
				id := reusePutObject(t, s, vec)
				fresh[i] = reproObj{id: id, vec: vec, docID: reuseDocIDOf(t, s, id)}
				require.Contains(t, oldDocIDs, fresh[i].docID, "new object %d must reuse a freed docID", i)
			}

			before := reproMeasureLoss(t, s, fresh)
			require.Equal(t, reproLoss{}, before, "control: every reused object resolvable before the merge")

			reproFlushObjects(t, s)
			if reproObjectsBucketSegments(s) > 1 {
				reproCompactToOneSegment(t, s)
			}

			after := reproMeasureLoss(t, s, fresh)
			t.Logf("after merge into one segment: %+v", after)
			assert.Zero(t, after.secondaryNil+after.secondaryWrong,
				"%d/%d live objects on reused docIDs unresolvable via the docID secondary index", after.secondaryNil+after.secondaryWrong, n)
			assert.Zero(t, after.byDocIDMissing,
				"%d/%d live objects on reused docIDs dropped by ObjectsByDocID", after.byDocIDMissing, n)
			assert.Zero(t, after.searchMissing,
				"%d/%d live objects on reused docIDs not returned by nearVector for their own vector", after.searchMissing, n)
			assert.Zero(t, after.rowCheckDefeats,
				"%d live docIDs pass assertNoTraces' object-row check", after.rowCheckDefeats)
		})
	}
}
