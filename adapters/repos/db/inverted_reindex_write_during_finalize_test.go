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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// objWithIDText builds a test object with a caller-controlled UUID, so a
// later PutObject with the same ID is an update, not a fresh insert.
func objWithIDText(className, id, text string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(id),
			Class:      className,
			Properties: map[string]interface{}{"title": text},
		},
	}
}

// TestReindex_ConcurrentWriteDuringRetokenize_IsTargetTokenized pins
// weaviate/0-weaviate-issues#298: a write racing a WORD→FIELD retokenize
// survives the swap but keeps SOURCE tokenization instead of TARGET.
func TestReindex_ConcurrentWriteDuringRetokenize_IsTargetTokenized(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25

	className := "ConcurrentRetok_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	// Corpus is 3-word phrases; under FIELD they collapse to one term each
	// and don't collide with the mid-window text below.
	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preStrategy := shard.store.Bucket(searchBucketName).Strategy()

	task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationField, preStrategy)

	// Drive to reindexed-but-not-swapped: iterator done, double-write live.
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	// Concurrent write in the FINALIZING window; FIELD collapses it to one
	// term, WORD splits it in two.
	const midText = "midalpha midbeta"
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, midText)))

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	fp := fingerprintInvertedBucket(t, shard.store.Bucket(searchBucketName))

	phraseTerms := 0
	for term := range fp {
		if strings.Contains(term, " ") {
			phraseTerms++
		}
	}
	require.Positivef(t, phraseTerms,
		"positive control: iterator-reindexed corpus must be FIELD-tokenized (phrase terms); got none")

	fieldIDs, fieldOK := fp[midText] // FIELD tokenization = whole value, lowercased
	assert.Truef(t, fieldOK && len(fieldIDs) > 0,
		"#298 wrong-tokenization: object written during the retokenize window is NOT under the "+
			"target FIELD term %q — it survived the swap but was SOURCE(WORD)-tokenized, so it is "+
			"unqueryable under the migration's target tokenization", midText)

	_, wordAlpha := fp["midalpha"]
	_, wordBeta := fp["midbeta"]
	assert.Falsef(t, wordAlpha || wordBeta,
		"#298 wrong-tokenization: concurrent write is indexed under SOURCE(WORD) terms "+
			"(midalpha=%v, midbeta=%v) in a FIELD-target bucket", wordAlpha, wordBeta)
}

// TestReindex_ConcurrentWriteDuringRetokenize_ReverseFieldToWord is the
// FIELD→WORD mirror of weaviate/0-weaviate-issues#298: the concurrent write
// keeps its SOURCE term, so a WORD query misses it after the flip.
func TestReindex_ConcurrentWriteDuringRetokenize_ReverseFieldToWord(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25

	className := "ConcurrentRetokRev_" + uuid.NewString()[:8]
	// FIELD-source class (the shared helper hardcodes WORD).
	class := newTestClassWithProps(className, []string{propName})
	class.Properties[0].Tokenization = models.PropertyTokenizationField

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	// Under FIELD source each 3-word phrase is a single whole-value term.
	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preStrategy := shard.store.Bucket(searchBucketName).Strategy()

	task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationWord, preStrategy)

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	// Concurrent write; WORD target splits it, FIELD source keeps it whole.
	const midText = "revalpha revbeta"
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, midText)))

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	fp := fingerprintInvertedBucket(t, shard.store.Bucket(searchBucketName))

	_, dictWord := fp["victor"]
	require.Truef(t, dictWord,
		"positive control: iterator-reindexed corpus must be WORD-tokenized (dictionary token 'victor' present); "+
			"got terms without it — premise is off")

	_, wordAlpha := fp["revalpha"]
	_, wordBeta := fp["revbeta"]
	assert.Truef(t, wordAlpha && wordBeta,
		"#298 wrong-tokenization (FIELD→WORD): concurrent write is NOT under the target WORD terms "+
			"revalpha/revbeta — a single-word query misses it (revalpha=%v, revbeta=%v)", wordAlpha, wordBeta)

	fieldIDs, fieldTerm := fp[midText] // FIELD whole-value term
	assert.Falsef(t, fieldTerm && len(fieldIDs) > 0,
		"#298 wrong-tokenization (FIELD→WORD): concurrent write kept its SOURCE(FIELD) whole-value term %q "+
			"in a WORD-target bucket", midText)
}

// TestReindex_ConcurrentWriteDuringRetokenize_Filterable pins
// weaviate/0-weaviate-issues#298 for FilterableRetokenizeStrategy: the same
// wrong-tokenization defect, not searchable-specific.
func TestReindex_ConcurrentWriteDuringRetokenize_Filterable(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25

	className := "ConcurrentRetokFilt_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	filtBucketName := helpers.BucketFromPropNameLSM(propName)

	task, _ := newFilterableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationField)

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	const midText = "filtalpha filtbeta"
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, midText)))

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	fp := fingerprintRoaringSetBucket(t, shard.store.Bucket(filtBucketName))

	phraseTerms := 0
	for term := range fp {
		if strings.Contains(term, " ") {
			phraseTerms++
		}
	}
	require.Positivef(t, phraseTerms,
		"positive control: iterator-reindexed filterable corpus must be FIELD-tokenized (phrase terms); got none")

	fieldIDs, fieldOK := fp[midText]
	assert.Truef(t, fieldOK && len(fieldIDs) > 0,
		"#298 wrong-tokenization (filterable): concurrent write is NOT under the target FIELD term %q — "+
			"a `where` filter on the field value misses it after the flip", midText)

	_, wordAlpha := fp["filtalpha"]
	_, wordBeta := fp["filtbeta"]
	assert.Falsef(t, wordAlpha || wordBeta,
		"#298 wrong-tokenization (filterable): concurrent write is under SOURCE(WORD) terms "+
			"(filtalpha=%v, filtbeta=%v) in a FIELD-target filterable bucket", wordAlpha, wordBeta)
}

// TestReindex_UpdateDuringRetokenize_GhostAndLostValue pins
// weaviate/0-weaviate-issues#298's update variant: an object updated
// mid-migration leaves a stale OLD-value ghost and loses its NEW value.
func TestReindex_UpdateDuringRetokenize_GhostAndLostValue(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25
	const oldValue = "ghostalpha ghostbeta"
	const newValue = "ghostgamma ghostdelta"

	className := "UpdateRetok_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	// Tracked object X imported BEFORE the migration so the iterator reindexes
	// its OLD value to the FIELD phrase term.
	xID := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, objWithIDText(className, xID, oldValue)))

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preStrategy := shard.store.Bucket(searchBucketName).Strategy()
	task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationField, preStrategy)

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	// Mid-window UPDATE of X (same ID) → delete-old + add-new double-write.
	require.NoError(t, shard.PutObject(ctx, objWithIDText(className, xID, newValue)))

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	fp := fingerprintInvertedBucket(t, shard.store.Bucket(searchBucketName))

	newIDs, newOK := fp[newValue]
	assert.Truef(t, newOK && len(newIDs) > 0,
		"#298 update: object's CURRENT value %q is NOT under the target FIELD term — a search for its "+
			"actual value misses it after the flip", newValue)

	oldIDs, oldOK := fp[oldValue]
	assert.Falsef(t, oldOK && len(oldIDs) > 0,
		"#298 update: object still matches its OLD (overwritten) value %q (stale ghost ids=%v) — the delete "+
			"double-write could not remove the iterator's FIELD phrase term", oldValue, oldIDs)
}
