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

// objWithIDText builds a test object with a caller-controlled UUID so a later
// PutObject with the same ID is an update (delete-old + add-new), not a fresh
// insert.
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

// retokenizeScenario declares one concurrent-write-during-retokenize journey
// for weaviate/0-weaviate-issues#298. run() drives the shared lifecycle —
// import a 25-phrase corpus, start the migration, inject one write in the
// reindexed-but-not-swapped window, swap — and returns the post-swap bucket
// fingerprint (term → docIDs). Each test supplies only its tokenization
// direction and its assertions; the scaffold is here so the four journeys
// don't repeat it.
type retokenizeScenario struct {
	// sourceTok overrides the corpus tokenization; "" leaves the class default
	// (WORD). Set to FIELD for the reverse (FIELD→WORD) direction.
	sourceTok string
	// targetTok is the migration's target tokenization.
	targetTok string
	// filterable drives FilterableRetokenizeStrategy (RoaringSet bucket)
	// instead of the searchable strategy.
	filterable bool
	// preImportText, if set, imports one tracked object (preImportID) BEFORE
	// the migration so the iterator reindexes it — used by the update journey.
	preImportID   string
	preImportText string
	// midText is the value written during the FINALIZING window. midID == ""
	// makes it a fresh insert; a non-empty midID (matching preImportID) makes
	// it an update of the tracked object.
	midID   string
	midText string
}

func (sc retokenizeScenario) run(t *testing.T) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25

	className := "Retok_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})
	if sc.sourceTok != "" {
		class.Properties[0].Tokenization = sc.sourceTok
	}

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	// Cleanup (not defer): the shard must outlive the caller's assertions.
	t.Cleanup(func() { shard.Shutdown(ctx) })

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	if sc.preImportText != "" {
		require.NoError(t, shard.PutObject(ctx,
			objWithIDText(className, sc.preImportID, sc.preImportText)))
	}

	var task *ShardReindexTaskGeneric
	var bucketName string
	if sc.filterable {
		bucketName = helpers.BucketFromPropNameLSM(propName)
		task, _ = newFilterableRetokenizeTask(t, idx, className, propName, sc.targetTok)
	} else {
		bucketName = helpers.BucketSearchableFromPropNameLSM(propName)
		preStrategy := shard.store.Bucket(bucketName).Strategy()
		task, _ = newSearchableRetokenizeTask(t, idx, className, propName, sc.targetTok, preStrategy)
	}

	// Drive to reindexed-but-not-swapped: iterator done, double-write live.
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	if sc.midID != "" {
		require.NoError(t, shard.PutObject(ctx, objWithIDText(className, sc.midID, sc.midText)))
	} else {
		require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, sc.midText)))
	}

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	if sc.filterable {
		return fingerprintRoaringSetBucket(t, shard.store.Bucket(bucketName))
	}
	return fingerprintInvertedBucket(t, shard.store.Bucket(bucketName))
}

// countPhraseTerms is the positive control for a FIELD target: the
// iterator-reindexed corpus must collapse each 3-word phrase to one
// space-bearing term. Zero means the migration produced nothing and the
// concurrent-write assertions below would prove nothing.
func countPhraseTerms(fp map[string][]uint64) int {
	n := 0
	for term := range fp {
		if strings.Contains(term, " ") {
			n++
		}
	}
	return n
}

// TestReindex_ConcurrentWriteDuringRetokenize_IsTargetTokenized pins the core
// weaviate/0-weaviate-issues#298 defect for the searchable WORD→FIELD case: a
// write in the reindexed-but-not-swapped window survives the swap but keeps
// SOURCE tokenization instead of TARGET.
func TestReindex_ConcurrentWriteDuringRetokenize_IsTargetTokenized(t *testing.T) {
	const midText = "midalpha midbeta"
	fp := retokenizeScenario{targetTok: models.PropertyTokenizationField, midText: midText}.run(t)

	require.Positivef(t, countPhraseTerms(fp),
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
// FIELD→WORD mirror: the concurrent write keeps its SOURCE whole-value term, so
// a WORD single-word query misses it after the flip.
func TestReindex_ConcurrentWriteDuringRetokenize_ReverseFieldToWord(t *testing.T) {
	const midText = "revalpha revbeta"
	fp := retokenizeScenario{
		sourceTok: models.PropertyTokenizationField,
		targetTok: models.PropertyTokenizationWord,
		midText:   midText,
	}.run(t)

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

// TestReindex_ConcurrentWriteDuringRetokenize_Filterable pins the same
// wrong-tokenization defect for FilterableRetokenizeStrategy — it is not
// searchable-specific.
func TestReindex_ConcurrentWriteDuringRetokenize_Filterable(t *testing.T) {
	const midText = "filtalpha filtbeta"
	fp := retokenizeScenario{
		targetTok:  models.PropertyTokenizationField,
		filterable: true,
		midText:    midText,
	}.run(t)

	require.Positivef(t, countPhraseTerms(fp),
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

// TestReindex_UpdateDuringRetokenize_GhostAndLostValue pins the update variant:
// an object updated mid-migration leaves a stale OLD-value ghost (the delete
// double-write cannot remove the iterator's FIELD phrase term) and loses its
// NEW value under the target tokenization.
func TestReindex_UpdateDuringRetokenize_GhostAndLostValue(t *testing.T) {
	const oldValue = "ghostalpha ghostbeta"
	const newValue = "ghostgamma ghostdelta"
	xID := uuid.NewString()
	fp := retokenizeScenario{
		targetTok: models.PropertyTokenizationField,
		// Import X before the migration so the iterator reindexes its OLD value,
		// then update it (same ID) mid-window.
		preImportID:   xID,
		preImportText: oldValue,
		midID:         xID,
		midText:       newValue,
	}.run(t)

	newIDs, newOK := fp[newValue]
	assert.Truef(t, newOK && len(newIDs) > 0,
		"#298 update: object's CURRENT value %q is NOT under the target FIELD term — a search for its "+
			"actual value misses it after the flip", newValue)

	oldIDs, oldOK := fp[oldValue]
	assert.Falsef(t, oldOK && len(oldIDs) > 0,
		"#298 update: object still matches its OLD (overwritten) value %q (stale ghost ids=%v) — the delete "+
			"double-write could not remove the iterator's FIELD phrase term", oldValue, oldIDs)
}
