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

// TestReindex_ConcurrentWriteDuringRetokenize_IsTargetTokenized answers the
// concurrent-write convergence question from
// weaviate/0-weaviate-issues#298 for a WORD→FIELD SearchableRetokenize.
//
// RED on this commit — and NOT the data-loss failure #298 predicted.
//
// An object written while the migration is reindexed-but-not-swapped DOES
// reach the post-swap bucket (no data loss), but it is indexed with the
// SOURCE (WORD) tokenization instead of the TARGET (FIELD) tokenization the
// migration exists to produce. Once the migration finishes and the schema
// flips to FIELD, that object is queryable only under its WORD terms, never
// under the FIELD whole-phrase term → silent wrong results for any write
// that overlaps a retokenize migration.
//
// Root cause (traced in code, not inferred from the number):
// SearchableRetokenizeStrategy.MakeAddCallback re-tokenizes to the target
// ONLY when property.RawValues is populated, else it falls back to the
// source-tokenized property.Items. RawValues is captured solely by
// Shard.AnalyzeObjectForMigrationWithOverlay (the iterator's backfill scan);
// the normal write path (PutObject → AnalyzeObject) never populates it, so
// the double-write of a concurrent object silently uses the old tokenization.
// FilterableRetokenizeStrategy carries the identical RawValues guard.
//
// Built-in positive control: the iterator-reindexed corpus IS correctly
// FIELD-tokenized in the same bucket (multi-word phrase terms), proving the
// target tokenization path works when RawValues is present. So the
// concurrent write's WORD form is specifically the double-write path
// failing, not a harness artifact or a broken swap.
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

	// Existing corpus: 3-word phrases from the 25-token dictionary. Under the
	// FIELD target each phrase collapses to a single whole-value term; the
	// tokens do not collide with the unique mid-window tokens below.
	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preStrategy := shard.store.Bucket(searchBucketName).Strategy()

	task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationField, preStrategy)

	// Drive to reindexed-but-not-swapped: iterator done, double-write live.
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	// Concurrent write in the FINALIZING window. Under the FIELD target these
	// two words must collapse to the single term "midalpha midbeta"; under
	// the source WORD tokenization they split into "midalpha" / "midbeta".
	const midText = "midalpha midbeta"
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, midText)))

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	fp := fingerprintInvertedBucket(t, shard.store.Bucket(searchBucketName))

	// Positive control: the reindexed corpus is FIELD-tokenized (phrase terms
	// carry a space). If this is zero the premise is off and the assertions
	// below prove nothing.
	phraseTerms := 0
	for term := range fp {
		if strings.Contains(term, " ") {
			phraseTerms++
		}
	}
	require.Positivef(t, phraseTerms,
		"positive control: iterator-reindexed corpus must be FIELD-tokenized (phrase terms); got none")

	// The concurrent write must be TARGET (FIELD) tokenized: present under
	// the whole-value term, absent as separate WORD terms.
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
// FIELD→WORD mirror — the direction the field↔word reindex soak actually
// exercises. Same defect, and the more impactful shape: the concurrent write
// keeps its SOURCE (FIELD) whole-value term, so after the flip a WORD
// single-word query (the common case) misses it entirely.
//
// Confirms the "by symmetry" claim in the WORD→FIELD report empirically, so
// the team can weigh whether this mechanism contributes to soak short-counts
// (a concurrent writer overlapping the migration would leave objects
// unqueryable under single-word terms).
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

	// Concurrent write in the window. Under the WORD target it must split into
	// "revalpha" / "revbeta"; under the source FIELD it stays one whole term.
	const midText = "revalpha revbeta"
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, midText)))

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	fp := fingerprintInvertedBucket(t, shard.store.Bucket(searchBucketName))

	// Positive control: the iterator-reindexed corpus is WORD-tokenized
	// (single-word dictionary tokens present, no spaces).
	_, dictWord := fp["victor"]
	require.Truef(t, dictWord,
		"positive control: iterator-reindexed corpus must be WORD-tokenized (dictionary token 'victor' present); "+
			"got terms without it — premise is off")

	// The concurrent write must be TARGET (WORD) tokenized: present under the
	// split single-word terms, absent under the whole-value FIELD term.
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

// TestReindex_ConcurrentWriteDuringRetokenize_Filterable proves the defect is
// not searchable-specific: FilterableRetokenizeStrategy carries the identical
// `RawValues > 0` guard in its own Add callback, so a concurrent write during
// a WORD→FIELD filterable retokenize lands SOURCE(WORD)-tokenized in the
// TARGET (RoaringSet) filterable bucket — a `where` filter on the field value
// then misses it after the flip.
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

	// Positive control: iterator-reindexed corpus is FIELD-tokenized (phrase terms).
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

// TestReindex_UpdateDuringRetokenize_GhostAndLostValue pins the update variant,
// which is strictly worse than the insert case. An object reindexed by the
// iterator (so its OLD value is a FIELD phrase term in the ingest bucket) is
// then UPDATED mid-window. The delete double-write callback re-tokenizes to
// target only when RawValues is populated (it never is on the live path), so
// it tries to delete the OLD value's SOURCE(WORD) terms — which are not in the
// target bucket — leaving the iterator's FIELD phrase term for the OLD value
// behind. The add callback writes the NEW value SOURCE(WORD)-tokenized. Net
// post-swap state:
//   - the object still matches its OLD (overwritten) value  → stale ghost
//   - the object does NOT match its NEW (current) value      → current value lost
//
// Positive control: the iterator DID produce the OLD value's FIELD phrase term
// (that is the ghost we observe), proving the target-tokenization path works
// and the failure is specifically the update double-write.
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

	// Current value must be queryable under the target FIELD phrase term.
	newIDs, newOK := fp[newValue]
	assert.Truef(t, newOK && len(newIDs) > 0,
		"#298 update: object's CURRENT value %q is NOT under the target FIELD term — a search for its "+
			"actual value misses it after the flip", newValue)

	// Old value must be gone — the object was updated away from it.
	oldIDs, oldOK := fp[oldValue]
	assert.Falsef(t, oldOK && len(oldIDs) > 0,
		"#298 update: object still matches its OLD (overwritten) value %q (stale ghost ids=%v) — the delete "+
			"double-write could not remove the iterator's FIELD phrase term", oldValue, oldIDs)
}
