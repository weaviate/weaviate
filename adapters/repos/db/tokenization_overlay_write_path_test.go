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
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestTokenizationOverlay_WritePath_IgnoresOverlay pins a real write-path
// correctness bug discovered during the #240 investigation:
// Shard.AnalyzeObject (write-path analyzer) doesn't consult the
// tokenizationOverlay, even though the query-path analyzer does and the
// migration-backfill AnalyzeObjectForMigrationWithOverlay does. A PUT
// during the SWAPPING window therefore lands SOURCE-tokenized terms in
// a TARGET-tokenized bucket. Red on current main; turns green once
// AnalyzeObject is wired to consult the overlay symmetrically.
func TestTokenizationOverlay_WritePath_IgnoresOverlay(t *testing.T) {
	ctx := testCtx()
	className := "TokOverlayWrite_" + uuid.NewString()[:8]
	const propName = "text"

	// Class created with the SOURCE tokenization (word). The migration
	// would normally flip this to field at OnTaskCompleted; this test
	// stops just before that flip.
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: []*models.Property{
			{
				Name:         propName,
				DataType:     []string{"text"},
				Tokenization: models.PropertyTokenizationWord,
			},
		},
	}

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	// Simulate the SWAPPING window: set overlay to TARGET (field).
	// Production sets this in maybeSetTokenizationOverlayPreSwap; the
	// schema's prop.Tokenization stays at SOURCE (word) until the
	// cluster-wide flip lands.
	shard.SetTokenizationOverlay(propName, models.PropertyTokenizationField)

	// Issue a PUT during the overlay-active window. With field
	// tokenization the value "two distinct words" is ONE term; with
	// word tokenization it's THREE terms ("two", "distinct", "words").
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				propName: "two distinct words",
			},
		},
	}
	require.NoError(t, shard.PutObject(ctx, obj))

	// Inspect the searchable bucket to see what got written. The bucket
	// is mapcollection-strategy on a non-blockmax class.
	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	bucket := shard.store.Bucket(bucketName)
	require.NotNilf(t, bucket, "searchable bucket %q must exist", bucketName)

	// Collect all term keys. With WORD tokenization there will be
	// three keys ("two", "distinct", "words"). With FIELD tokenization
	// there will be one key ("two distinct words").
	terms := readMapBucketTerms(t, ctx, bucket)
	sort.Strings(terms)
	t.Logf("on-disk terms with overlay=field, live schema=word: %v", terms)

	// Pin the bug: the overlay says "use field tokenization" but the
	// write path ignores it. If the assertion BELOW fails (i.e. terms
	// contain "two", "distinct", "words" instead of "two distinct
	// words"), the write path is honoring the LIVE schema's word
	// tokenization, not the overlay — that's the #240 Symptom B
	// candidate root cause.
	//
	// We assert what the OVERLAY-RESPECTING write path WOULD produce.
	// The current (broken) code path produces the OPPOSITE, so this
	// assertion fails on `main`. Once Shard.AnalyzeObject is wired to
	// consult the overlay (analogous to AnalyzeObjectForMigrationWith-
	// Overlay), this test passes without modification.
	expectedFieldTerms := []string{"two distinct words"}
	assert.ElementsMatchf(t, expectedFieldTerms, terms,
		"write-path bug — overlay=field is being ignored. "+
			"Production behavior: writes during SWAPPING window get tokenized "+
			"against live (OLD) schema; the canonical bucket now NEW-tokenized "+
			"so the new-tokenized OLD-tokens land in the NEW bucket → per-replica "+
			"divergence (weaviate/0-weaviate-issues#240). "+
			"Expected (overlay-respected): %v; got (overlay-ignored, current): %v",
		expectedFieldTerms, terms)
}

// readMapBucketTerms returns every term-key from a mapcollection /
// inverted searchable bucket. MapCursor() works on both strategies.
// We don't decode the per-term docID payload here — the test asserts
// on the term-key SET only, which is what discriminates word vs
// field tokenization of the input.
func readMapBucketTerms(t *testing.T, ctx context.Context, b *lsmkv.Bucket) []string {
	t.Helper()
	c, err := b.MapCursor()
	require.NoError(t, err, "MapCursor on searchable bucket")
	defer c.Close()
	var out []string
	for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {
		out = append(out, string(append([]byte(nil), k...)))
	}
	return out
}
