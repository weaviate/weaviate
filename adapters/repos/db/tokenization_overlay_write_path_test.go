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

// TestTokenizationOverlay_WritePath_IgnoresOverlay pins a candidate
// root-cause hypothesis for weaviate/0-weaviate-issues#240 Symptom B
// (per-replica divergence on change-tokenization migrations).
//
// Hypothesis: during the SWAPPING window of a change-tokenization
// migration, every node has its tokenizationOverlay set to the
// TARGET tokenization (via maybeSetTokenizationOverlayPreSwap) but
// the CLUSTER-WIDE schema flip from OnTaskCompleted has not yet
// landed locally. In this window:
//
//   - Live schema as seen by the analyzer: SOURCE tokenization
//     (e.g. "word")
//   - Canonical bucket on disk: TARGET-tokenized data (e.g. "field"),
//     courtesy of the local per-shard swap
//   - Tokenization overlay: TARGET ("field") — QUERY path is wired
//     to consult this (BM25Searcher.effectiveTokenization), so
//     reads against the bucket use TARGET tokenization
//
// On the WRITE path (Shard.PutObject → Shard.AnalyzeObject at
// shard_write_inverted.go:100) the analyzer is constructed bare:
//
//	inverted.NewAnalyzer(s.isFallbackToSearchable, object.Class().String())
//
// with NO WithSchemaOverlay call. So an incoming PUT in the SWAPPING
// window gets tokenized against the LIVE schema (SOURCE = "word")
// and the resulting per-term postings land in the CANONICAL bucket
// which is now TARGET-tokenized. Per-replica timing of how long this
// window lasts (RAFT propagation latency + scheduler tick variance)
// determines how many writes are mis-tokenized on each replica → the
// post-FINISHED inverted bucket diverges across replicas → BM25
// queries get different counts on different nodes.
//
// Independent of #240 this is a smaller-scope correctness bug:
// migration backfill uses AnalyzeObjectForMigrationWithOverlay to
// honour the overlay (see shard_write_inverted.go:125-143 godoc),
// but LIVE writes during the same overlay-active window do not. The
// two paths should be symmetric.
//
// This test pins the bug by checking the actual on-disk term keys
// after a PUT with the overlay set: with a working write path the
// keys reflect the TARGET tokenization; with the current broken
// write path they reflect the SOURCE tokenization.
//
// If the assertion fails (TARGET-tokenized keys missing, SOURCE
// keys present) → bug reproduced → confirms Theory 1 from the #240
// investigation. Once the fix lands (analyzer wired to consult the
// overlay on the write path), this test will start passing without
// the assertion message needing to change.
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
	// words"), the write path is honouring the LIVE schema's word
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
			"Production behaviour: writes during SWAPPING window get tokenized "+
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
