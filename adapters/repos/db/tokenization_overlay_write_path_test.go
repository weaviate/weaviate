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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins AnalyzeObject tokenizing writes against the overlay during the
// SWAPPING window, matching the query path.
func TestTokenizationOverlay_WritePath_HonorsOverlay(t *testing.T) {
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

	// Simulate the SWAPPING window: production sets this per prop via the
	// onPropSwapped hook while the schema's tokenization is still SOURCE.
	shard.SetPropertyOverlay(propName, inverted.PropertyOverlay{
		Tokenization: models.PropertyTokenizationField,
	})

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

	// Bucket is mapcollection-strategy on a non-blockmax class.
	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	bucket := shard.store.Bucket(bucketName)
	require.NotNilf(t, bucket, "searchable bucket %q must exist", bucketName)

	terms := readMapBucketTerms(t, ctx, bucket)
	sort.Strings(terms)
	t.Logf("on-disk terms with overlay=field, live schema=word: %v", terms)

	// Pin: the write path must honor the overlay.
	expectedFieldTerms := []string{"two distinct words"}
	assert.ElementsMatchf(t, expectedFieldTerms, terms,
		"write path did not analyze against the overlay. A write in the "+
			"SWAPPING window must use the overlay tokenization (field), not the "+
			"live schema tokenization (word), or a replica writes old-tokenized "+
			"terms into the new-tokenized bucket and replicas diverge. "+
			"Expected terms: %v; got: %v",
		expectedFieldTerms, terms)
}

// readMapBucketTerms returns every term-key from a mapcollection bucket;
// the term set alone is what discriminates word- from field-tokenized input.
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
