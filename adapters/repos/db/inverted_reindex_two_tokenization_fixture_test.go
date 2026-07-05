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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/tokenizer"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// twoTokenizationFixture: one shard with a FIELD- and a WORD-tokenized
// searchable prop carrying identical content, so redirecting the FIELD
// prop's bucket pointer to the WORD bucket emulates the runtime field→word
// swap with both generations as distinct, identity-comparable buckets.
type twoTokenizationFixture struct {
	shard *Shard
	idx   *Index
	// captured pre-swap for identity assertions
	fieldBucket *lsmkv.Bucket
	wordBucket  *lsmkv.Bucket

	className string
	fieldProp string // FIELD-tokenized: pre-swap content
	wordProp  string // WORD-tokenized: post-swap content
	phrase    string // present verbatim in matchDocs docs, in BOTH props
	matchDocs int    // docs carrying phrase; validCount for consistent pairs
}

// setupTwoTokenizationShard writes numDocs docs of which matchDocs carry the
// phrase in BOTH props; keeping the phrase out of 100% of docs avoids BM25's
// over-frequent-term IDF collapse, so matched docs reliably score above zero.
func setupTwoTokenizationShard(t *testing.T, ctx context.Context, className string) *twoTokenizationFixture {
	t.Helper()
	const (
		fieldProp = "alpha"
		wordProp  = "beta"
		phrase    = "hello world"
		filler    = "lorem ipsum"
		numDocs   = 8
		matchDocs = 4
	)

	class := buildTwoTokenizationClass(className, fieldProp, wordProp)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { _ = shard.Shutdown(ctx) })

	// Inverted/BlockMax is the production default and the strategy a
	// field→word retokenization runs on.
	for _, p := range []string{fieldProp, wordProp} {
		require.Equal(t, lsmkv.StrategyInverted,
			shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(p)).Strategy(),
			"searchable bucket for %q must start at Inverted", p)
	}

	for i := 0; i < numDocs; i++ {
		text := phrase
		if i >= matchDocs {
			text = filler
		}
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					fieldProp: text,
					wordProp:  text,
				},
			},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	fieldBucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(fieldProp))
	wordBucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(wordProp))
	require.NotNil(t, fieldBucket)
	require.NotNil(t, wordBucket)
	require.NotSame(t, fieldBucket, wordBucket, "field and word buckets must be distinct objects")

	return &twoTokenizationFixture{
		shard:       shard,
		idx:         idx,
		fieldBucket: fieldBucket,
		wordBucket:  wordBucket,
		className:   className,
		fieldProp:   fieldProp,
		wordProp:    wordProp,
		phrase:      phrase,
		matchDocs:   matchDocs,
	}
}

func buildTwoTokenizationClass(className, fieldProp, wordProp string) *models.Class {
	vFalse := false
	vTrue := true
	mkProp := func(name, tok string) *models.Property {
		return &models.Property{
			Name:            name,
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    tok,
			IndexFilterable: &vFalse,
			IndexSearchable: &vTrue,
		}
	}
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Bm25:                   &models.BM25Config{K1: 1.2, B: 0.75},
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      true,
		},
		Properties: []*models.Property{
			mkProp(fieldProp, models.PropertyTokenizationField),
			mkProp(wordProp, models.PropertyTokenizationWord),
		},
	}
}

// lookupCount emulates a keyword query from a (tokenization, bucket) pair —
// a CONSISTENT pair finds the docs, a MIXED pair (the bug) misses → 0.
func lookupCount(ctx context.Context, tokenization string, bucket *lsmkv.Bucket, className, query string) int {
	if bucket == nil {
		return 0
	}
	terms := tokenizer.TokenizeForClass(tokenization, query, className)
	best := 0
	for _, term := range terms {
		dp, err := bucket.DocPointerWithScoreList(ctx, []byte(term), 1)
		if err != nil {
			return -1
		}
		if len(dp) > best {
			best = len(dp)
		}
	}
	return best
}
