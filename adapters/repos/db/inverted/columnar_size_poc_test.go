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

package inverted

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/columnar"
)

// TestColumnarIndexPOC_ResidentSize reports the process-lifetime heap footprint
// of the built index for the numeric (fixed-width) and text (variable-length)
// corpora, so we can reason about memory cost per key.
func TestColumnarIndexPOC_ResidentSize(t *testing.T) {
	const n = benchCorpusSize

	numeric := newNumericFixture(t, n)
	numIdx, err := columnar.BuildFromBucket(numeric.bucket, uint64(numeric.numDocs+1), columnarTestLogger())
	require.NoError(t, err)

	textF := newContainsFixture(t, n)
	textBucket := textF.store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	textIdx, err := columnar.BuildFromBucket(textBucket, uint64(textF.numDocs+1), columnarTestLogger())
	require.NoError(t, err)

	report := func(name string, idx *columnar.ColumnarIndex) {
		info := idx.Info()
		t.Logf("%s: keys=%d keyWidth=%d prefix=%d docWidth=%d resident=%.2f MB (%.1f bytes/key)",
			name, info.Keys, info.KeyWidth, info.KeyPrefix, info.DocIDWidth,
			float64(info.SizeBytes)/1024/1024, float64(info.SizeBytes)/float64(info.Keys))
	}
	report("numeric(fixed)", numIdx)
	report("text(blob)", textIdx)
}
