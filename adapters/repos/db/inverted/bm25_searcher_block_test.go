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

package inverted

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCombineResults_PropagatesGetObjectsError verifies that combineResults
// surfaces an error from getObjectsAndScores to its caller instead of
// swallowing it and returning silent empty results. The store here has no
// objects bucket, so getObjectsAndScores fails to load the objects — that
// failure must reach the caller.
func TestCombineResults_PropagatesGetObjectsError(t *testing.T) {
	logger := logrus.New()
	dirName := t.TempDir()

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	defer store.Shutdown(context.Background())

	// Create the objects bucket but deliberately WITHOUT a class name, so the
	// object decode in getObjectsAndScores fails (ClassName() errors). This
	// exercises the load-error path rather than a nil-bucket path.
	require.Nil(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1)))
	require.NotNil(t, store.Bucket(helpers.ObjectsBucketLSM))

	b := &BM25Searcher{
		store:  store,
		logger: logger,
	}

	// One property, one segment, one candidate doc id — enough to make
	// getObjectsAndScores attempt (and fail) an objects lookup.
	allIds := [][][]uint64{{{1}}}
	allScores := [][][]float32{{{0.5}}}
	allExplanation := [][][][]*terms.DocPointerWithScore{{{{nil}}}}
	queryTerms := [][]string{{"journey"}}

	objects, scores, err := b.combineResults(allIds, allScores, allExplanation, queryTerms, additional.Properties{}, 10)
	require.Error(t, err, "combineResults must propagate the objects-loading error rather than swallow it")
	require.Nil(t, objects)
	require.Nil(t, scores)
}
