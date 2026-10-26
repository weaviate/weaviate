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

package hfresh

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestFlatSearchSkipsCandidatesDeletedMidQuery(t *testing.T) {
	n := 100
	vectors, _ := testinghelpers.RandomVecs(n, 1, 32)
	const deletedID = uint64(5)

	index := newSearchTestIndex(t, vectors, func(ctx context.Context, id uint64) error {
		if id == deletedID && ctx.Value(searchMarker{}) != nil {
			return storobj.NewErrNotFoundf(id, "deleted mid-query")
		}
		return nil
	})

	// a small allowList (< flatSearchCutoff) routes the search through
	// flatSearch; it must skip the deleted candidate, not fail the query
	allowIDs := make([]uint64, n)
	for i := range allowIDs {
		allowIDs[i] = uint64(i)
	}
	allowList := helpers.NewAllowList(allowIDs...)

	searchCtx := context.WithValue(t.Context(), searchMarker{}, true)
	ids, _, err := index.SearchByVector(searchCtx, vectors[deletedID], 10, allowList)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
	require.NotContains(t, ids, deletedID,
		"flatSearch must drop a candidate whose vector vanished mid-query")
}
