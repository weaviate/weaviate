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
	"path/filepath"
	"strconv"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// TestGenerateQueryTermsAndStatsBoost: a malformed boost must error instead of
// silently scoring the property at zero.
func TestGenerateQueryTermsAndStatsBoost(t *testing.T) {
	tests := []struct {
		name        string
		property    string
		wantErr     error
		wantBoost   float32
		wantErrText string
	}{
		{
			name:      "plain property scores at the default boost",
			property:  "title",
			wantBoost: 1,
		},
		{
			name:      "valid boost is parsed",
			property:  "title^2",
			wantBoost: 2,
		},
		{
			name:        "non-numeric boost is rejected",
			property:    "title^abc",
			wantErr:     strconv.ErrSyntax,
			wantErrText: `parse boost of property "title^abc"`,
		},
		{
			name:        "out-of-range boost is rejected",
			property:    "title^1e309",
			wantErr:     strconv.ErrRange,
			wantErrText: `parse boost of property "title^1e309"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			searcher := newBoostTestSearcher(t)
			class := &models.Class{
				Class: "Movie",
				Properties: []*models.Property{{
					Name:         "title",
					DataType:     []string{"text"},
					Tokenization: models.PropertyTokenizationWord,
				}},
			}

			terms, _, pins, err := searcher.generateQueryTermsAndStats(context.Background(), class,
				searchparams.KeywordRanking{Query: "space", Properties: []string{test.property}})
			defer pins.release()

			if test.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, test.wantErr)
				require.Contains(t, err.Error(), test.wantErrText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantBoost, terms.propertyBoosts["title"])
		})
	}
}

func newBoostTestSearcher(t *testing.T) *BM25Searcher {
	t.Helper()
	logger := logrus.New()
	dirName := t.TempDir()

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	require.NoError(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1)))
	require.NoError(t, store.CreateOrLoadBucket(context.Background(),
		helpers.BucketSearchableFromPropNameLSM("title"),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))

	tracker, err := NewJsonShardMetaData(filepath.Join(dirName, "proplengths"), logger)
	require.NoError(t, err)
	t.Cleanup(func() { tracker.Close() })

	return &BM25Searcher{
		store:          store,
		propLenTracker: tracker,
		logger:         logger,
	}
}
