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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
)

// Pins that the durable stamp lets the second of two siblings completing in one tick flip the class flag.
func TestShouldDeferBlockmaxFlip_StampBreaksWedge(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tr := true

	newProvider := func(cls *models.Class) *ReindexProvider {
		return &ReindexProvider{
			logger:        logger,
			schemaManager: &schema.Manager{SchemaReader: classReaderStub{class: cls}},
		}
	}

	classWith := func(siblingStamp *bool) *models.Class {
		return &models.Class{
			Class:               "Docs",
			InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: false}, // partial: never flipped
			Properties: []*models.Property{
				{Name: "propX", DataType: []string{"text"}, IndexSearchable: &tr, SearchableBlockmax: siblingStamp}, // sibling
				{Name: "propY", DataType: []string{"text"}, IndexSearchable: &tr},                                   // completing now
			},
		}
	}

	// propY is completing its change-algorithm cutover.
	completeY := &ReindexTaskPayload{
		MigrationType: ReindexTypeChangeAlgorithm, Collection: "Docs", Properties: []string{"propY"},
	}

	t.Run("sibling stamped blockmax → no spurious defer (wedge broken)", func(t *testing.T) {
		p := newProvider(classWith(&tr))
		deferFlip, err := p.shouldDeferBlockmaxFlip(completeY, logger)
		require.NoError(t, err)
		require.False(t, deferFlip, "a stamped sibling must not force a defer")
	})

	t.Run("sibling unstamped → defer (genuinely still WAND)", func(t *testing.T) {
		p := newProvider(classWith(nil))
		deferFlip, err := p.shouldDeferBlockmaxFlip(completeY, logger)
		require.NoError(t, err)
		require.True(t, deferFlip, "an unstamped sibling is genuinely WAND → defer")
	})
}
