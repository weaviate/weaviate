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

package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// TestCreatePaths_StripClientSetSearchableBlockmax pins that a client-set
// SearchableBlockmax is nilled on every create path (only read-repair should
// seed it) — asserted on the property that reaches schemaManager, not the
// returned class.
func TestCreatePaths_StripClientSetSearchableBlockmax(t *testing.T) {
	ctx := context.Background()
	tr := true

	findProp := func(props []*models.Property, name string) *models.Property {
		for _, p := range props {
			if p.Name == name {
				return p
			}
		}
		return nil
	}

	t.Run("AddClass strips client-set stamp", func(t *testing.T) {
		handler, fake := newTestHandler(t, &fakeDB{})

		class := &models.Class{
			Class: "StripOnCreate",
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}, SearchableBlockmax: &tr},
				{Name: "body", DataType: []string{"text"}},
			},
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}

		var stored *models.Class
		fake.On("AddClass", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			stored = args.Get(0).(*models.Class)
		}).Return(nil)
		fake.On("QueryCollectionsCount", "").Return(0, nil)

		_, _, err := handler.AddClass(ctx, nil, class)
		require.NoError(t, err)

		require.NotNil(t, stored)
		require.Nil(t, findProp(stored.Properties, "title").SearchableBlockmax,
			"client-set searchableBlockmax must be nil in the stored class")
	})

	t.Run("AddClassProperty strips client-set stamp", func(t *testing.T) {
		handler, fake := newTestHandler(t, &fakeDB{})

		existing := &models.Class{
			Class:             "StripOnAddProp",
			Properties:        []*models.Property{{Name: "title", DataType: []string{"text"}}},
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}
		fake.On("ReadOnlyClass", existing.Class).Return(existing)

		var storedProps []*models.Property
		fake.On("AddProperty", existing.Class, mock.Anything).Run(func(args mock.Arguments) {
			storedProps = args.Get(1).([]*models.Property)
		}).Return(nil)

		newProp := &models.Property{Name: "body", DataType: []string{"text"}, SearchableBlockmax: &tr}
		_, _, err := handler.AddClassProperty(ctx, nil, existing.Class, false, newProp)
		require.NoError(t, err)

		require.Len(t, storedProps, 1)
		require.Nil(t, storedProps[0].SearchableBlockmax,
			"client-set searchableBlockmax must be nil in the stored property")
	})
}
