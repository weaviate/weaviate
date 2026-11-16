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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema"
)

// mergingSchema applies each migration's property update to class with the FSM's masked merge.
type mergingSchema struct {
	schema.SchemaReader
	schema.SchemaManager
	class   *models.Class
	updates int
}

func (m *mergingSchema) ReadOnlyClass(string) *models.Class { return m.class }

func (m *mergingSchema) WaitForUpdate(context.Context, uint64) error { return nil }

func (m *mergingSchema) UpdatePropertyFromMigration(_ context.Context, _ string, _ api.MigrationRun,
	prop *models.Property, fields ...string,
) (uint64, error) {
	m.updates++
	m.class.Properties = clusterschema.MergePropsMasked(m.class.Properties, []*models.Property{prop}, fields)
	return 0, nil
}

func TestSchemaChangeOfARunStampsItsVersion(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	searchable := func(c *models.Class) {
		c.Properties[0].IndexSearchable = boolPtr(true)
		c.Properties[0].SearchableBlockmax = boolPtr(true)
	}
	for _, tc := range []struct {
		mt ReindexMigrationType
		// atTarget puts the property at the run's target before the run, so only the stamp changes.
		atTarget func(*models.Class)
	}{
		{mt: ReindexTypeChangeTokenization, atTarget: toFieldTokenization},
		{mt: ReindexTypeChangeTokenizationFilterable, atTarget: toFieldTokenization},
		{mt: ReindexTypeEnableFilterable, atTarget: func(c *models.Class) { c.Properties[0].IndexFilterable = boolPtr(true) }},
		{mt: ReindexTypeEnableRangeable, atTarget: func(c *models.Class) { c.Properties[0].IndexRangeFilters = boolPtr(true) }},
		{mt: ReindexTypeEnableSearchable, atTarget: func(c *models.Class) { searchable(c); toFieldTokenization(c) }},
		{mt: ReindexTypeChangeAlgorithm, atTarget: searchable},
	} {
		for _, already := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/already at target %t", tc.mt, already), func(t *testing.T) {
				// body is searchable and not on blockmax, so change-algorithm leaves the class flag alone.
				class := &models.Class{
					Class: "Docs", InvertedIndexConfig: &models.InvertedIndexConfig{},
					Properties: []*models.Property{
						{
							Name: "title", DataType: []string{"text"}, Tokenization: models.PropertyTokenizationWord,
							IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false), IndexRangeFilters: boolPtr(false),
						},
						{Name: "body", DataType: []string{"text"}, IndexSearchable: boolPtr(true)},
					},
				}
				if already {
					tc.atTarget(class)
				}
				store := &mergingSchema{class: class}
				handler, err := schema.NewHandler(store, store, nil, logger, nil, nil, config.Config{},
					nil, nil, nil, nil, nil, nil, schema.Parser{}, nil, nil, nil)
				require.NoError(t, err)
				p := newClassificationProvider(class)
				p.schemaManager = &schema.Manager{SchemaReader: store, Handler: handler}
				payload := &ReindexTaskPayload{
					MigrationType: tc.mt, Collection: "Docs", Properties: []string{"title"},
					TargetTokenization: models.PropertyTokenizationField,
				}
				flip := func(version uint64) {
					require.NoError(t, p.flipSemanticMigrationSchema(ctx, api.MigrationRun{Version: version}, payload, logger))
				}

				flip(7)
				assert.Equal(t, int64(7), store.class.Properties[0].BucketGeneration)
				assert.Zero(t, store.class.Properties[1].BucketGeneration, "only the run's properties")
				updates := store.updates
				assert.Positive(t, updates)
				flip(7)
				flip(5)
				assert.Equal(t, int64(7), store.class.Properties[0].BucketGeneration, "an older run cannot lower it")
				assert.Equal(t, updates, store.updates, "a repeat sends nothing")
			})
		}
	}
}
