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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestReportKeyDocColumnConfig pins that a configured key/doc column which builds nothing is warned about, not silent.
func TestReportKeyDocColumnConfig(t *testing.T) {
	filterable := true
	article := &models.Class{
		Class: "Article",
		Properties: []*models.Property{
			{
				// one value, one key: the only text shape a column can answer
				Name:            "sku",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: &filterable,
			},
			{
				// word tokenization can turn one value into several keys, so the query path never reads a column
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &filterable,
			},
			{
				Name:            "unindexed",
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: func() *bool { b := false; return &b }(),
			},
			{
				Name:            "location",
				DataType:        schema.DataTypeGeoCoordinates.PropString(),
				IndexFilterable: &filterable,
			},
		},
	}
	// every property ineligible, so the wildcard over it builds nothing
	gazette := &models.Class{
		Class: "Gazette",
		Properties: []*models.Property{{
			Name:            "headline",
			DataType:        schema.DataTypeText.PropString(),
			IndexFilterable: func() *bool { b := false; return &b }(),
		}},
	}

	tests := []struct {
		name  string
		class *models.Class
		names []string
		want  string
	}{
		{name: "nothing configured", class: article},
		{name: "a property that gets one", class: article, names: []string{"sku"}},
		{
			name:  "a text property the query path would not route",
			class: article, names: []string{"title"},
			want: "tokenization",
		},
		{name: "every property of a collection", class: article, names: []string{config.AllProperties}},
		{
			name:  "a property the collection does not have",
			class: article, names: []string{"subtitle"},
			want: "a column is built if one is added",
		},
		{
			name:  "a property with no filterable index",
			class: article, names: []string{"unindexed"},
			want: "no filterable index",
		},
		{
			name:  "a geo property",
			class: article, names: []string{"location"},
			want: "geo properties",
		},
		{
			// only the first letter is canonicalised, so "Sku" reaches "sku" as a filter would, and "SKU" would not
			name:  "a property named as it was typed, not as the schema holds it",
			class: article, names: []string{"Sku"},
		},
		{
			name:  "every property of a collection, none of them eligible",
			class: gazette, names: []string{config.AllProperties},
			want: "no property that gets a key/doc column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			reportKeyDocColumnConfig(logger, tt.names, tt.class)

			if tt.want == "" {
				assert.Empty(t, hook.AllEntries(), "nothing to report")
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			assert.Equal(t, logrus.WarnLevel, entry.Level)
			assert.Contains(t, entry.Message, tt.want)
		})
	}
}

// AddClass reports every configured property the class lacks; AddProperty only an added ineligible one the configuration names.
func TestMigratorReportsKeyDocColumnConfig(t *testing.T) {
	const className = "Article"
	ctx := testCtx()
	repo, migrator, _, hook := newLazyLoadRepoWithConfig(t, singleShardState(), true, -1,
		func(cfg *Config) {
			cfg.IndexFilterableKeyDocColumn = map[string][]string{
				className: {"skew", "sku", "title"},
			}
		})
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	warnings := func() []*logrus.Entry {
		var out []*logrus.Entry
		for _, e := range hook.AllEntries() {
			if e.Data["action"] == "keydoccolumn_config" && e.Level == logrus.WarnLevel {
				out = append(out, e)
			}
		}
		return out
	}

	require.NoError(t, migrator.AddClass(ctx, newClassWithWarmProp(className)))
	got := warnings()
	var props []any
	for _, e := range got {
		props = append(props, e.Data["property"])
		assert.Contains(t, e.Message, "a column is built if one is added")
	}
	assert.ElementsMatch(t, []any{"skew", "sku", "title"}, props,
		"one line per configured property the class lacks")
	hook.Reset()

	field := textProp("sku", true)
	field.Tokenization = models.PropertyTokenizationField
	word := textProp("title", true)
	word.Tokenization = models.PropertyTokenizationWord
	unnamed := textProp("body", true)
	unnamed.Tokenization = models.PropertyTokenizationWord
	require.NoError(t, migrator.AddProperty(ctx, className, field, word, unnamed))

	got = warnings()
	require.Len(t, got, 1, "only the ineligible property is reported")
	assert.Equal(t, "title", got[0].Data["property"])
	assert.Contains(t, got[0].Message, "tokenization")
}
