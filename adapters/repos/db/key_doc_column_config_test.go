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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestReportUnmatchedKeyDocColumns pins that a configuration naming something
// the schema does not have is said out loud.
//
// Every one of these leaves a node that starts cleanly and serves ContainsAny at
// fold speed forever, with the only evidence being a debug line that was never
// written — so a typo is indistinguishable from a feature that is simply not
// helping.
func TestReportUnmatchedKeyDocColumns(t *testing.T) {
	filterable := true
	classes := []*models.Class{{
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
				// the default for text, which turns one value into any number of
				// keys — so the query path never routes it to a column
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
	}, {
		// every property ineligible, so the wildcard over it builds nothing
		Class: "Gazette",
		Properties: []*models.Property{{
			Name:            "headline",
			DataType:        schema.DataTypeText.PropString(),
			IndexFilterable: func() *bool { b := false; return &b }(),
		}},
	}}

	tests := []struct {
		name       string
		configured map[string][]string
		want       string
	}{
		{name: "nothing configured"},
		{
			name:       "a property that gets one",
			configured: map[string][]string{"Article": {"sku"}},
		},
		{
			// the shape MF7 was about: configured, built, and never read,
			// because the query path declines every tokenization but field
			name:       "a text property the query path would not route",
			configured: map[string][]string{"Article": {"title"}},
			want:       "tokenization",
		},
		{
			name:       "every property of a collection that exists",
			configured: map[string][]string{"Article": {config.AllProperties}},
		},
		{
			name:       "a collection the schema does not have",
			configured: map[string][]string{"Periodical": {"title"}},
			want:       "does not have",
		},
		{
			name:       "a property the collection does not have",
			configured: map[string][]string{"Article": {"subtitle"}},
			want:       "no such property",
		},
		{
			name:       "a property with no filterable index",
			configured: map[string][]string{"Article": {"unindexed"}},
			want:       "no filterable index",
		},
		{
			name:       "a geo property",
			configured: map[string][]string{"Article": {"location"}},
			want:       "geo properties",
		},
		{
			// the schema stores "sku"; an operator who capitalised the first
			// letter gets the column, as a filter written that way gets the
			// property — only the first letter is canonicalised, so "SKU" would
			// be held as "sKU" and is a different property
			name:       "a property named as it was typed, not as the schema holds it",
			configured: map[string][]string{"Article": {"Sku"}},
		},
		{
			name:       "every property of a collection, none of them eligible",
			configured: map[string][]string{"Gazette": {config.AllProperties}},
			want:       "no property that gets a key/doc column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			reportUnmatchedKeyDocColumns(logger, tt.configured, classes)

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
