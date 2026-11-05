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

package searchparams

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestGetPropertyByName_DotNotation pins that a property is matched by the
// first dot-notation segment, so a nested reference resolves to its root
// property.
func TestGetPropertyByName_DotNotation(t *testing.T) {
	class := &models.Class{
		Class: "Test",
		Properties: []*models.Property{
			{Name: "plain"},
			{Name: "nested"},
		},
	}

	tests := []struct {
		propName string
		want     string
		wantErr  bool
	}{
		{propName: "plain", want: "plain"},
		{propName: "nested", want: "nested"},
		{propName: "nested.inner", want: "nested"},
		{propName: "nested.inner.deep", want: "nested"},
		{propName: "missing", wantErr: true},
		{propName: "missing.inner", wantErr: true},
		{propName: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.propName, func(t *testing.T) {
			got, err := GetPropertyByName(class, tt.propName)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Name)
		})
	}
}

func boolPtr(v bool) *bool { return &v }

func TestExpandKeywordProperties(t *testing.T) {
	text := func(name string, searchable *bool) *models.Property {
		return &models.Property{
			Name:            name,
			DataType:        schema.DataTypeText.PropString(),
			IndexSearchable: searchable,
		}
	}
	textArray := func(name string, searchable *bool) *models.Property {
		return &models.Property{
			Name:            name,
			DataType:        schema.DataTypeTextArray.PropString(),
			IndexSearchable: searchable,
		}
	}

	tests := []struct {
		name            string
		class           *models.Class
		want            []string
		wantErr         string
		wantSentinel    bool
		wantNoExplicit  bool
		wantExplicitFix bool
	}{
		{
			name:    "nil class",
			wantErr: "class not found",
		},
		{
			name: "nil indexSearchable on text defaults to searchable",
			class: &models.Class{Class: "Articles", Properties: []*models.Property{
				text("title", nil),
				text("body", boolPtr(true)),
				{Name: "year", DataType: schema.DataTypeInt.PropString()},
			}},
			want: []string{"title", "body"},
		},
		{
			name: "mixed searchable and disabled text is an error",
			class: &models.Class{Class: "TestCollection", Properties: []*models.Property{
				text("text", boolPtr(false)),
				text("title", nil),
			}},
			wantErr:         `indexSearchable is disabled on "text"`,
			wantSentinel:    true,
			wantExplicitFix: true,
		},
		{
			name: "disabled text array is named even when another text property is searchable",
			class: &models.Class{Class: "Notes", Properties: []*models.Property{
				text("title", boolPtr(true)),
				textArray("tags", boolPtr(false)),
			}},
			wantErr:      `indexSearchable is disabled on "tags"`,
			wantSentinel: true,
		},
		{
			name: "several disabled properties are all named",
			class: &models.Class{Class: "Notes", Properties: []*models.Property{
				text("title", boolPtr(true)),
				text("text", boolPtr(false)),
				textArray("tags", boolPtr(false)),
			}},
			wantErr:      `indexSearchable is disabled on "text" and "tags"`,
			wantSentinel: true,
		},
		{
			name: "every text property disabled",
			class: &models.Class{Class: "Ledger", Properties: []*models.Property{
				{Name: "year", DataType: schema.DataTypeInt.PropString()},
				text("code", boolPtr(false)),
			}},
			wantErr: `collection "Ledger" has no searchable properties`,
			// Naming properties cannot help: every text property is disabled.
			wantSentinel:   true,
			wantNoExplicit: true,
		},
		{
			name: "no text properties",
			class: &models.Class{Class: "Counts", Properties: []*models.Property{
				{Name: "year", DataType: schema.DataTypeInt.PropString(), IndexSearchable: boolPtr(false)},
			}},
			wantErr:      `collection "Counts" has no searchable properties`,
			wantSentinel: true,
		},
		{
			name: "an int with indexSearchable false does not block a text search",
			class: &models.Class{Class: "Books", Properties: []*models.Property{
				text("title", nil),
				{Name: "year", DataType: schema.DataTypeInt.PropString(), IndexSearchable: boolPtr(false)},
			}},
			want: []string{"title"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExpandKeywordProperties(tt.class)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				if tt.wantSentinel {
					assert.Contains(t, err.Error(), "indexSearchable")
					var missing inverted.MissingIndexError
					require.ErrorAs(t, err, &missing)
				}
				if tt.wantNoExplicit {
					assert.Contains(t, err.Error(), "Enable indexSearchable on a text property")
					assert.NotContains(t, err.Error(), "search properties")
				}
				if tt.wantExplicitFix {
					assert.Contains(t, err.Error(), "set the search properties")
				}
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
