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

package rank

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestParamsValidate(t *testing.T) {
	class := &models.Class{
		Class: "Tickets",
		Properties: []*models.Property{
			{Name: "text", DataType: schema.DataTypeText.PropString()},
			{Name: "legacy", DataType: schema.DataTypeString.PropString()},
			{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
			{Name: "tags", DataType: schema.DataTypeTextArray.PropString()},
			{Name: "count", DataType: schema.DataTypeInt.PropString()},
			{Name: "day", DataType: schema.DataTypeDate.PropString()},
			{Name: "ref", DataType: schema.DataTypeUUID.PropString()},
			{Name: "photoHash", DataType: schema.DataTypeBlobHash.PropString()},
			{
				Name: "meta", DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{{Name: "isbn", DataType: schema.DataTypeText.PropString()}},
			},
			{Name: "owner", DataType: []string{"Users"}},
			{Name: "owners", DataType: []string{"Users", "Teams"}},
		},
	}
	query := "The customer is about to churn"
	blank := " \t\n"
	empty := ""
	tests := []struct {
		name     string
		property string
		query    *string
		wantErr  string
	}{
		{name: "a text property", property: "text", query: &query},
		{name: "a property of the deprecated string type", property: "legacy", query: &query},
		{name: "a blob property", property: "photo", query: &query},
		{name: "no query", property: "text", wantErr: "rerank: query is required"},
		{name: "an empty query", property: "text", query: &empty, wantErr: "rerank: query is required"},
		{name: "a query of whitespace", property: "text", query: &blank, wantErr: "rerank: query is required"},
		{
			name: "a property the class does not have", property: "nope", query: &query,
			wantErr: `rerank: no such prop with name 'nope' found in class 'Tickets' in the schema. Check your schema files for which properties in this class are available`,
		},
		{
			name: "no property", property: "", query: &query,
			wantErr: `rerank: no such prop with name '' found in class 'Tickets' in the schema. Check your schema files for which properties in this class are available`,
		},
		{
			name: "a text array", property: "tags", query: &query,
			wantErr: `rerank: property "tags" of collection "Tickets" is of type text[]; a rerank reads a text or blob property`,
		},
		{
			name: "an int", property: "count", query: &query,
			wantErr: `rerank: property "count" of collection "Tickets" is of type int; a rerank reads a text or blob property`,
		},
		{
			name: "a date", property: "day", query: &query,
			wantErr: `rerank: property "day" of collection "Tickets" is of type date; a rerank reads a text or blob property`,
		},
		{
			name: "a uuid", property: "ref", query: &query,
			wantErr: `rerank: property "ref" of collection "Tickets" is of type uuid; a rerank reads a text or blob property`,
		},
		{
			name: "a blob hash", property: "photoHash", query: &query,
			wantErr: `rerank: property "photoHash" of collection "Tickets" is of type blobHash; a rerank reads a text or blob property`,
		},
		{
			name: "the path of a nested property", property: "meta.isbn", query: &query,
			wantErr: `rerank: no such prop with name 'meta.isbn' found in class 'Tickets' in the schema. Check your schema files for which properties in this class are available`,
		},
		{
			name: "a path under a text property", property: "text.isbn", query: &query,
			wantErr: `rerank: no such prop with name 'text.isbn' found in class 'Tickets' in the schema. Check your schema files for which properties in this class are available`,
		},
		{
			name: "an object", property: "meta", query: &query,
			wantErr: `rerank: property "meta" of collection "Tickets" is of type object; a rerank reads a text or blob property`,
		},
		{
			name: "a reference", property: "owner", query: &query,
			wantErr: `rerank: property "owner" of collection "Tickets" is of type Users; a rerank reads a text or blob property`,
		},
		{
			name: "a reference to two collections", property: "owners", query: &query,
			wantErr: `rerank: property "owners" of collection "Tickets" is of type Users, Teams; a rerank reads a text or blob property`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Params{Property: &tt.property, Query: tt.query}.Validate(class)

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}
