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

package objects

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/versioned"
)

func TestGetReferenceClassesNamesMissingTarget(t *testing.T) {
	article := &models.Class{
		Class:      "Article",
		Properties: []*models.Property{{Name: "hasParagraphs", DataType: []string{"Paragraph"}}},
	}
	sm := &fakeSchemaManager{GetSchemaResponse: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{article}}}}

	tests := []struct {
		name    string
		toClass string
		wantErr string
	}{
		{name: "target class in the beacon", toClass: "Missing", wantErr: `target class "Missing" not found in schema`},
		{name: "target class from the reference property", wantErr: `target class "Paragraph" not found in schema`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetched := map[string]versioned.Class{"Article": {Class: article}}
			_, _, _, err := getReferenceClasses(context.Background(), nil, sm, "Article", "hasParagraphs", tt.toClass, fetched)
			require.EqualError(t, err, tt.wantErr)
		})
	}
}
