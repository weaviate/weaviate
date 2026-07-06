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

package v1

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSchemaResolver(t *testing.T) {
	class := &models.Class{
		Class: "Article",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "wordCount", DataType: []string{"int"}},
		},
	}

	// countingGetClass returns class for its own name and nil for anything else,
	// counting every invocation so tests can assert memoization.
	countingGetClass := func(calls *int) func(string) (*models.Class, error) {
		return func(className string) (*models.Class, error) {
			*calls++
			if className == class.Class {
				return class, nil
			}
			return nil, nil
		}
	}

	t.Run("resolve", func(t *testing.T) {
		tests := []struct {
			name      string
			getClass  func(string) (*models.Class, error)
			className string
			wantErr   string
		}{
			{
				name:      "existing class resolves",
				getClass:  func(string) (*models.Class, error) { return class, nil },
				className: "Article",
			},
			{
				name:      "nil class returns not-found",
				getClass:  func(string) (*models.Class, error) { return nil, nil },
				className: "Unknown",
				wantErr:   "could not find class Unknown in schema",
			},
			{
				name:      "getClass error is propagated",
				getClass:  func(n string) (*models.Class, error) { return nil, fmt.Errorf("not authorized for %s", n) },
				className: "Article",
				wantErr:   "not authorized for Article",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rc, err := newSchemaResolver(tt.getClass).resolve(tt.className)
				if tt.wantErr != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tt.wantErr)
					return
				}
				require.NoError(t, err)
				require.NotNil(t, rc)
			})
		}
	})

	t.Run("property lookups", func(t *testing.T) {
		tests := []struct {
			name         string
			prop         string
			wantDataType schema.DataType
			wantErr      bool
		}{
			{name: "primitive int property", prop: "wordCount", wantDataType: schema.DataTypeInt},
			{name: "primitive text property", prop: "title", wantDataType: schema.DataTypeText},
			{name: "unknown property errors", prop: "missing", wantErr: true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var calls int
				rc, err := newSchemaResolver(countingGetClass(&calls)).resolve("Article")
				require.NoError(t, err)

				dt, dtErr := rc.primitiveDataType(tt.prop)
				prop, propErr := rc.property(tt.prop)
				if tt.wantErr {
					require.Error(t, dtErr)
					require.Error(t, propErr)
					return
				}
				require.NoError(t, dtErr)
				require.Equal(t, tt.wantDataType, *dt)
				require.NoError(t, propErr)
				require.Equal(t, tt.prop, prop.Name)
			})
		}
	})

	t.Run("memoizes across repeated lookups", func(t *testing.T) {
		var calls int
		rs := newSchemaResolver(countingGetClass(&calls))

		for i := 0; i < 5; i++ {
			rc, err := rs.resolve("Article")
			require.NoError(t, err)
			_, err = rc.primitiveDataType("wordCount")
			require.NoError(t, err)
		}

		require.Equal(t, 1, calls, "getClass must be called once per distinct class, not once per lookup")
	})
}
