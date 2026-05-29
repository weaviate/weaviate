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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestRefFilterExtractor_paramsForNestedRequest_consistencyLevel(t *testing.T) {
	newExtractor := func(consistencyLevel string) *refFilterExtractor {
		return &refFilterExtractor{
			filter: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.ClassName("Author"),
					Property: schema.PropertyName("wroteBooks"),
					Child: &filters.Path{
						Class:    schema.ClassName("Book"),
						Property: schema.PropertyName("genre"),
					},
				},
				Value: &filters.Value{Value: "dystopian", Type: schema.DataTypeText},
			},
			limit:            100,
			consistencyLevel: consistencyLevel,
		}
	}

	tests := []struct {
		name             string
		consistencyLevel string
		expectReplProps  bool
	}{
		{
			name:             "propagates parent consistency level",
			consistencyLevel: "QUORUM",
			expectReplProps:  true,
		},
		{
			name:             "no consistency level leaves replication props unset",
			consistencyLevel: "",
			expectReplProps:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newExtractor(tt.consistencyLevel)

			params, err := r.paramsForNestedRequest()
			require.NoError(t, err)

			if tt.expectReplProps {
				require.NotNil(t, params.ReplicationProperties)
				assert.Equal(t, tt.consistencyLevel, params.ReplicationProperties.ConsistencyLevel)
			} else {
				assert.Nil(t, params.ReplicationProperties)
			}
		})
	}
}
