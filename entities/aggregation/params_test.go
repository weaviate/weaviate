//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package aggregation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Params_Unmarshal(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		isMultiVector bool
	}{
		{
			name: "regular vector",
			payload: `{
				"targetVector": "vector1",
				"searchVector": [1.0, 2.0]
			}`,
			isMultiVector: false,
		},
		{
			name: "multi vector",
			payload: `{
				"targetVector": "vector1",
				"searchVector": [[1.0, 2.0], [2.0]]
			}`,
			isMultiVector: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var par Params
			err := json.Unmarshal([]byte(tt.payload), &par)
			require.NoError(t, err)
			require.NotNil(t, par.SearchVector)
			if tt.isMultiVector {
				vector, ok := par.SearchVector.([][]float32)
				assert.True(t, ok)
				assert.True(t, len(vector) > 0)
			} else {
				vector, ok := par.SearchVector.([]float32)
				assert.True(t, ok)
				assert.True(t, len(vector) > 0)
			}
		})
	}
}
