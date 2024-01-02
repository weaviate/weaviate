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

package sharding

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Config(t *testing.T) {
	type test struct {
		name        string
		input       interface{}
		expected    Config
		expectedErr error
	}

	tests := []test{
		{
			name:  "nothing specified, all defaults",
			input: nil,
			expected: Config{
				VirtualPerPhysical:  DefaultVirtualPerPhysical,
				DesiredCount:        7, // cluster size
				DesiredVirtualCount: DefaultVirtualPerPhysical * 7,
				ActualCount:         7, // cluster size
				ActualVirtualCount:  DefaultVirtualPerPhysical * 7,
				Key:                 DefaultKey,
				Strategy:            DefaultStrategy,
				Function:            DefaultFunction,
			},
		},

		{
			name: "everything specified, everything legal",
			input: map[string]interface{}{
				"virtualPerPhysical":  json.Number("64"),
				"desiredCount":        json.Number("3"),
				"desiredVirtualCount": json.Number("192"),
				"replicas":            json.Number("3"),
				"key":                 "_id",
				"strategy":            "hash",
				"function":            "murmur3",
			},
			expected: Config{
				VirtualPerPhysical:  64,
				DesiredCount:        3,
				DesiredVirtualCount: 192,
				ActualCount:         3,
				ActualVirtualCount:  192,
				Key:                 "_id",
				Strategy:            "hash",
				Function:            "murmur3",
			},
		},

		{
			name: "everything specified, everything legal, from disk using floats for numbers",
			input: map[string]interface{}{
				"virtualPerPhysical":  float64(64),
				"desiredCount":        float64(3),
				"desiredVirtualCount": float64(192),
				"replicas":            float64(4),
				"key":                 "_id",
				"strategy":            "hash",
				"function":            "murmur3",
			},
			expected: Config{
				VirtualPerPhysical:  64,
				DesiredCount:        3,
				DesiredVirtualCount: 192,
				ActualCount:         3,
				ActualVirtualCount:  192,
				Key:                 "_id",
				Strategy:            "hash",
				Function:            "murmur3",
			},
		},

		{
			name: "unsupported sharding key",
			input: map[string]interface{}{
				"key":      "myCustomField",
				"strategy": "hash",
				"function": "murmur3",
			},
			expectedErr: errors.New("sharding only supported on key '_id' " +
				"for now, got: myCustomField"),
		},

		{
			name: "unsupported sharding strategy",
			input: map[string]interface{}{
				"key":      "_id",
				"strategy": "range",
				"function": "murmur3",
			},
			expectedErr: errors.New("sharding only supported with strategy 'hash' " +
				"for now, got: range"),
		},

		{
			name: "unsupported sharding function",
			input: map[string]interface{}{
				"key":      "_id",
				"strategy": "hash",
				"function": "md5",
			},
			expectedErr: errors.New("sharding only supported with function 'murmur3' " +
				"for now, got: md5"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := ParseConfig(test.input, 7) // pretend cluster size is 7

			if test.expectedErr == nil {
				assert.Nil(t, err)
				assert.Equal(t, test.expected, cfg)
			} else {
				require.NotNil(t, err, "should have error'd")
				assert.Equal(t, test.expectedErr.Error(), err.Error())
			}
		})
	}
}
