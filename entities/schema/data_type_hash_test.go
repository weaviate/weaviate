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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func TestIsLikelySHA256Hash(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid lowercase hash",
			input:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expected: true,
		},
		{
			name:     "valid uppercase hash",
			input:    "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
			expected: true,
		},
		{
			name:     "valid mixed case hash",
			input:    "e3B0C44298fc1c149afBF4c8996fb92427ae41e4649B934ca495991b7852b855",
			expected: true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "too short",
			input:    "e3b0c44298fc1c149afbf4c8996fb924",
			expected: false,
		},
		{
			name:     "too long",
			input:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85500",
			expected: false,
		},
		{
			name:     "non-hex characters",
			input:    "g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expected: false,
		},
		{
			name:     "spaces in string",
			input:    "e3b0c44298fc1c149afbf4c8996fb924 7ae41e4649b934ca495991b7852b855",
			expected: false,
		},
		{
			name:     "all zeros rejected",
			input:    "0000000000000000000000000000000000000000000000000000000000000000",
			expected: false,
		},
		{
			name:     "all f's rejected",
			input:    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			expected: false,
		},
		{
			name:     "regular text 64 chars",
			input:    "this is not a hash but it is exactly sixty four characters long!",
			expected: false,
		},
		{
			name:     "base64 encoded data",
			input:    "aGVsbG8gd29ybGQ=aGVsbG8gd29ybGQ=aGVsbG8gd29ybGQ=aGVsbG8gd29y",
			expected: false,
		},
		{
			name:     "output of HashBlob",
			input:    HashBlob("dGVzdA=="),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsLikelySHA256Hash(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHashBlobHashPrimitiveProperties(t *testing.T) {
	tests := []struct {
		name     string
		class    *models.Class
		props    map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name: "flat top-level blobHash property",
			class: &models.Class{
				Class: "TestClass",
				Properties: []*models.Property{
					{
						Name:     "image",
						DataType: []string{"blobHash"},
					},
					{
						Name:     "text",
						DataType: []string{"text"},
					},
				},
			},
			props: map[string]interface{}{
				"image": "aGVsbG8=",
				"text":  "some text",
			},
			expected: map[string]interface{}{
				"image": HashBlob("aGVsbG8="),
				"text":  "some text",
			},
		},
		{
			name: "nested object blobHash property",
			class: &models.Class{
				Class: "TestClass",
				Properties: []*models.Property{
					{
						Name:     "meta",
						DataType: []string{"object"},
						NestedProperties: []*models.NestedProperty{
							{
								Name:     "image",
								DataType: []string{"blobHash"},
							},
							{
								Name:     "title",
								DataType: []string{"text"},
							},
						},
					},
				},
			},
			props: map[string]interface{}{
				"meta": map[string]interface{}{
					"image": "aGVsbG8=",
					"title": "a title",
				},
			},
			expected: map[string]interface{}{
				"meta": map[string]interface{}{
					"image": HashBlob("aGVsbG8="),
					"title": "a title",
				},
			},
		},
		{
			name: "nested object[] array blobHash properties",
			class: &models.Class{
				Class: "TestClass",
				Properties: []*models.Property{
					{
						Name:     "gallery",
						DataType: []string{"object[]"},
						NestedProperties: []*models.NestedProperty{
							{
								Name:     "image",
								DataType: []string{"blobHash"},
							},
							{
								Name:     "caption",
								DataType: []string{"text"},
							},
						},
					},
				},
			},
			props: map[string]interface{}{
				"gallery": []interface{}{
					map[string]interface{}{
						"image":   "aGVsbG8=",
						"caption": "cap 1",
					},
					map[string]interface{}{
						"image":   "d29ybGQ=",
						"caption": "cap 2",
					},
				},
			},
			expected: map[string]interface{}{
				"gallery": []interface{}{
					map[string]interface{}{
						"image":   HashBlob("aGVsbG8="),
						"caption": "cap 1",
					},
					map[string]interface{}{
						"image":   HashBlob("d29ybGQ="),
						"caption": "cap 2",
					},
				},
			},
		},
		{
			name: "deeply nested object properties",
			class: &models.Class{
				Class: "TestClass",
				Properties: []*models.Property{
					{
						Name:     "meta",
						DataType: []string{"object"},
						NestedProperties: []*models.NestedProperty{
							{
								Name:     "submeta",
								DataType: []string{"object"},
								NestedProperties: []*models.NestedProperty{
									{
										Name:     "image",
										DataType: []string{"blobHash"},
									},
								},
							},
						},
					},
				},
			},
			props: map[string]interface{}{
				"meta": map[string]interface{}{
					"submeta": map[string]interface{}{
						"image": "aGVsbG8=",
					},
				},
			},
			expected: map[string]interface{}{
				"meta": map[string]interface{}{
					"submeta": map[string]interface{}{
						"image": HashBlob("aGVsbG8="),
					},
				},
			},
		},
		{
			name: "nil and missing properties",
			class: &models.Class{
				Class: "TestClass",
				Properties: []*models.Property{
					{
						Name:     "image",
						DataType: []string{"blobHash"},
					},
					{
						Name:     "meta",
						DataType: []string{"object"},
						NestedProperties: []*models.NestedProperty{
							{
								Name:     "image",
								DataType: []string{"blobHash"},
							},
						},
					},
				},
			},
			props: map[string]interface{}{
				"image": nil,
				"meta":  nil,
			},
			expected: map[string]interface{}{
				"image": nil,
				"meta":  nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			HashBlobHashPrimitiveProperties(tt.class, tt.props)
			assert.Equal(t, tt.expected, tt.props)
		})
	}
}
