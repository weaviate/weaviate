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

package crossref

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestBulkBuilder(t *testing.T) {
	tests := []struct {
		name          string
		expectedFn    func(id string) string
		estimatedSize int
		iterations    int
		className     string
		withClassName bool
	}{
		{
			name:          "with class name - enough-prealloc",
			withClassName: true,
			className:     "MyClass",
			expectedFn: func(id string) string {
				return fmt.Sprintf("weaviate://localhost/MyClass/%s", id)
			},
			estimatedSize: 25,
			iterations:    25,
		},
		{
			name:          "with class name with non-ASCII- enough-prealloc",
			withClassName: true,
			className:     "My國Class",
			expectedFn: func(id string) string {
				return fmt.Sprintf("weaviate://localhost/My國Class/%s", id)
			},
			estimatedSize: 25,
			iterations:    25,
		},
		{
			name:          "with class name - not enough-prealloc",
			withClassName: true,
			className:     "MyClass",
			expectedFn: func(id string) string {
				return fmt.Sprintf("weaviate://localhost/MyClass/%s", id)
			},
			estimatedSize: 10,
			iterations:    25,
		},
		{
			name:          "with class name with non-ASCII - not enough-prealloc",
			withClassName: true,
			className:     "My國Class",
			expectedFn: func(id string) string {
				return fmt.Sprintf("weaviate://localhost/My國Class/%s", id)
			},
			estimatedSize: 10,
			iterations:    25,
		},
		{
			name:          "without class name - enough-prealloc",
			withClassName: false,
			className:     "MyClass",
			expectedFn: func(id string) string {
				return fmt.Sprintf("weaviate://localhost/%s", id)
			},
			estimatedSize: 25,
			iterations:    25,
		},
		{
			name:          "without class name - not enough-prealloc",
			withClassName: false,
			className:     "MyClass",
			expectedFn: func(id string) string {
				return fmt.Sprintf("weaviate://localhost/%s", id)
			},
			estimatedSize: 10,
			iterations:    25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb := NewBulkBuilderWithEstimates(tt.estimatedSize, tt.className, 1.00)
			for i := 0; i < tt.iterations; i++ {
				id := uuid.New().String()
				if tt.withClassName {
					res := bb.ClassAndID(tt.className, strfmt.UUID(id))
					assert.Equal(t, tt.expectedFn(id), string(res))
				} else {
					res := bb.LegacyIDOnly(strfmt.UUID(id))
					assert.Equal(t, tt.expectedFn(id), string(res))
				}
			}
		})
	}
}
