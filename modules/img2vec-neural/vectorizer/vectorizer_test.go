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

package vectorizer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

const image = "iVBORw0KGgoAAAANSUhEUgAAAGAAAAA/CAYAAAAfQM0aAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyRpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoTWFjaW50b3NoKSIgeG1wTU06SW5zdGFuY2VJRD0ieG1wLmlpZDpCRjQ5NEM3RDI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDpCRjQ5NEM3RTI5QTkxMUUyOTc1NENCMzI4N0QwNDNCOSI+IDx4bXBNTTpEZXJpdmVkRnJvbSBzdFJlZjppbnN0YW5jZUlEPSJ4bXAuaWlkOkJGNDk0QzdCMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5IiBzdFJlZjpkb2N1bWVudElEPSJ4bXAuZGlkOkJGNDk0QzdDMjlBOTExRTI5NzU0Q0IzMjg3RDA0M0I5Ii8+IDwvcmRmOkRlc2NyaXB0aW9uPiA8L3JkZjpSREY+IDwveDp4bXBtZXRhPiA8P3hwYWNrZXQgZW5kPSJyIj8+WeGRxAAAB2hJREFUeNrUXFts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"

func TestVectorizer(t *testing.T) {
	t.Run("should vectorize image", func(t *testing.T) {
		// given
		client := &fakeClient{}
		vectorizer := &Vectorizer{client}
		config := newConfigBuilder().addSetting("imageFields", []interface{}{"image"}).build()
		settings := NewClassSettings(config)
		object := &models.Object{
			ID: "some-uuid",
			Properties: map[string]interface{}{
				"image": image,
			},
		}

		// when
		err := vectorizer.Object(context.Background(), object, nil, settings)

		// then
		require.Nil(t, err)
		assert.NotNil(t, object.Vector)
	})

	t.Run("should vectorize 2 image fields", func(t *testing.T) {
		// given
		client := &fakeClient{}
		vectorizer := &Vectorizer{client}
		config := newConfigBuilder().addSetting("imageFields", []interface{}{"image1", "image2"}).build()
		settings := NewClassSettings(config)
		object := &models.Object{
			ID: "some-uuid",
			Properties: map[string]interface{}{
				"image1": image,
				"image2": image,
			},
		}

		// when
		err := vectorizer.Object(context.Background(), object, nil, settings)

		// then
		require.Nil(t, err)
		assert.NotNil(t, object.Vector)
	})
}

func TestVectorizerWithDiff(t *testing.T) {
	type testCase struct {
		name              string
		input             *models.Object
		diff              *moduletools.ObjectDiff
		expectedVectorize bool
	}

	tests := []testCase{
		{
			name: "no diff",
			input: &models.Object{
				ID: "some-uuid",
				Properties: map[string]interface{}{
					"image": image,
					"text":  "text",
				},
			},
			diff:              nil,
			expectedVectorize: true,
		},
		{
			name: "diff all props unchanged",
			input: &models.Object{
				ID: "some-uuid",
				Properties: map[string]interface{}{
					"image": image,
					"text":  "text",
				},
			},
			diff: newObjectDiffWithVector().
				WithProp("image", image, image).
				WithProp("text", "text", "text"),
			expectedVectorize: false,
		},
		{
			name: "diff one vectorizable prop changed",
			input: &models.Object{
				ID: "some-uuid",
				Properties: map[string]interface{}{
					"image": image,
					"text":  "text",
				},
			},
			diff: newObjectDiffWithVector().
				WithProp("image", "", image),
			expectedVectorize: true,
		},
		{
			name: "all non-vectorizable props changed",
			input: &models.Object{
				ID: "some-uuid",
				Properties: map[string]interface{}{
					"image": image,
					"text":  "text",
				},
			},
			diff: newObjectDiffWithVector().
				WithProp("text", "old text", "text"),
			expectedVectorize: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			vectorizer := &Vectorizer{client}
			config := newConfigBuilder().addSetting("imageFields", []interface{}{"image"}).build()
			settings := NewClassSettings(config)

			err := vectorizer.Object(context.Background(), test.input, test.diff, settings)

			require.Nil(t, err)
			if test.expectedVectorize {
				assert.Equal(t, models.C11yVector{1, 2, 3, 4, 5}, test.input.Vector)
			} else {
				assert.Equal(t, models.C11yVector{0, 0, 0, 0, 0}, test.input.Vector)
			}
		})
	}
}

func newObjectDiffWithVector() *moduletools.ObjectDiff {
	return moduletools.NewObjectDiff([]float32{0, 0, 0, 0, 0})
}
