//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package vectorizer

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorizer(t *testing.T) {
	t.Run("should vectorize image", func(t *testing.T) {
		// given
		client := &fakeClient{}
		vectorizer := &Vectorizer{client}
		config := newConfigBuilder().addSetting("imageFields", []interface{}{"image"}).build()
		settings := NewClassSettings(config)
		object := &models.Object{
			ID: "som-uuid",
			Properties: map[string]interface{}{
				"image": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAApAAAAByCAY",
			},
		}

		// when
		err := vectorizer.Object(context.Background(), object, settings)

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
			ID: "som-uuid",
			Properties: map[string]interface{}{
				"image1": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAApAAAAByCAY",
				"image2": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAApAAAAByCAY",
			},
		}

		// when
		err := vectorizer.Object(context.Background(), object, settings)

		// then
		require.Nil(t, err)
		assert.NotNil(t, object.Vector)
	})

	t.Run("should not vectorize image that is not a base64 encoded image", func(t *testing.T) {
		// given
		client := &fakeClient{}
		vectorizer := &Vectorizer{client}
		config := newConfigBuilder().addSetting("imageFields", []interface{}{"image"}).build()
		settings := NewClassSettings(config)
		object := &models.Object{
			ID: "som-uuid",
			Properties: map[string]interface{}{
				"image": "image",
			},
		}

		// when
		err := vectorizer.Object(context.Background(), object, settings)

		// then
		require.NotNil(t, err)
		assert.Equal(t, "not base64 encoded image", err.Error())
		assert.Nil(t, object.Vector)
	})
}
