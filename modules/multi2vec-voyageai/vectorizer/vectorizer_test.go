package vectorizer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

const testImage = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAEElEQVR4nGJiYGAABAAA//8ADAADcZGLFwAAAABJRU5ErkJggg=="

func TestVectorizer(t *testing.T) {
	t.Run("should vectorize text and image", func(t *testing.T) {
		client := &fakeClient{}
		vectorizer := New(client)
		config := newConfigBuilder().
			addSetting("imageFields", []interface{}{"image"}).
			addSetting("textFields", []interface{}{"text"}).
			build()

		propsSchema := []*models.Property{
			{
				Name:     "image",
				DataType: schema.DataTypeBlob.PropString(),
			},
			{
				Name:     "text",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		props := map[string]interface{}{
			"image": testImage,
			"text":  "hello world",
		}
		object := &models.Object{
			ID:         "some-uuid",
			Properties: props,
		}
		comp := moduletools.NewVectorizablePropsComparatorDummy(propsSchema, props)

		vector, _, err := vectorizer.Object(context.Background(), object, comp, config)

		require.Nil(t, err)
		assert.NotNil(t, vector)
		assert.Equal(t, []string{"hello world"}, client.lastInput)
		assert.Equal(t, []string{testImage}, client.lastImages)
	})

	t.Run("with weights", func(t *testing.T) {
		client := &fakeClient{}
		vectorizer := New(client)
		config := newConfigBuilder().
			addSetting("imageFields", []interface{}{"image"}).
			addSetting("textFields", []interface{}{"text"}).
			addWeights([]interface{}{0.7}, []interface{}{0.3}).
			build()

		propsSchema := []*models.Property{
			{
				Name:     "image",
				DataType: schema.DataTypeBlob.PropString(),
			},
			{
				Name:     "text",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		props := map[string]interface{}{
			"image": testImage,
			"text":  "hello world",
		}
		object := &models.Object{
			ID:         "some-uuid",
			Properties: props,
		}
		comp := moduletools.NewVectorizablePropsComparatorDummy(propsSchema, props)

		vector, _, err := vectorizer.Object(context.Background(), object, comp, config)

		require.Nil(t, err)
		assert.NotNil(t, vector)
		expectedVector := []float32{1.85, 3.7, 5.55, 7.4, 9.25}
		assert.Equal(t, expectedVector, vector)
	})
}
