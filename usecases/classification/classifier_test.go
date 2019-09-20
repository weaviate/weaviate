package classification

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Classifier(t *testing.T) {
	t.Run("with invalid data", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		_, err := New(sg, nil).Schedule(models.Classification{})
		assert.NotNil(t, err, "should error with invalid user input")
	})

	var id strfmt.UUID
	// so we can reuse it for follow up requests, such as checking the status

	t.Run("with valid data", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		classifier := New(sg, repo)

		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID

		})

		t.Run("retrieving the same classificiation by id", func(t *testing.T) {
			class, err := classifier.Get(id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, id, class.ID)
			assert.Equal(t, models.ClassificationStatusRunning, class.Status)

		})

	})

}
