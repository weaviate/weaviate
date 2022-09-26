package test

import (
	"os"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/articles"
	"github.com/stretchr/testify/assert"
)

func TestCentroid(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))

	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	helper.CreateClass(t, paragraphClass)
	helper.CreateClass(t, articleClass)

	defer func() {
		helper.DeleteClass(t, articleClass.Class)
		helper.DeleteClass(t, paragraphClass.Class)
	}()

	para1 := articles.NewParagraph().
		WithVector([]float32{2, 4, 6}).
		WithContents(
			"Anyone who cares about JDM engines knows about the RB26.")
	helper.CreateObject(t, para1.Object())

	para2 := articles.NewParagraph().
		WithVector([]float32{4, 6, 8}).
		WithContents(
			"It is best known for its use in the legendary Skyline GT-R.")
	helper.CreateObject(t, para2.Object())

	t.Run("create object with references", func(t *testing.T) {
		t.Run("with one reference", func(t *testing.T) {
			article := articles.NewArticle().
				WithTitle("Popularity of the Nissan RB26DETT").
				WithReferences(&models.SingleRef{
					Beacon: newBeacon(para1.Class, para1.ID),
				})
			helper.CreateObject(t, article.Object())
			defer helper.DeleteObject(t, article.Object())

			res := helper.AssertGetObject(t, article.Class, article.ID, "vector")
			assert.EqualValues(t, para1.Vector, res.Vector)
		})

		t.Run("with multiple references", func(t *testing.T) {
			article := articles.NewArticle().
				WithTitle("Popularity of the Nissan RB26DETT").
				WithReferences(
					&models.SingleRef{Beacon: newBeacon(para1.Class, para1.ID)},
					&models.SingleRef{Beacon: newBeacon(para2.Class, para2.ID)},
				)
			helper.CreateObject(t, article.Object())
			defer helper.DeleteObject(t, article.Object())

			res := helper.AssertGetObject(t, article.Class, article.ID, "vector")
			expectedVec := []float32{3, 5, 7}
			assert.EqualValues(t, expectedVec, res.Vector)
		})
	})

	t.Run("create object and PUT references", func(t *testing.T) {
		article := articles.NewArticle().
			WithTitle("Popularity of the Nissan RB26DETT")

		helper.CreateObject(t, article.Object())
		defer helper.DeleteObject(t, article.Object())

		res := helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.Nil(t, res.Vector)
		assert.Equal(t, article.ID, res.ID)

		article.WithReferences(
			&models.SingleRef{Beacon: newBeacon(para1.Class, para1.ID)},
			&models.SingleRef{Beacon: newBeacon(para2.Class, para2.ID)},
		)

		helper.UpdateObject(t, article.Object())

		res = helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.Equal(t, article.ID, res.ID)
		expectedVec := []float32{3, 5, 7}
		assert.EqualValues(t, expectedVec, res.Vector)
	})

	t.Run("create object with references, remove references", func(t *testing.T) {
		ref1 := &models.SingleRef{Beacon: newBeacon(para1.Class, para1.ID)}
		ref2 := &models.SingleRef{Beacon: newBeacon(para2.Class, para2.ID)}

		article := articles.NewArticle().
			WithTitle("Popularity of the Nissan RB26DETT").
			WithReferences(ref1, ref2)
		helper.CreateObject(t, article.Object())
		defer helper.DeleteObject(t, article.Object())

		res := helper.AssertGetObject(t, article.Class, article.ID, "vector")
		expectedVec := []float32{3, 5, 7}
		assert.EqualValues(t, expectedVec, res.Vector)

		helper.DeleteReference(t, article.Object(), ref2, "hasParagraphs")
		res = helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.EqualValues(t, para1.Vector, res.Vector)

		helper.DeleteReference(t, article.Object(), ref1, "hasParagraphs")
		res = helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.Nil(t, res.Vector)
	})

	t.Run("create object add references, remove references", func(t *testing.T) {
		ref1 := &models.SingleRef{Beacon: newBeacon(para1.Class, para1.ID)}
		ref2 := &models.SingleRef{Beacon: newBeacon(para2.Class, para2.ID)}

		article := articles.NewArticle().
			WithTitle("Popularity of the Nissan RB26DETT")
		helper.CreateObject(t, article.Object())
		defer helper.DeleteObject(t, article.Object())

		res := helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.Nil(t, res.Vector)

		helper.AddReference(t, article.Object(), ref1, "hasParagraphs")
		res = helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.EqualValues(t, para1.Vector, res.Vector)

		helper.AddReference(t, article.Object(), ref2, "hasParagraphs")
		res = helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.EqualValues(t, []float32{3, 5, 7}, res.Vector)

		helper.DeleteReference(t, article.Object(), ref1, "hasParagraphs")
		res = helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.EqualValues(t, para2.Vector, res.Vector)

		helper.DeleteReference(t, article.Object(), ref2, "hasParagraphs")
		res = helper.AssertGetObject(t, article.Class, article.ID, "vector")
		assert.Nil(t, res.Vector)
	})
}

func newBeacon(className string, id strfmt.UUID) strfmt.URI {
	return crossref.New("localhost", className, id).SingleRef().Beacon
}
