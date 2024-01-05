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

package test

import (
	"os"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestCentroid(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))

	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()
	articleClass.ModuleConfig = map[string]interface{}{
		"ref2vec-centroid": map[string]interface{}{
			"referenceProperties": []string{"hasParagraphs"},
		},
	}
	articleClass.Vectorizer = "ref2vec-centroid"

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

	t.Run("batch create objects", func(t *testing.T) {
		ref1 := &models.SingleRef{Beacon: newBeacon(para1.Class, para1.ID)}
		ref2 := &models.SingleRef{Beacon: newBeacon(para2.Class, para2.ID)}

		article1 := articles.NewArticle().
			WithTitle("Popularity of the Nissan RB26DETT").
			WithReferences(ref1, ref2)
		defer helper.DeleteObject(t, article1.Object())

		article2 := articles.NewArticle().
			WithTitle("A Nissan RB Origin Story").
			WithReferences(ref1, ref2)
		defer helper.DeleteObject(t, article2.Object())

		batch := []*models.Object{article1.Object(), article2.Object()}
		helper.CreateObjectsBatch(t, batch)

		res := helper.AssertGetObject(t, article1.Class, article1.ID, "vector")
		assert.EqualValues(t, []float32{3, 5, 7}, res.Vector)
		res = helper.AssertGetObject(t, article2.Class, article2.ID, "vector")
		assert.EqualValues(t, []float32{3, 5, 7}, res.Vector)
	})

	// TODO: Uncomment when batch refs supports centroid re-calc
	//t.Run("batch create references", func(t *testing.T) {
	//	article := articles.NewArticle().
	//		WithTitle("Popularity of the Nissan RB26DETT")
	//	defer helper.DeleteObject(t, article.Object())
	//
	//	refs := []*models.BatchReference{
	//		{
	//			From: strfmt.URI(crossref.NewSource("Article", "hasParagraphs", article.ID).String()),
	//			To:   strfmt.URI(crossref.NewLocalhost("Paragraph", para1.ID).String()),
	//		},
	//		{
	//			From: strfmt.URI(crossref.NewSource("Article", "hasParagraphs", article.ID).String()),
	//			To:   strfmt.URI(crossref.NewLocalhost("Paragraph", para2.ID).String()),
	//		},
	//	}
	//
	//	helper.AddReferences(t, refs)
	//	res := helper.AssertGetObject(t, article.Class, article.ID, "vector")
	//	assert.EqualValues(t, []float32{3, 5, 7}, res.Vector)
	//})
}

func newBeacon(className string, id strfmt.UUID) strfmt.URI {
	return crossref.New("localhost", className, id).SingleRef().Beacon
}
