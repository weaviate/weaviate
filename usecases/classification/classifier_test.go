//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package classification

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Classifier(t *testing.T) {
	t.Run("with invalid data", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		_, err := New(sg, nil, nil).Schedule(context.Background(), models.Classification{})
		assert.NotNil(t, err, "should error with invalid user input")
	})

	var id strfmt.UUID
	// so we can reuse it for follow up requests, such as checking the status

	t.Run("with valid data", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		vectorRepo := newFakeVectorRepo(testDataToBeClassified(), testDataAlreadyClassified())
		classifier := New(sg, repo, vectorRepo)

		k := int32(1)
		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			K:                  &k,
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID

		})

		t.Run("retrieving the same classificiation by id", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, id, class.ID)
			assert.Equal(t, models.ClassificationStatusRunning, class.Status)
		})

		// TODO: improve by polling instead
		time.Sleep(100 * time.Millisecond)

		t.Run("status is now completed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusCompleted, class.Status)
		})

		t.Run("the classifier updated the things/actions with the classified references", func(t *testing.T) {
			require.Len(t, vectorRepo.db, 6)

			t.Run("food", func(t *testing.T) {
				idArticleFoodOne := "06a1e824-889c-4649-97f9-1ed3fa401d8e"
				idArticleFoodTwo := "6402e649-b1e0-40ea-b192-a64eab0d5e56"

				checkRef(t, vectorRepo, idArticleFoodOne, "exactCategory", idCategoryFoodAndDrink)
				checkRef(t, vectorRepo, idArticleFoodTwo, "mainCategory", idMainCategoryFoodAndDrink)
			})

			t.Run("politics", func(t *testing.T) {
				idArticlePoliticsOne := "75ba35af-6a08-40ae-b442-3bec69b355f9"
				idArticlePoliticsTwo := "f850439a-d3cd-4f17-8fbf-5a64405645cd"

				checkRef(t, vectorRepo, idArticlePoliticsOne, "exactCategory", idCategoryPolitics)
				checkRef(t, vectorRepo, idArticlePoliticsTwo, "mainCategory", idMainCategoryPoliticsAndSociety)
			})

			t.Run("society", func(t *testing.T) {
				idArticleSocietyOne := "a2bbcbdc-76e1-477d-9e72-a6d2cfb50109"
				idArticleSocietyTwo := "069410c3-4b9e-4f68-8034-32a066cb7997"

				checkRef(t, vectorRepo, idArticleSocietyOne, "exactCategory", idCategorySociety)
				checkRef(t, vectorRepo, idArticleSocietyTwo, "mainCategory", idMainCategoryPoliticsAndSociety)
			})
		})
	})

	t.Run("when errors occur during classification", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		vectorRepo := newFakeVectorRepo(testDataToBeClassified(), testDataAlreadyClassified())
		vectorRepo.errorOnAggregate = errors.New("something went wrong")
		classifier := New(sg, repo, vectorRepo)

		k := int32(1)
		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			K:                  &k,
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID

		})

		// TODO: improve by polling instead
		time.Sleep(100 * time.Millisecond)

		t.Run("status is now failed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusFailed, class.Status)
			expectedErr := "classification failed: " +
				"classify Article/75ba35af-6a08-40ae-b442-3bec69b355f9: something went wrong, " +
				"classify Article/f850439a-d3cd-4f17-8fbf-5a64405645cd: something went wrong, " +
				"classify Article/a2bbcbdc-76e1-477d-9e72-a6d2cfb50109: something went wrong, " +
				"classify Article/069410c3-4b9e-4f68-8034-32a066cb7997: something went wrong, " +
				"classify Article/06a1e824-889c-4649-97f9-1ed3fa401d8e: something went wrong, " +
				"classify Article/6402e649-b1e0-40ea-b192-a64eab0d5e56: something went wrong"
			assert.Equal(t, expectedErr, class.Error)
		})
	})

	t.Run("when there is nothing to be classified", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		vectorRepo := newFakeVectorRepo(nil, testDataAlreadyClassified())
		classifier := New(sg, repo, vectorRepo)

		k := int32(1)
		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			K:                  &k,
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID

		})

		// TODO: improve by polling instead
		time.Sleep(100 * time.Millisecond)

		t.Run("status is now failed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusFailed, class.Status)
			expectedErr := "classification failed: " +
				"no classes to be classified - did you run a previous classification already?"
			assert.Equal(t, expectedErr, class.Error)
		})
	})
}

func checkRef(t *testing.T, repo *fakeVectorRepo, source, propName, target string) {
	repo.Lock()
	defer repo.Unlock()

	thing, ok := repo.db[strfmt.UUID(source)]
	require.True(t, ok, "thing must be present")

	schema, ok := thing.Schema.(map[string]interface{})
	require.True(t, ok, "schema must be map")

	prop, ok := schema[propName]
	require.True(t, ok, "ref prop must be present")

	refs, ok := prop.(models.MultipleRef)
	require.True(t, ok, "ref prop must be models.MultipleRef")
	require.Len(t, refs, 1, "refs must have len 1")

	assert.Equal(t, fmt.Sprintf("weaviate://localhost/things/%s", target), refs[0].Beacon.String(), "beacon must match")
}
