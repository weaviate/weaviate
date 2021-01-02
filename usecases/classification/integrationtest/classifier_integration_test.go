//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package classification_integration_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Classifier_KNN_SaveConsistency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	var id strfmt.UUID

	sg := &fakeSchemaGetter{}

	vrepo := db.New(logger, db.Config{RootPath: dirName})
	vrepo.SetSchemaGetter(sg)
	err := vrepo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)
	migrator := db.NewMigrator(vrepo, logger)

	// so we can reuse it for follow up requests, such as checking the status
	size := 400
	data := largeTestDataSize(size)

	t.Run("creating the classes", func(t *testing.T) {
		for _, c := range testSchema().Things.Classes {
			require.Nil(t,
				migrator.AddClass(context.Background(), kind.Thing, c))
		}

		sg.schema = testSchema()
	})

	t.Run("importing the training data", func(t *testing.T) {
		classified := testDataAlreadyClassified()
		bt := make(kinds.BatchThings, len(classified))
		for i, elem := range classified {
			bt[i] = kinds.BatchThing{
				OriginalIndex: i,
				UUID:          elem.ID,
				Vector:        elem.Vector,
				Thing:         elem.Thing(),
			}
		}

		res, err := vrepo.BatchPutThings(context.Background(), bt)
		require.Nil(t, err)
		for _, elem := range res {
			require.Nil(t, elem.Err)
		}
	})

	t.Run("importing the to be classified data", func(t *testing.T) {
		bt := make(kinds.BatchThings, size)
		for i, elem := range data {
			bt[i] = kinds.BatchThing{
				OriginalIndex: i,
				UUID:          elem.ID,
				Vector:        elem.Vector,
				Thing:         elem.Thing(),
			}
		}
		res, err := vrepo.BatchPutThings(context.Background(), bt)
		require.Nil(t, err)
		for _, elem := range res {
			require.Nil(t, elem.Err)
		}
	})

	t.Run("with valid data", func(t *testing.T) {
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}
		classifier := classification.New(sg, repo, vrepo, authorizer, nil, logger)

		k := int32(1)
		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			K:                  &k,
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), nil, params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID
		})

		t.Run("retrieving the same classificiation by id", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, id, class.ID)
			assert.Equal(t, models.ClassificationStatusRunning, class.Status)
		})

		waitForStatusToNoLongerBeRunning(t, classifier, id)

		t.Run("status is now completed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusCompleted, class.Status)
		})

		t.Run("verify everything is classified", func(t *testing.T) {
			filter := filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    "Article",
						Property: "exactCategory",
					},
					Value: &filters.Value{
						Value: 0,
						Type:  schema.DataTypeInt,
					},
				},
			}
			res, err := vrepo.ClassSearch(context.Background(), traverser.GetParams{
				ClassName: "Article",
				Filters:   &filter,
				Pagination: &filters.Pagination{
					Limit: 100000,
				},
				Kind: kind.Thing,
			})

			require.Nil(t, err)
			assert.Equal(t, 0, len(res))
		})

		// t.Run("the classifier updated the things/actions with the classified references", func(t *testing.T) {
		// 	vectorRepo.Lock()
		// 	require.Len(t, vectorRepo.db, size)
		// 	vectorRepo.Unlock()

		// 	// t.Run("food", func(t *testing.T) {
		// 	// 	idArticleFoodOne := "06a1e824-889c-4649-97f9-1ed3fa401d8e"
		// 	// 	idArticleFoodTwo := "6402e649-b1e0-40ea-b192-a64eab0d5e56"

		// 	// 	checkRef(t, vectorRepo, idArticleFoodOne, "exactCategory", idCategoryFoodAndDrink)
		// 	// 	checkRef(t, vectorRepo, idArticleFoodTwo, "mainCategory", idMainCategoryFoodAndDrink)
		// 	// })

		// 	// t.Run("politics", func(t *testing.T) {
		// 	// 	idArticlePoliticsOne := "75ba35af-6a08-40ae-b442-3bec69b355f9"
		// 	// 	idArticlePoliticsTwo := "f850439a-d3cd-4f17-8fbf-5a64405645cd"

		// 	// 	checkRef(t, vectorRepo, idArticlePoliticsOne, "exactCategory", idCategoryPolitics)
		// 	// 	checkRef(t, vectorRepo, idArticlePoliticsTwo, "mainCategory", idMainCategoryPoliticsAndSociety)
		// 	// })

		// 	// t.Run("society", func(t *testing.T) {
		// 	// 	idArticleSocietyOne := "a2bbcbdc-76e1-477d-9e72-a6d2cfb50109"
		// 	// 	idArticleSocietyTwo := "069410c3-4b9e-4f68-8034-32a066cb7997"

		// 	// 	checkRef(t, vectorRepo, idArticleSocietyOne, "exactCategory", idCategorySociety)
		// 	// 	checkRef(t, vectorRepo, idArticleSocietyTwo, "mainCategory", idMainCategoryPoliticsAndSociety)
		// 	// })
		// })
	})
}

func waitForStatusToNoLongerBeRunning(t *testing.T, classifier *classification.Classifier, id strfmt.UUID) {
	testhelper.AssertEventuallyEqualWithFrequencyAndTimeout(t, true, func() interface{} {
		class, err := classifier.Get(context.Background(), nil, id)
		require.Nil(t, err)
		require.NotNil(t, class)

		return class.Status != models.ClassificationStatusRunning
	}, 100*time.Millisecond, 20*time.Second, "wait until status in no longer running")
}
