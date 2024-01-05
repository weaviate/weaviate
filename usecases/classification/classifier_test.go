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

package classification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

func newNullLogger() *logrus.Logger {
	log, _ := test.NewNullLogger()
	return log
}

func Test_Classifier_KNN(t *testing.T) {
	t.Run("with invalid data", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		_, err := New(sg, nil, nil, &fakeAuthorizer{}, newNullLogger(), nil).
			Schedule(context.Background(), nil, models.Classification{})
		assert.NotNil(t, err, "should error with invalid user input")
	})

	var id strfmt.UUID
	// so we can reuse it for follow up requests, such as checking the status

	t.Run("with valid data", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}
		vectorRepo := newFakeVectorRepoKNN(testDataToBeClassified(), testDataAlreadyClassified())
		classifier := New(sg, repo, vectorRepo, authorizer, newNullLogger(), nil)

		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Settings: map[string]interface{}{
				"k": json.Number("1"),
			},
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), nil, params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID
		})

		t.Run("retrieving the same classification by id", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, id, class.ID)
			assert.Equal(t, models.ClassificationStatusRunning, class.Status)
		})

		// TODO: improve by polling instead
		time.Sleep(500 * time.Millisecond)

		t.Run("status is now completed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusCompleted, class.Status)
		})

		t.Run("the classifier updated the actions with the classified references", func(t *testing.T) {
			vectorRepo.Lock()
			require.Len(t, vectorRepo.db, 6)
			vectorRepo.Unlock()

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
		authorizer := &fakeAuthorizer{}
		vectorRepo := newFakeVectorRepoKNN(testDataToBeClassified(), testDataAlreadyClassified())
		vectorRepo.errorOnAggregate = errors.New("something went wrong")
		classifier := New(sg, repo, vectorRepo, authorizer, newNullLogger(), nil)

		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Settings: map[string]interface{}{
				"k": json.Number("1"),
			},
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), nil, params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID
		})

		waitForStatusToNoLongerBeRunning(t, classifier, id)

		t.Run("status is now failed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusFailed, class.Status)
			expectedErrStrings := []string{
				"classification failed: ",
				"classify Article/75ba35af-6a08-40ae-b442-3bec69b355f9: something went wrong",
				"classify Article/f850439a-d3cd-4f17-8fbf-5a64405645cd: something went wrong",
				"classify Article/a2bbcbdc-76e1-477d-9e72-a6d2cfb50109: something went wrong",
				"classify Article/069410c3-4b9e-4f68-8034-32a066cb7997: something went wrong",
				"classify Article/06a1e824-889c-4649-97f9-1ed3fa401d8e: something went wrong",
				"classify Article/6402e649-b1e0-40ea-b192-a64eab0d5e56: something went wrong",
			}

			for _, msg := range expectedErrStrings {
				assert.Contains(t, class.Error, msg)
			}
		})
	})

	t.Run("when there is nothing to be classified", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}
		vectorRepo := newFakeVectorRepoKNN(nil, testDataAlreadyClassified())
		classifier := New(sg, repo, vectorRepo, authorizer, newNullLogger(), nil)

		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Settings: map[string]interface{}{
				"k": json.Number("1"),
			},
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), nil, params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID
		})

		waitForStatusToNoLongerBeRunning(t, classifier, id)

		t.Run("status is now failed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusFailed, class.Status)
			expectedErr := "classification failed: " +
				"no classes to be classified - did you run a previous classification already?"
			assert.Equal(t, expectedErr, class.Error)
		})
	})
}

func Test_Classifier_Custom_Classifier(t *testing.T) {
	var id strfmt.UUID
	// so we can reuse it for follow up requests, such as checking the status

	t.Run("with unreconginzed custom module classifier name", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}

		vectorRepo := newFakeVectorRepoContextual(testDataToBeClassified(), testDataPossibleTargets())
		logger, _ := test.NewNullLogger()

		// vectorizer := &fakeVectorizer{words: testDataVectors()}
		modulesProvider := NewFakeModulesProvider()
		classifier := New(sg, repo, vectorRepo, authorizer, logger, modulesProvider)

		notRecoginzedContextual := "text2vec-contextionary-custom-not-recognized"
		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Type:               notRecoginzedContextual,
		}

		t.Run("scheduling an unrecognized classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), nil, params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID
		})

		t.Run("retrieving the same classification by id", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, id, class.ID)
		})

		// TODO: improve by polling instead
		time.Sleep(500 * time.Millisecond)

		t.Run("status is failed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusFailed, class.Status)
			assert.Equal(t, notRecoginzedContextual, class.Type)
			assert.Contains(t, class.Error, "classifier "+notRecoginzedContextual+" not found")
		})
	})

	t.Run("with valid data", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}

		vectorRepo := newFakeVectorRepoContextual(testDataToBeClassified(), testDataPossibleTargets())
		logger, _ := test.NewNullLogger()

		modulesProvider := NewFakeModulesProvider()
		classifier := New(sg, repo, vectorRepo, authorizer, logger, modulesProvider)

		contextual := "text2vec-contextionary-custom-contextual"
		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Type:               contextual,
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), nil, params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID
		})

		t.Run("retrieving the same classification by id", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, id, class.ID)
		})

		// TODO: improve by polling instead
		time.Sleep(500 * time.Millisecond)

		t.Run("status is now completed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusCompleted, class.Status)
		})

		t.Run("the classifier updated the actions with the classified references", func(t *testing.T) {
			vectorRepo.Lock()
			require.Len(t, vectorRepo.db, 6)
			vectorRepo.Unlock()

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
		authorizer := &fakeAuthorizer{}
		vectorRepo := newFakeVectorRepoKNN(testDataToBeClassified(), testDataAlreadyClassified())
		vectorRepo.errorOnAggregate = errors.New("something went wrong")
		logger, _ := test.NewNullLogger()
		classifier := New(sg, repo, vectorRepo, authorizer, logger, nil)

		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Settings: map[string]interface{}{
				"k": json.Number("1"),
			},
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), nil, params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID
		})

		waitForStatusToNoLongerBeRunning(t, classifier, id)

		t.Run("status is now failed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusFailed, class.Status)
			expectedErrStrings := []string{
				"classification failed: ",
				"classify Article/75ba35af-6a08-40ae-b442-3bec69b355f9: something went wrong",
				"classify Article/f850439a-d3cd-4f17-8fbf-5a64405645cd: something went wrong",
				"classify Article/a2bbcbdc-76e1-477d-9e72-a6d2cfb50109: something went wrong",
				"classify Article/069410c3-4b9e-4f68-8034-32a066cb7997: something went wrong",
				"classify Article/06a1e824-889c-4649-97f9-1ed3fa401d8e: something went wrong",
				"classify Article/6402e649-b1e0-40ea-b192-a64eab0d5e56: something went wrong",
			}
			for _, msg := range expectedErrStrings {
				assert.Contains(t, class.Error, msg)
			}
		})
	})

	t.Run("when there is nothing to be classified", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}
		vectorRepo := newFakeVectorRepoKNN(nil, testDataAlreadyClassified())
		logger, _ := test.NewNullLogger()
		classifier := New(sg, repo, vectorRepo, authorizer, logger, nil)

		params := models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Settings: map[string]interface{}{
				"k": json.Number("1"),
			},
		}

		t.Run("scheduling a classification", func(t *testing.T) {
			class, err := classifier.Schedule(context.Background(), nil, params)
			require.Nil(t, err, "should not error")
			require.NotNil(t, class)

			assert.Len(t, class.ID, 36, "an id was assigned")
			id = class.ID
		})

		waitForStatusToNoLongerBeRunning(t, classifier, id)

		t.Run("status is now failed", func(t *testing.T) {
			class, err := classifier.Get(context.Background(), nil, id)
			require.Nil(t, err)
			require.NotNil(t, class)
			assert.Equal(t, models.ClassificationStatusFailed, class.Status)
			expectedErr := "classification failed: " +
				"no classes to be classified - did you run a previous classification already?"
			assert.Equal(t, expectedErr, class.Error)
		})
	})
}

func Test_Classifier_WhereFilterValidation(t *testing.T) {
	t.Run("when invalid whereFilters are received", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}
		vectorRepo := newFakeVectorRepoKNN(testDataToBeClassified(), testDataAlreadyClassified())
		classifier := New(sg, repo, vectorRepo, authorizer, newNullLogger(), nil)

		t.Run("with only one of the where filters being set", func(t *testing.T) {
			whereFilter := &models.WhereFilter{
				Path:      []string{"id"},
				Operator:  "Like",
				ValueText: ptString("*"),
			}
			testData := []struct {
				name                  string
				classificationType    string
				classificationFilters *models.ClassificationFilters
			}{
				{
					name:               "Contextual only source where filter set",
					classificationType: TypeContextual,
					classificationFilters: &models.ClassificationFilters{
						SourceWhere: whereFilter,
					},
				},
				{
					name:               "Contextual only target where filter set",
					classificationType: TypeContextual,
					classificationFilters: &models.ClassificationFilters{
						TargetWhere: whereFilter,
					},
				},
				{
					name:               "ZeroShot only source where filter set",
					classificationType: TypeZeroShot,
					classificationFilters: &models.ClassificationFilters{
						SourceWhere: whereFilter,
					},
				},
				{
					name:               "ZeroShot only target where filter set",
					classificationType: TypeZeroShot,
					classificationFilters: &models.ClassificationFilters{
						TargetWhere: whereFilter,
					},
				},
				{
					name:               "KNN only source where filter set",
					classificationType: TypeKNN,
					classificationFilters: &models.ClassificationFilters{
						SourceWhere: whereFilter,
					},
				},
				{
					name:               "KNN only training set where filter set",
					classificationType: TypeKNN,
					classificationFilters: &models.ClassificationFilters{
						TrainingSetWhere: whereFilter,
					},
				},
			}
			for _, td := range testData {
				t.Run(td.name, func(t *testing.T) {
					params := models.Classification{
						Class:              "Article",
						BasedOnProperties:  []string{"description"},
						ClassifyProperties: []string{"exactCategory", "mainCategory"},
						Settings: map[string]interface{}{
							"k": json.Number("1"),
						},
						Type:    td.classificationType,
						Filters: td.classificationFilters,
					}
					class, err := classifier.Schedule(context.Background(), nil, params)
					assert.Nil(t, err)
					assert.NotNil(t, class)

					assert.Len(t, class.ID, 36, "an id was assigned")
					waitForStatusToNoLongerBeRunning(t, classifier, class.ID)
				})
			}
		})
	})

	t.Run("[deprecated string] when valueString whereFilters are received", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}
		vectorRepo := newFakeVectorRepoKNN(testDataToBeClassified(), testDataAlreadyClassified())
		classifier := New(sg, repo, vectorRepo, authorizer, newNullLogger(), nil)

		validFilter := &models.WhereFilter{
			Path:      []string{"description"},
			Operator:  "Equal",
			ValueText: ptString("valueText is valid"),
		}
		deprecatedFilter := &models.WhereFilter{
			Path:        []string{"description"},
			Operator:    "Equal",
			ValueString: ptString("valueString is accepted"),
		}

		t.Run("with deprecated sourceFilter", func(t *testing.T) {
			params := models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory", "mainCategory"},
				Settings: map[string]interface{}{
					"k": json.Number("1"),
				},
				Filters: &models.ClassificationFilters{
					SourceWhere: deprecatedFilter,
				},
				Type: TypeContextual,
			}

			_, err := classifier.Schedule(context.Background(), nil, params)
			assert.Nil(t, err)
		})

		t.Run("with deprecated targetFilter", func(t *testing.T) {
			params := models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory", "mainCategory"},
				Settings: map[string]interface{}{
					"k": json.Number("1"),
				},
				Filters: &models.ClassificationFilters{
					SourceWhere: validFilter,
					TargetWhere: deprecatedFilter,
				},
				Type: TypeContextual,
			}

			_, err := classifier.Schedule(context.Background(), nil, params)
			assert.Nil(t, err)
		})

		t.Run("with deprecated trainingFilter", func(t *testing.T) {
			params := models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory", "mainCategory"},
				Settings: map[string]interface{}{
					"k": json.Number("1"),
				},
				Filters: &models.ClassificationFilters{
					SourceWhere:      validFilter,
					TrainingSetWhere: deprecatedFilter,
				},
				Type: TypeKNN,
			}

			_, err := classifier.Schedule(context.Background(), nil, params)
			assert.Nil(t, err)
		})
	})
}

type genericFakeRepo interface {
	get(strfmt.UUID) (*models.Object, bool)
}

func checkRef(t *testing.T, repo genericFakeRepo, source, propName, target string) {
	object, ok := repo.get(strfmt.UUID(source))
	require.True(t, ok, "object must be present")

	schema, ok := object.Properties.(map[string]interface{})
	require.True(t, ok, "schema must be map")

	prop, ok := schema[propName]
	require.True(t, ok, "ref prop must be present")

	refs, ok := prop.(models.MultipleRef)
	require.True(t, ok, "ref prop must be models.MultipleRef")
	require.Len(t, refs, 1, "refs must have len 1")

	assert.Equal(t, fmt.Sprintf("weaviate://localhost/%s", target), refs[0].Beacon.String(), "beacon must match")
}

func waitForStatusToNoLongerBeRunning(t *testing.T, classifier *Classifier, id strfmt.UUID) {
	testhelper.AssertEventuallyEqualWithFrequencyAndTimeout(t, true, func() interface{} {
		class, err := classifier.Get(context.Background(), nil, id)
		require.Nil(t, err)
		require.NotNil(t, class)

		return class.Status != models.ClassificationStatusRunning
	}, 100*time.Millisecond, 20*time.Second, "wait until status in no longer running")
}
