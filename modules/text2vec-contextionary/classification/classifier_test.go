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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	testhelper "github.com/weaviate/weaviate/test/helper"
	usecasesclassfication "github.com/weaviate/weaviate/usecases/classification"
)

func TestContextualClassifier_ParseSettings(t *testing.T) {
	t.Run("should parse with default values with empty settings are passed", func(t *testing.T) {
		// given
		classifier := New(&fakeVectorizer{})
		params := &models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Type:               "text2vec-contextionary-contextual",
		}

		// when
		err := classifier.ParseClassifierSettings(params)

		// then
		assert.Nil(t, err)
		settings := params.Settings
		assert.NotNil(t, settings)
		paramsContextual, ok := settings.(*ParamsContextual)
		assert.NotNil(t, paramsContextual)
		assert.True(t, ok)
		assert.Equal(t, int32(3), *paramsContextual.MinimumUsableWords)
		assert.Equal(t, int32(50), *paramsContextual.InformationGainCutoffPercentile)
		assert.Equal(t, int32(3), *paramsContextual.InformationGainMaximumBoost)
		assert.Equal(t, int32(80), *paramsContextual.TfidfCutoffPercentile)
	})

	t.Run("should parse classifier settings", func(t *testing.T) {
		// given
		classifier := New(&fakeVectorizer{})
		params := &models.Classification{
			Class:              "Article",
			BasedOnProperties:  []string{"description"},
			ClassifyProperties: []string{"exactCategory", "mainCategory"},
			Type:               "text2vec-contextionary-contextual",
			Settings: map[string]interface{}{
				"minimumUsableWords":              json.Number("1"),
				"informationGainCutoffPercentile": json.Number("2"),
				"informationGainMaximumBoost":     json.Number("3"),
				"tfidfCutoffPercentile":           json.Number("4"),
			},
		}

		// when
		err := classifier.ParseClassifierSettings(params)

		// then
		assert.Nil(t, err)
		assert.NotNil(t, params.Settings)
		settings, ok := params.Settings.(*ParamsContextual)
		assert.NotNil(t, settings)
		assert.True(t, ok)
		assert.Equal(t, int32(1), *settings.MinimumUsableWords)
		assert.Equal(t, int32(2), *settings.InformationGainCutoffPercentile)
		assert.Equal(t, int32(3), *settings.InformationGainMaximumBoost)
		assert.Equal(t, int32(4), *settings.TfidfCutoffPercentile)
	})
}

func TestContextualClassifier_Classify(t *testing.T) {
	var id strfmt.UUID
	// so we can reuse it for follow up requests, such as checking the status

	t.Run("with valid data", func(t *testing.T) {
		sg := &fakeSchemaGetter{testSchema()}
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}

		vectorRepo := newFakeVectorRepoContextual(testDataToBeClassified(), testDataPossibleTargets())
		logger, _ := test.NewNullLogger()

		vectorizer := &fakeVectorizer{words: testDataVectors()}
		modulesProvider := NewFakeModulesProvider(vectorizer)
		classifier := usecasesclassfication.New(sg, repo, vectorRepo, authorizer, logger, modulesProvider)

		contextual := "text2vec-contextionary-contextual"
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

				checkRef(t, vectorRepo, idArticleFoodOne, "ExactCategory", "exactCategory", idCategoryFoodAndDrink)
				checkRef(t, vectorRepo, idArticleFoodTwo, "MainCategory", "mainCategory", idMainCategoryFoodAndDrink)
			})

			t.Run("politics", func(t *testing.T) {
				idArticlePoliticsOne := "75ba35af-6a08-40ae-b442-3bec69b355f9"
				idArticlePoliticsTwo := "f850439a-d3cd-4f17-8fbf-5a64405645cd"

				checkRef(t, vectorRepo, idArticlePoliticsOne, "ExactCategory", "exactCategory", idCategoryPolitics)
				checkRef(t, vectorRepo, idArticlePoliticsTwo, "MainCategory", "mainCategory", idMainCategoryPoliticsAndSociety)
			})

			t.Run("society", func(t *testing.T) {
				idArticleSocietyOne := "a2bbcbdc-76e1-477d-9e72-a6d2cfb50109"
				idArticleSocietyTwo := "069410c3-4b9e-4f68-8034-32a066cb7997"

				checkRef(t, vectorRepo, idArticleSocietyOne, "ExactCategory", "exactCategory", idCategorySociety)
				checkRef(t, vectorRepo, idArticleSocietyTwo, "MainCategory", "mainCategory", idMainCategoryPoliticsAndSociety)
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
		classifier := usecasesclassfication.New(sg, repo, vectorRepo, authorizer, logger, nil)

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
		classifier := usecasesclassfication.New(sg, repo, vectorRepo, authorizer, logger, nil)

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

func waitForStatusToNoLongerBeRunning(t *testing.T, classifier *usecasesclassfication.Classifier, id strfmt.UUID) {
	testhelper.AssertEventuallyEqualWithFrequencyAndTimeout(t, true, func() interface{} {
		class, err := classifier.Get(context.Background(), nil, id)
		require.Nil(t, err)
		require.NotNil(t, class)

		return class.Status != models.ClassificationStatusRunning
	}, 100*time.Millisecond, 20*time.Second, "wait until status in no longer running")
}

type genericFakeRepo interface {
	get(strfmt.UUID) (*models.Object, bool)
}

func checkRef(t *testing.T, repo genericFakeRepo, source, targetClass, propName, target string) {
	object, ok := repo.get(strfmt.UUID(source))
	require.True(t, ok, "object must be present")

	schema, ok := object.Properties.(map[string]interface{})
	require.True(t, ok, "schema must be map")

	prop, ok := schema[propName]
	require.True(t, ok, "ref prop must be present")

	refs, ok := prop.(models.MultipleRef)
	require.True(t, ok, "ref prop must be models.MultipleRef")
	require.Len(t, refs, 1, "refs must have len 1")

	assert.Equal(t, crossref.NewLocalhost(targetClass, strfmt.UUID(target)).String(), refs[0].Beacon.String(), "beacon must match")
}

type fakeVectorizer struct {
	words map[string][]float32
}

func (f *fakeVectorizer) MultiVectorForWord(ctx context.Context, words []string) ([][]float32, error) {
	out := make([][]float32, len(words))
	for i, word := range words {
		vector, ok := f.words[strings.ToLower(word)]
		if !ok {
			continue
		}
		out[i] = vector
	}
	return out, nil
}

func (f *fakeVectorizer) VectorOnlyForCorpi(ctx context.Context, corpi []string,
	overrides map[string]string,
) ([]float32, error) {
	words := strings.Split(corpi[0], " ")
	if len(words) == 0 {
		return nil, fmt.Errorf("vector for corpi called without words")
	}

	vectors, _ := f.MultiVectorForWord(ctx, words)

	return f.centroid(vectors, words)
}

func (f *fakeVectorizer) centroid(in [][]float32, words []string) ([]float32, error) {
	withoutNilVectors := make([][]float32, len(in))
	if len(in) == 0 {
		return nil, fmt.Errorf("got nil vector list for words: %v", words)
	}

	i := 0
	for _, vec := range in {
		if vec == nil {
			continue
		}

		withoutNilVectors[i] = vec
		i++
	}
	withoutNilVectors = withoutNilVectors[:i]
	if i == 0 {
		return nil, fmt.Errorf("no usable words: %v", words)
	}

	// take the first vector assuming all have the same length
	out := make([]float32, len(withoutNilVectors[0]))

	for _, vec := range withoutNilVectors {
		for i, dim := range vec {
			out[i] = out[i] + dim
		}
	}

	for i, sum := range out {
		out[i] = sum / float32(len(withoutNilVectors))
	}

	return out, nil
}
