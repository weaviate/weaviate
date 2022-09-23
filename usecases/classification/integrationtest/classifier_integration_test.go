//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package classification_integration_test

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Classifier_KNN_SaveConsistency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	var id strfmt.UUID

	shardState := singleShardState()
	sg := &fakeSchemaGetter{shardState: shardState}

	vrepo := db.New(logger, db.Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	},
		&fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	vrepo.SetSchemaGetter(sg)
	err := vrepo.WaitForStartup(context.Background())
	require.Nil(t, err)
	migrator := db.NewMigrator(vrepo, logger)

	// so we can reuse it for follow up requests, such as checking the status
	size := 400
	data := largeTestDataSize(size)

	t.Run("preparations", func(t *testing.T) {
		t.Run("creating the classes", func(t *testing.T) {
			for _, c := range testSchema().Objects.Classes {
				require.Nil(t,
					migrator.AddClass(context.Background(), c, shardState))
			}

			sg.schema = testSchema()
		})

		t.Run("importing the training data", func(t *testing.T) {
			classified := testDataAlreadyClassified()
			bt := make(objects.BatchObjects, len(classified))
			for i, elem := range classified {
				bt[i] = objects.BatchObject{
					OriginalIndex: i,
					UUID:          elem.ID,
					Vector:        elem.Vector,
					Object:        elem.Object(),
				}
			}

			res, err := vrepo.BatchPutObjects(context.Background(), bt)
			require.Nil(t, err)
			for _, elem := range res {
				require.Nil(t, elem.Err)
			}
		})

		t.Run("importing the to be classified data", func(t *testing.T) {
			bt := make(objects.BatchObjects, size)
			for i, elem := range data {
				bt[i] = objects.BatchObject{
					OriginalIndex: i,
					UUID:          elem.ID,
					Vector:        elem.Vector,
					Object:        elem.Object(),
				}
			}
			res, err := vrepo.BatchPutObjects(context.Background(), bt)
			require.Nil(t, err)
			for _, elem := range res {
				require.Nil(t, elem.Err)
			}
		})
	})

	t.Run("classification journey", func(t *testing.T) {
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}
		classifier := classification.New(sg, repo, vrepo, authorizer, logger, nil)

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
			assert.Equal(t, int64(400), class.Meta.CountSucceeded)
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
					Limit: 10000,
				},
			})

			require.Nil(t, err)
			assert.Equal(t, 0, len(res))
		})
	})
}

func Test_Classifier_ZeroShot_SaveConsistency(t *testing.T) {
	t.Skip()
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	var id strfmt.UUID

	sg := &fakeSchemaGetter{shardState: singleShardState()}

	vrepo := db.New(logger, db.Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	vrepo.SetSchemaGetter(sg)
	err := vrepo.WaitForStartup(context.Background())
	require.Nil(t, err)
	migrator := db.NewMigrator(vrepo, logger)

	t.Run("preparations", func(t *testing.T) {
		t.Run("creating the classes", func(t *testing.T) {
			for _, c := range testSchemaForZeroShot().Objects.Classes {
				require.Nil(t,
					migrator.AddClass(context.Background(), c, sg.shardState))
			}

			sg.schema = testSchemaForZeroShot()
		})

		t.Run("importing the training data", func(t *testing.T) {
			classified := testDataZeroShotUnclassified()
			bt := make(objects.BatchObjects, len(classified))
			for i, elem := range classified {
				bt[i] = objects.BatchObject{
					OriginalIndex: i,
					UUID:          elem.ID,
					Vector:        elem.Vector,
					Object:        elem.Object(),
				}
			}

			res, err := vrepo.BatchPutObjects(context.Background(), bt)
			require.Nil(t, err)
			for _, elem := range res {
				require.Nil(t, elem.Err)
			}
		})
	})

	t.Run("classification journey", func(t *testing.T) {
		repo := newFakeClassificationRepo()
		authorizer := &fakeAuthorizer{}
		classifier := classification.New(sg, repo, vrepo, authorizer, logger, nil)

		params := models.Classification{
			Class:              "Recipes",
			BasedOnProperties:  []string{"text"},
			ClassifyProperties: []string{"ofFoodType"},
			Type:               "zeroshot",
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
			assert.Equal(t, int64(2), class.Meta.CountSucceeded)
		})

		t.Run("verify everything is classified", func(t *testing.T) {
			filter := filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    "Recipes",
						Property: "ofFoodType",
					},
					Value: &filters.Value{
						Value: 0,
						Type:  schema.DataTypeInt,
					},
				},
			}
			res, err := vrepo.ClassSearch(context.Background(), traverser.GetParams{
				ClassName: "Recipes",
				Filters:   &filter,
				Pagination: &filters.Pagination{
					Limit: 100000,
				},
			})

			require.Nil(t, err)
			assert.Equal(t, 0, len(res))
		})
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
