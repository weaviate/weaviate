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

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

func Test_MergingObjects(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "MergeTestTarget",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
				},
				{
					Class:               "MergeTestSource",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{ // tries to have "one of each property type"
						{
							Name:         "string",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "text",
							DataType: []string{"text"},
						},
						{
							Name:     "number",
							DataType: []string{"number"},
						},
						{
							Name:     "int",
							DataType: []string{"int"},
						},
						{
							Name:     "date",
							DataType: []string{"date"},
						},
						{
							Name:     "geo",
							DataType: []string{"geoCoordinates"},
						},
						{
							Name:     "toTarget",
							DataType: []string{"MergeTestTarget"},
						},
					},
				},
				{
					Class:               "MergeTestNoVector",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "foo",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
				},
			},
		},
	}

	t.Run("add required classes", func(t *testing.T) {
		for _, class := range sch.Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
				require.Nil(t, err)
			})
		}
	})

	schemaGetter.schema = sch

	target1 := strfmt.UUID("897be7cc-1ae1-4b40-89d9-d3ea98037638")
	target2 := strfmt.UUID("5cc94aba-93e4-408a-ab19-3d803216a04e")
	target3 := strfmt.UUID("81982705-8b1e-4228-b84c-911818d7ee85")
	target4 := strfmt.UUID("7f69c263-17f4-4529-a54d-891a7c008ca4")
	sourceID := strfmt.UUID("8738ddd5-a0ed-408d-a5d6-6f818fd56be6")
	noVecID := strfmt.UUID("b4933761-88b2-4666-856d-298eb1ad0a59")

	t.Run("add objects", func(t *testing.T) {
		now := time.Now().UnixNano() / int64(time.Millisecond)
		err := repo.PutObject(context.Background(), &models.Object{
			ID:    sourceID,
			Class: "MergeTestSource",
			Properties: map[string]interface{}{
				"string": "only the string prop set",
			},
			CreationTimeUnix:   now,
			LastUpdateTimeUnix: now,
		}, []float32{0.5}, nil)
		require.Nil(t, err)

		targetDimensionsBefore := GetDimensionsFromRepo(repo, "MergeTestTarget")

		targets := []strfmt.UUID{target1, target2, target3, target4}

		for i, target := range targets {
			err = repo.PutObject(context.Background(), &models.Object{
				ID:    target,
				Class: "MergeTestTarget",
				Properties: map[string]interface{}{
					"name": fmt.Sprintf("target item %d", i),
				},
			}, []float32{0.5}, nil)
			require.Nil(t, err)
		}

		targetDimensionsAfter := GetDimensionsFromRepo(repo, "MergeTestTarget")
		require.Equal(t, targetDimensionsBefore+4, targetDimensionsAfter)

		err = repo.PutObject(context.Background(), &models.Object{
			ID:    noVecID,
			Class: "MergeTestNoVector",
			Properties: map[string]interface{}{
				"foo": "bar",
			},
			CreationTimeUnix:   now,
			LastUpdateTimeUnix: now,
		}, nil, nil)
		require.Nil(t, err)

		targetDimensionsAfterNoVec := GetDimensionsFromRepo(repo, "MergeTestTarget")
		require.Equal(t, targetDimensionsAfter, targetDimensionsAfterNoVec)
	})

	var lastUpdateTimeUnix int64

	t.Run("fetch original object's update timestamp", func(t *testing.T) {
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, additional.Properties{
			LastUpdateTimeUnix: true,
		}, "")
		require.Nil(t, err)

		lastUpdateTimeUnix = source.Object().LastUpdateTimeUnix
		require.NotEmpty(t, lastUpdateTimeUnix)
	})

	t.Run("merge other previously unset properties into it", func(t *testing.T) {
		// give the lastUpdateTimeUnix time to be different.
		// on some machines this may not be needed, but for
		// faster processors, the difference is undetectable
		time.Sleep(time.Millisecond)

		md := objects.MergeDocument{
			Class: "MergeTestSource",
			ID:    sourceID,
			PrimitiveSchema: map[string]interface{}{
				"number": 7.0,
				"int":    int64(9),
				"geo": &models.GeoCoordinates{
					Latitude:  ptFloat32(30.2),
					Longitude: ptFloat32(60.2),
				},
				"text": "some text",
			},
			UpdateTime: time.Now().UnixNano() / int64(time.Millisecond),
		}

		err := repo.Merge(context.Background(), md, nil, "")
		assert.Nil(t, err)
	})

	t.Run("compare merge object's update time with original", func(t *testing.T) {
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, additional.Properties{
			LastUpdateTimeUnix: true,
		}, "")
		require.Nil(t, err)

		assert.Greater(t, source.Object().LastUpdateTimeUnix, lastUpdateTimeUnix)
	})

	t.Run("check that the object was successfully merged", func(t *testing.T) {
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, additional.Properties{}, "")
		require.Nil(t, err)

		sch := source.Object().Properties.(map[string]interface{})
		expectedSchema := map[string]interface{}{
			// from original
			"string": "only the string prop set",

			// from merge
			"number": 7.0,
			"int":    float64(9),
			"geo": &models.GeoCoordinates{
				Latitude:  ptFloat32(30.2),
				Longitude: ptFloat32(60.2),
			},
			"text": "some text",
		}

		assert.Equal(t, expectedSchema, sch)
	})

	t.Run("trying to merge from non-existing index", func(t *testing.T) {
		md := objects.MergeDocument{
			Class: "WrongClass",
			ID:    sourceID,
			PrimitiveSchema: map[string]interface{}{
				"number": 7.0,
			},
		}

		err := repo.Merge(context.Background(), md, nil, "")
		assert.Equal(t, fmt.Errorf(
			"merge from non-existing index for WrongClass"), err)
	})
	t.Run("add a reference and replace one prop", func(t *testing.T) {
		source, err := crossref.ParseSource(fmt.Sprintf(
			"weaviate://localhost/MergeTestSource/%s/toTarget", sourceID))
		require.Nil(t, err)
		targets := []strfmt.UUID{target1}
		refs := make(objects.BatchReferences, len(targets))
		for i, target := range targets {
			to, err := crossref.Parse(fmt.Sprintf("weaviate://localhost/%s", target))
			require.Nil(t, err)
			refs[i] = objects.BatchReference{
				Err:  nil,
				From: source,
				To:   to,
			}
		}
		md := objects.MergeDocument{
			Class: "MergeTestSource",
			ID:    sourceID,
			PrimitiveSchema: map[string]interface{}{
				"string": "let's update the string prop",
			},
			References: refs,
		}
		err = repo.Merge(context.Background(), md, nil, "")
		assert.Nil(t, err)
	})

	t.Run("check that the object was successfully merged", func(t *testing.T) {
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, additional.Properties{}, "")
		require.Nil(t, err)

		ref, err := crossref.Parse(fmt.Sprintf("weaviate://localhost/%s", target1))
		require.Nil(t, err)

		sch := source.Object().Properties.(map[string]interface{})
		expectedSchema := map[string]interface{}{
			"string": "let's update the string prop",
			"number": 7.0,
			"int":    float64(9),
			"geo": &models.GeoCoordinates{
				Latitude:  ptFloat32(30.2),
				Longitude: ptFloat32(60.2),
			},
			"text": "some text",
			"toTarget": models.MultipleRef{
				ref.SingleRef(),
			},
		}

		assert.Equal(t, expectedSchema, sch)
	})

	t.Run("add more references in rapid succession", func(t *testing.T) {
		// this test case prevents a regression on gh-1016
		source, err := crossref.ParseSource(fmt.Sprintf(
			"weaviate://localhost/MergeTestSource/%s/toTarget", sourceID))
		require.Nil(t, err)
		targets := []strfmt.UUID{target2, target3, target4}
		refs := make(objects.BatchReferences, len(targets))
		for i, target := range targets {
			to, err := crossref.Parse(fmt.Sprintf("weaviate://localhost/%s", target))
			require.Nil(t, err)
			refs[i] = objects.BatchReference{
				Err:  nil,
				From: source,
				To:   to,
			}
		}
		md := objects.MergeDocument{
			Class:      "MergeTestSource",
			ID:         sourceID,
			References: refs,
		}
		err = repo.Merge(context.Background(), md, nil, "")
		assert.Nil(t, err)
	})

	t.Run("check all references are now present", func(t *testing.T) {
		source, err := repo.ObjectByID(context.Background(), sourceID, nil, additional.Properties{}, "")
		require.Nil(t, err)

		refs := source.Object().Properties.(map[string]interface{})["toTarget"]
		refsSlice, ok := refs.(models.MultipleRef)
		require.True(t, ok, fmt.Sprintf("toTarget must be models.MultipleRef, but got %#v", refs))

		foundBeacons := []string{}
		for _, ref := range refsSlice {
			foundBeacons = append(foundBeacons, ref.Beacon.String())
		}
		expectedBeacons := []string{
			fmt.Sprintf("weaviate://localhost/%s", target1),
			fmt.Sprintf("weaviate://localhost/%s", target2),
			fmt.Sprintf("weaviate://localhost/%s", target3),
			fmt.Sprintf("weaviate://localhost/%s", target4),
		}

		assert.ElementsMatch(t, foundBeacons, expectedBeacons)
	})

	t.Run("merge object with no vector", func(t *testing.T) {
		err = repo.Merge(context.Background(), objects.MergeDocument{
			Class:           "MergeTestNoVector",
			ID:              noVecID,
			PrimitiveSchema: map[string]interface{}{"foo": "baz"},
		}, nil, "")
		require.Nil(t, err)

		orig, err := repo.ObjectByID(context.Background(), noVecID, nil, additional.Properties{}, "")
		require.Nil(t, err)

		expectedSchema := map[string]interface{}{
			"foo": "baz",
			"id":  noVecID,
		}

		assert.Equal(t, expectedSchema, orig.Schema)
	})
}

// This prevents a regression on
// https://github.com/weaviate/weaviate/issues/2193
//
// Prior to the fix it was possible that a prop that was not touched during the
// merge (and therefore only loaded from disk) failed during the
// inverted-indexing for the new doc id. This was then hidden by the fact that
// error handling was broken inside the inverted.Analyzer. This test tries to
// make sure that every possible property type stays intact if untouched
// during a Merge operation
//
// To achieve this, every prop in this class exists twice, once with the prefix
// 'touched_' and once with 'untouched_'. In the initial insert both properties
// contain the same value, but then during the patch merge, the 'touched_'
// properties are updated to a different value while the 'untouched_'
// properties are left untouched. Then we try to retrieve the object through a
// filter matching each property. The 'untouched_' properties are matched with
// the original value, the 'touched_' props are matched with the updated ones
func Test_Merge_UntouchedPropsCorrectlyIndexed(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		MaxImportGoroutinesFactor: 1,
		QueryMaximumResults:       10000,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)
	hnswConfig := enthnsw.NewDefaultUserConfig()
	hnswConfig.Skip = true
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "TestClass",
					VectorIndexConfig:   hnswConfig,
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{ // tries to have "one of each property type"
						{
							Name:         "untouched_string",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:         "touched_string",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:         "untouched_string_array",
							DataType:     schema.DataTypeTextArray.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:         "touched_string_array",
							DataType:     schema.DataTypeTextArray.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name: "untouched_text", Tokenization: "word",
							DataType: []string{"text"},
						},
						{
							Name: "touched_text", Tokenization: "word",
							DataType: []string{"text"},
						},
						{
							Name: "untouched_text_array", Tokenization: "word",
							DataType: []string{"text[]"},
						},
						{
							Name: "touched_text_array", Tokenization: "word",
							DataType: []string{"text[]"},
						},
						{Name: "untouched_number", DataType: []string{"number"}},
						{Name: "touched_number", DataType: []string{"number"}},
						{Name: "untouched_number_array", DataType: []string{"number[]"}},
						{Name: "touched_number_array", DataType: []string{"number[]"}},
						{Name: "untouched_int", DataType: []string{"int"}},
						{Name: "touched_int", DataType: []string{"int"}},
						{Name: "untouched_int_array", DataType: []string{"int[]"}},
						{Name: "touched_int_array", DataType: []string{"int[]"}},
						{Name: "untouched_date", DataType: []string{"date"}},
						{Name: "touched_date", DataType: []string{"date"}},
						{Name: "untouched_date_array", DataType: []string{"date[]"}},
						{Name: "touched_date_array", DataType: []string{"date[]"}},
						{Name: "untouched_geo", DataType: []string{"geoCoordinates"}},
						{Name: "touched_geo", DataType: []string{"geoCoordinates"}},
					},
				},
			},
		},
	}

	t.Run("add required classes", func(t *testing.T) {
		for _, class := range sch.Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
				require.Nil(t, err)
			})
		}
	})

	schemaGetter.schema = sch

	t.Run("add initial object", func(t *testing.T) {
		id := 0
		err := repo.PutObject(context.Background(), &models.Object{
			ID:    uuidFromInt(id),
			Class: "TestClass",
			Properties: map[string]interface{}{
				"untouched_number":       float64(id),
				"untouched_number_array": []interface{}{float64(id)},
				"untouched_int":          id,
				"untouched_int_array":    []interface{}{int64(id)},
				"untouched_string":       fmt.Sprintf("%d", id),
				"untouched_string_array": []string{fmt.Sprintf("%d", id)},
				"untouched_text":         fmt.Sprintf("%d", id),
				"untouched_text_array":   []string{fmt.Sprintf("%d", id)},
				"untouched_date":         time.Unix(0, 0).Add(time.Duration(id) * time.Hour),
				"untouched_date_array":   []time.Time{time.Unix(0, 0).Add(time.Duration(id) * time.Hour)},
				"untouched_geo": &models.GeoCoordinates{
					ptFloat32(float32(id)), ptFloat32(float32(id)),
				},

				"touched_number":       float64(id),
				"touched_number_array": []interface{}{float64(id)},
				"touched_int":          id,
				"touched_int_array":    []interface{}{int64(id)},
				"touched_string":       fmt.Sprintf("%d", id),
				"touched_string_array": []string{fmt.Sprintf("%d", id)},
				"touched_text":         fmt.Sprintf("%d", id),
				"touched_text_array":   []string{fmt.Sprintf("%d", id)},
				"touched_date":         time.Unix(0, 0).Add(time.Duration(id) * time.Hour),
				"touched_date_array":   []time.Time{time.Unix(0, 0).Add(time.Duration(id) * time.Hour)},
				"touched_geo": &models.GeoCoordinates{
					ptFloat32(float32(id)), ptFloat32(float32(id)),
				},
			},
			CreationTimeUnix:   int64(id),
			LastUpdateTimeUnix: int64(id),
		}, []float32{0.5}, nil)
		require.Nil(t, err)
	})

	t.Run("patch half the props (all that contain 'touched')", func(t *testing.T) {
		updateID := 28
		md := objects.MergeDocument{
			Class: "TestClass",
			ID:    uuidFromInt(0),
			PrimitiveSchema: map[string]interface{}{
				"touched_number":       float64(updateID),
				"touched_number_array": []interface{}{float64(updateID)},
				"touched_int":          updateID,
				"touched_int_array":    []interface{}{int64(updateID)},
				"touched_string":       fmt.Sprintf("%d", updateID),
				"touched_string_array": []string{fmt.Sprintf("%d", updateID)},
				"touched_text":         fmt.Sprintf("%d", updateID),
				"touched_text_array":   []string{fmt.Sprintf("%d", updateID)},
				"touched_date":         time.Unix(0, 0).Add(time.Duration(updateID) * time.Hour),
				"touched_date_array":   []time.Time{time.Unix(0, 0).Add(time.Duration(updateID) * time.Hour)},
				"touched_geo": &models.GeoCoordinates{
					ptFloat32(float32(updateID)), ptFloat32(float32(updateID)),
				},
			},
			References: nil,
		}
		err = repo.Merge(context.Background(), md, nil, "")
		assert.Nil(t, err)
	})

	t.Run("retrieve by each individual prop", func(t *testing.T) {
		retrieve := func(prefix string, id int) func(t *testing.T) {
			return func(t *testing.T) {
				type test struct {
					name   string
					filter *filters.LocalFilter
				}

				tests := []test{
					{
						name: "string filter",
						filter: buildFilter(
							fmt.Sprintf("%s_string", prefix),
							fmt.Sprintf("%d", id),
							eq,
							schema.DataTypeText),
					},
					{
						name: "string array filter",
						filter: buildFilter(
							fmt.Sprintf("%s_string_array", prefix),
							fmt.Sprintf("%d", id),
							eq,
							schema.DataTypeText),
					},
					{
						name: "text filter",
						filter: buildFilter(
							fmt.Sprintf("%s_text", prefix),
							fmt.Sprintf("%d", id),
							eq,
							dtText),
					},
					{
						name: "text array filter",
						filter: buildFilter(
							fmt.Sprintf("%s_text_array", prefix),
							fmt.Sprintf("%d", id),
							eq,
							dtText),
					},
					{
						name: "int filter",
						filter: buildFilter(
							fmt.Sprintf("%s_int", prefix), id, eq, dtInt),
					},
					{
						name: "int array filter",
						filter: buildFilter(
							fmt.Sprintf("%s_int_array", prefix), id, eq, dtInt),
					},
					{
						name: "number filter",
						filter: buildFilter(
							fmt.Sprintf("%s_number", prefix), float64(id), eq, dtNumber),
					},
					{
						name: "number array filter",
						filter: buildFilter(
							fmt.Sprintf("%s_number_array", prefix), float64(id), eq, dtNumber),
					},
					{
						name: "date filter",
						filter: buildFilter(
							fmt.Sprintf("%s_date", prefix),
							time.Unix(0, 0).Add(time.Duration(id)*time.Hour),
							eq, dtDate),
					},
					{
						name: "date array filter",
						filter: buildFilter(
							fmt.Sprintf("%s_date_array", prefix),
							time.Unix(0, 0).Add(time.Duration(id)*time.Hour),
							eq, dtDate),
					},
					{
						name: "geoFilter filter",
						filter: buildFilter(
							fmt.Sprintf("%s_geo", prefix),
							filters.GeoRange{
								GeoCoordinates: &models.GeoCoordinates{
									ptFloat32(float32(id)), ptFloat32(float32(id)),
								},
								Distance: 2,
							},
							wgr, dtGeoCoordinates),
					},
				}

				for _, tc := range tests {
					t.Run(tc.name, func(t *testing.T) {
						params := dto.GetParams{
							ClassName:  "TestClass",
							Pagination: &filters.Pagination{Limit: 5},
							Filters:    tc.filter,
						}
						res, err := repo.VectorSearch(context.Background(), params)
						require.Nil(t, err)
						require.Len(t, res, 1)

						// hard-code the only uuid
						assert.Equal(t, uuidFromInt(0), res[0].ID)
					})
				}
			}
		}
		t.Run("using untouched", retrieve("untouched", 0))
		t.Run("using touched", retrieve("touched", 28))
	})
}

func uuidFromInt(in int) strfmt.UUID {
	return strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", in)).String())
}
