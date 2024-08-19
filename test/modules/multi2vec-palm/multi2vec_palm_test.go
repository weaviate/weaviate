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

package multi2vec_palm_tests

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func testMulti2VecPaLM(host, gcpProject, location string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Helper methods
		// get image and video blob fns
		getBlob := func(path string) (string, error) {
			f, err := os.Open(path)
			if err != nil {
				return "", err
			}
			content, err := io.ReadAll(f)
			if err != nil {
				return "", err
			}
			return base64.StdEncoding.EncodeToString(content), nil
		}
		getImageBlob := func(i int) (string, error) {
			path := fmt.Sprintf("./data/images/%v.jpg", i)
			return getBlob(path)
		}
		getVideoBlob := func(i int) (string, error) {
			path := fmt.Sprintf("./data/videos/%v.mp4", i)
			return getBlob(path)
		}
		// query test helper
		testQuery := func(t *testing.T,
			className, nearMediaArgument, titleProperty, titlePropertyValue string,
			targetVectors map[string]int,
		) {
			var targetVectorsList []string
			for targetVector := range targetVectors {
				targetVectorsList = append(targetVectorsList, targetVector)
			}
			query := fmt.Sprintf(`
			{
				Get {
					%s(
						%s
					){
						%s
						_additional {
							certainty
							vectors {%s}
						}
					}
				}
			}
		`, className, nearMediaArgument, titleProperty, strings.Join(targetVectorsList, ","))

			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			objs := result.Get("Get", className).AsSlice()
			require.Len(t, objs, 2)
			title := objs[0].(map[string]interface{})[titleProperty]
			assert.Equal(t, titlePropertyValue, title)
			additional, ok := objs[0].(map[string]interface{})["_additional"].(map[string]interface{})
			require.True(t, ok)
			certainty := additional["certainty"].(json.Number)
			assert.NotNil(t, certainty)
			certaintyValue, err := certainty.Float64()
			require.NoError(t, err)
			assert.Greater(t, certaintyValue, 0.0)
			assert.GreaterOrEqual(t, certaintyValue, 0.9)
			vectors, ok := additional["vectors"].(map[string]interface{})
			require.True(t, ok)

			targetVectorsMap := make(map[string][]float32)
			for targetVector := range targetVectors {
				vector, ok := vectors[targetVector].([]interface{})
				require.True(t, ok)

				vec := make([]float32, len(vector))
				for i := range vector {
					val, err := vector[i].(json.Number).Float64()
					require.NoError(t, err)
					vec[i] = float32(val)
				}

				targetVectorsMap[targetVector] = vec
			}
			for targetVector, targetVectorDimensions := range targetVectors {
				require.Len(t, targetVectorsMap[targetVector], targetVectorDimensions)
			}
		}
		// Define class
		className := "PaLMClipTest"
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name: "image_title", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "image_description", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "video_title", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "video_description", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "image", DataType: []string{schema.DataTypeBlob.String()},
				},
				{
					Name: "video", DataType: []string{schema.DataTypeBlob.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"clip_palm": {
					Vectorizer: map[string]interface{}{
						"multi2vec-palm": map[string]interface{}{
							"imageFields":        []interface{}{"image"},
							"vectorizeClassName": false,
							"location":           location,
							"projectId":          gcpProject,
						},
					},
					VectorIndexType: "flat",
				},
				"clip_palm_128": {
					Vectorizer: map[string]interface{}{
						"multi2vec-palm": map[string]interface{}{
							"imageFields":        []interface{}{"image"},
							"vectorizeClassName": false,
							"location":           location,
							"projectId":          gcpProject,
							"dimensions":         128,
						},
					},
					VectorIndexType: "flat",
				},
				"clip_palm_256": {
					Vectorizer: map[string]interface{}{
						"multi2vec-palm": map[string]interface{}{
							"imageFields":        []interface{}{"image"},
							"vectorizeClassName": false,
							"location":           location,
							"projectId":          gcpProject,
							"dimensions":         256,
						},
					},
					VectorIndexType: "flat",
				},
				"clip_palm_video": {
					Vectorizer: map[string]interface{}{
						"multi2vec-palm": map[string]interface{}{
							"videoFields":        []interface{}{"video"},
							"vectorizeClassName": false,
							"location":           location,
							"projectId":          gcpProject,
						},
					},
					VectorIndexType: "flat",
				},
				"clip_palm_weights": {
					Vectorizer: map[string]interface{}{
						"multi2vec-palm": map[string]interface{}{
							"textFields":  []interface{}{"image_title", "image_description"},
							"imageFields": []interface{}{"image"},
							"weights": map[string]interface{}{
								"textFields":  []interface{}{0.05, 0.05},
								"imageFields": []interface{}{0.9},
							},
							"vectorizeClassName": false,
							"location":           location,
							"projectId":          gcpProject,
							"dimensions":         512,
						},
					},
					VectorIndexType: "flat",
				},
			},
		}
		// create schema
		helper.CreateClass(t, class)
		defer helper.DeleteClass(t, class.Class)

		t.Run("import data", func(t *testing.T) {
			f, err := os.Open("./data/data.csv")
			require.NoError(t, err)
			defer f.Close()
			var objs []*models.Object
			i := 0
			csvReader := csv.NewReader(f)
			for {
				line, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				if i > 0 {
					id := line[1]
					imageTitle := line[2]
					imageDescription := line[3]
					imageBlob, err := getImageBlob(i)
					require.NoError(t, err)
					videoTitle := line[4]
					videoDescription := line[5]
					videoBlob, err := getVideoBlob(i)
					require.NoError(t, err)
					obj := &models.Object{
						Class: class.Class,
						ID:    strfmt.UUID(id),
						Properties: map[string]interface{}{
							"image_title":       imageTitle,
							"image_description": imageDescription,
							"image":             imageBlob,
							"video_title":       videoTitle,
							"video_description": videoDescription,
							"video":             videoBlob,
						},
					}
					objs = append(objs, obj)
				}
				i++
			}
			for _, obj := range objs {
				helper.CreateObject(t, obj)
				helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
			}
		})

		t.Run("nearImage", func(t *testing.T) {
			blob, err := getImageBlob(2)
			require.NoError(t, err)
			targetVector := "clip_palm"
			nearMediaArgument := fmt.Sprintf(`
				nearImage: {
					image: "%s"
					targetVectors: ["%s"]
				}
			`, blob, targetVector)
			titleProperty := "image_title"
			titlePropertyValue := "waterfalls"
			targetVectors := map[string]int{
				"clip_palm":         1408,
				"clip_palm_128":     128,
				"clip_palm_256":     256,
				"clip_palm_video":   1408,
				"clip_palm_weights": 512,
			}
			testQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})

		t.Run("nearVideo", func(t *testing.T) {
			blob, err := getVideoBlob(2)
			require.NoError(t, err)
			targetVector := "clip_palm_video"
			nearMediaArgument := fmt.Sprintf(`
				nearVideo: {
					video: "%s"
					targetVectors: ["%s"]
				}
			`, blob, targetVector)
			titleProperty := "video_title"
			titlePropertyValue := "dog"
			targetVectors := map[string]int{
				"clip_palm":         1408,
				"clip_palm_128":     128,
				"clip_palm_256":     256,
				"clip_palm_video":   1408,
				"clip_palm_weights": 512,
			}
			testQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})
	}
}
