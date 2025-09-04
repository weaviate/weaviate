//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package multimodal

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const DefaultTimeout = 2 * time.Minute

const (
	PropertyImageTitle       = "image_title"
	PropertyImageDescription = "image_description"
	PropertyImage            = "image"
	PropertyVideoTitle       = "video_title"
	PropertyVideoDescription = "video_description"
	PropertyVideo            = "video"
)

func BaseClass(className string, withVideo bool) *models.Class {
	properties := []*models.Property{
		{
			Name: PropertyImageTitle, DataType: []string{schema.DataTypeText.String()},
		},
		{
			Name: PropertyImageDescription, DataType: []string{schema.DataTypeText.String()},
		},
		{
			Name: PropertyImage, DataType: []string{schema.DataTypeBlob.String()},
		},
	}
	if withVideo {
		videoProperties := []*models.Property{
			{
				Name: PropertyVideoTitle, DataType: []string{schema.DataTypeText.String()},
			},
			{
				Name: PropertyVideoDescription, DataType: []string{schema.DataTypeText.String()},
			},
			{
				Name: PropertyVideo, DataType: []string{schema.DataTypeBlob.String()},
			},
		}
		properties = append(properties, videoProperties...)
	}
	return &models.Class{
		Class:      className,
		Properties: properties,
	}
}

func InsertObjects(t *testing.T, dataFolderPath, className string, withVideo bool) {
	f, err := GetCSV(dataFolderPath)
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
			imageBlob, err := GetImageBlob(dataFolderPath, i)
			require.NoError(t, err)
			properties := map[string]interface{}{
				PropertyImageTitle:       imageTitle,
				PropertyImageDescription: imageDescription,
				PropertyImage:            imageBlob,
			}
			if withVideo {
				videoTitle := line[4]
				videoDescription := line[5]
				videoBlob, err := GetVideoBlob(dataFolderPath, i)
				require.NoError(t, err)
				properties[PropertyVideoTitle] = videoTitle
				properties[PropertyVideoDescription] = videoDescription
				properties[PropertyVideo] = videoBlob
			}

			obj := &models.Object{
				Class:      className,
				ID:         strfmt.UUID(id),
				Properties: properties,
			}
			objs = append(objs, obj)
		}
		i++
	}
	for _, obj := range objs {
		err := helper.CreateObjectWithTimeout(t, obj, DefaultTimeout)
		require.NoError(t, err)
		obj := helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
		require.NotNil(t, obj)
	}
}

func GetIDs(t *testing.T, dataFolderPath string) []string {
	f, err := GetCSV(dataFolderPath)
	require.NoError(t, err)
	defer f.Close()
	var ids []string
	i := 0
	csvReader := csv.NewReader(f)
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if i > 0 {
			ids = append(ids, line[1])
		}
		i++
	}
	return ids
}

func CheckObjects(t *testing.T, dataFolderPath, className string, vectors, multivectors []string) {
	for _, id := range GetIDs(t, dataFolderPath) {
		t.Run(id, func(t *testing.T) {
			obj, err := helper.GetObject(t, className, strfmt.UUID(id), "vector")
			require.NoError(t, err)
			require.NotNil(t, obj)
			require.GreaterOrEqual(t, len(obj.Vectors), len(vectors)+len(multivectors))
			for _, vec := range vectors {
				require.IsType(t, []float32{}, obj.Vectors[vec])
				require.True(t, len(obj.Vectors[vec].([]float32)) > 0)
			}
			for _, multivec := range multivectors {
				require.IsType(t, [][]float32{}, obj.Vectors[multivec])
				require.True(t, len(obj.Vectors[multivec].([][]float32)) > 0)
			}
		})
	}
}

func GetImageBlob(dataFolderPath string, i int) (string, error) {
	path := fmt.Sprintf("%s/images/%v.jpg", dataFolderPath, i)
	return helper.GetBase64EncodedData(path)
}

func GetVideoBlob(dataFolderPath string, i int) (string, error) {
	path := fmt.Sprintf("%s/videos/%v.mp4", dataFolderPath, i)
	return helper.GetBase64EncodedData(path)
}

func GetCSV(dataFolderPath string) (*os.File, error) {
	return os.Open(fmt.Sprintf("%s/data.csv", dataFolderPath))
}

// query test helper
func TestQuery(t *testing.T,
	className, nearMediaArgument, titleProperty, titlePropertyValue string,
	targetVectors map[string]int,
) {
	var targetVectorsList []string
	for targetVector := range targetVectors {
		targetVectorsList = append(targetVectorsList, targetVector)
	}
	additionalVectors := ""
	if len(targetVectorsList) > 0 {
		additionalVectors = fmt.Sprintf("vectors {%s}", strings.Join(targetVectorsList, ","))
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
							%s
						}
					}
				}
			}
		`, className, nearMediaArgument, titleProperty, additionalVectors)

	result := graphqlhelper.AssertGraphQLWithTimeout(t, helper.RootAuth, DefaultTimeout, query)
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
	assert.GreaterOrEqual(t, certaintyValue, 0.6)
	if len(targetVectorsList) > 0 {
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
}
