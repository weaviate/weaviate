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

package test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
)

const (
	envGcsEndpoint            = "GCS_ENDPOINT"
	envGcsStorageEmulatorHost = "STORAGE_EMULATOR_HOST"
	envGcsCredentials         = "GOOGLE_APPLICATION_CREDENTIALS"
	envGcsProjectID           = "GOOGLE_CLOUD_PROJECT"
	envGcsBucket              = "STORAGE_GCS_BUCKET"
)

func addTestClass(t *testing.T, className string) {
	class := &models.Class{
		Class: className,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "contents",
				DataType: []string{"string"},
			},
		},
	}

	createClass(t, class)
}

func addTestData(t *testing.T, className string) {
	const (
		noteLengthMin = 4
		noteLengthMax = 1024

		batchSize  = 10
		numBatches = 50
	)

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numBatches; i++ {
		batch := make([]*models.Object, batchSize)
		for j := 0; j < batchSize; j++ {
			contentsLength := noteLengthMin + seededRand.Intn(noteLengthMax-noteLengthMin+1)
			contents := helper.GetRandomString(contentsLength)

			batch[j] = &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"contents": contents},
			}
		}

		createObjectsBatch(t, batch)
	}
}
