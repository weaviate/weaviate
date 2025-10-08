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

package test_suits

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
)

func generateRandomVector(dimensionality int) []float32 {
	if dimensionality <= 0 {
		return nil
	}

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	slice := make([]float32, dimensionality)
	for i := range slice {
		slice[i] = r.Float32()
	}
	return slice
}

func generateRandomMultiVector(dimensionality, len int) [][]float32 {
	multiVector := make([][]float32, len)
	for i := range len {
		multiVector[i] = generateRandomVector(dimensionality)
	}
	return multiVector
}

func batchInsertObjects(t *testing.T, client *wvt.Client, objs []*models.Object) {
	resp, err := client.Batch().ObjectsBatcher().
		WithObjects(objs...).
		Do(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, resp)
	for _, r := range resp {
		require.NotNil(t, r.Result)
		require.NotNil(t, r.Result.Status)
		assert.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *r.Result.Status)
	}
}
