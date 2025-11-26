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

package batch

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func TestVectorizeBatchObjects(t *testing.T) {
	type testCase struct {
		name       string
		objs       []*models.Object
		skipObject []bool
		batchSize  int
		errs       map[int]error
		vectors    [][]float32
	}
	tests := []testCase{
		{
			name:       "2 objects, dont't skip, batch size 1",
			objs:       generateObjects(2),
			skipObject: generateSkipObjects(2),
			batchSize:  1,
		},
		{
			name:       "2 objects, dont't skip, batch size 1",
			objs:       generateObjects(2),
			skipObject: []bool{true, false},
			batchSize:  1,
		},
		{
			name:       "5 objects, skip every second, batch size 4",
			objs:       generateObjects(5),
			skipObject: []bool{true, false, true, false, false},
			batchSize:  4,
		},
		{
			name:       "5 objects, skip every second, batch size 4",
			objs:       generateObjects(5),
			skipObject: []bool{true, false, true, true, true},
			batchSize:  4,
		},
		{
			name:       "5 objects, skip every second, batch size 4",
			objs:       generateObjects(5),
			skipObject: generateSkipObjects(5),
			batchSize:  4,
		},
		{
			name:       "9 objects, skip every second, batch size 3",
			objs:       generateObjects(9),
			skipObject: []bool{true, false, true, false, false, true, false, true, false},
			batchSize:  3,
		},
		{
			name:       "10 objects, skip every second, batch size 3",
			objs:       generateObjects(10),
			skipObject: []bool{true, false, true, false, false, true, false, true, false, false},
			batchSize:  3,
		},
		{
			name:       "11 objects, skip every second, batch size 3",
			objs:       generateObjects(11),
			skipObject: []bool{true, false, true, false, false, true, false, true, false, false, false},
			batchSize:  3,
		},
		{
			name:       "100 objects, batch size 5",
			objs:       generateObjects(100),
			skipObject: generateSkipObjects(100),
			batchSize:  5,
		},
		{
			name:       "10 objects, check if the returned vectors and exactly the same as we expect",
			objs:       generateObjects(10),
			skipObject: generateSkipObjects(10),
			batchSize:  3,
			vectors:    [][]float32{{1.0}, {2.0}, {3.0}, {4.0}, {5.0}, {6.0}, {7.0}, {8.0}, {9.0}, {10.0}},
		},
	}
	// add 10 random tests
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range 10 {
		objsCount := r.Intn(500)
		batchSize := r.Intn(15)
		tests = append(tests, testCase{
			name:       fmt.Sprintf("random test: %v, objects: %v, batchSize: %v", i, objsCount, batchSize),
			objs:       generateObjects(objsCount),
			skipObject: randomSkipObjects(objsCount),
			batchSize:  batchSize,
		})
	}
	// tests with errors
	errorRequested := fmt.Errorf("error requested")
	testsWithErrors := []testCase{
		{
			name:       "10 objects, batch size 5, second batch errors",
			objs:       generateObjectsWithErrorAt(10, 7),
			skipObject: generateSkipObjects(10),
			batchSize:  5,
			errs: map[int]error{
				5: errorRequested,
				6: errorRequested,
				7: errorRequested,
				8: errorRequested,
				9: errorRequested,
			},
		},
		{
			name:       "10 objects, batch size 5, second batch errors, some elements are skipped",
			objs:       generateObjectsWithErrorAt(10, 8),
			skipObject: []bool{true, false, false, false, false, true, false, true, false, false},
			batchSize:  5,
			errs: map[int]error{
				8: errorRequested,
				9: errorRequested,
			},
		},
		{
			name:       "10 objects, batch size 4, second and third batch errors",
			objs:       generateObjectsWithErrorAt(10, 7, 9),
			skipObject: generateSkipObjects(10),
			batchSize:  4,
			errs: map[int]error{
				4: errorRequested,
				5: errorRequested,
				6: errorRequested,
				7: errorRequested,
				8: errorRequested,
				9: errorRequested,
			},
		},
		{
			name:       "10 objects, batch size 4, first and third batch errors",
			objs:       generateObjectsWithErrorAt(10, 0, 7, 9),
			skipObject: []bool{false, false, true, false, false, true, false, true, false, false},
			batchSize:  3,
			errs: map[int]error{
				0: errorRequested,
				1: errorRequested,
				3: errorRequested,
				9: errorRequested,
			},
		},
	}
	tests = append(tests, testsWithErrors...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, _ := test.NewNullLogger()
			vectorizer := fakeBatchObjectsVectorizer{tt.vectors}
			res, _, errs := VectorizeBatchObjects(context.Background(), tt.objs, tt.skipObject, nil, l, vectorizer.Objects, tt.batchSize)
			if tt.errs != nil {
				require.Equal(t, len(tt.errs), len(errs))
				for index := range tt.errs {
					err, ok := errs[index]
					require.True(t, ok)
					assert.EqualError(t, err, tt.errs[index].Error())
				}
			} else {
				require.Empty(t, errs)
			}
			for i, skipObject := range tt.skipObject {
				if _, errorExists := errs[i]; !errorExists {
					if skipObject {
						assert.Empty(t, res[i])
					} else {
						assert.NotEmpty(t, res[i])
					}
				} else {
					assert.Empty(t, res[i])
				}
			}
			for i, vec := range tt.vectors {
				assert.Equal(t, vec, res[i])
			}
		})
	}
}

type fakeBatchObjectsVectorizer struct {
	vectors [][]float32
}

func (f *fakeBatchObjectsVectorizer) Objects(ctx context.Context, objs []*models.Object, cfg moduletools.ClassConfig) ([][]float32, models.AdditionalProperties, error) {
	vectors := make([][]float32, len(objs))
	for i := range objs {
		if objs[i].ID.String() == "error" {
			return vectors, nil, fmt.Errorf("error requested")
		}
		if len(f.vectors) > 0 {
			if vectorIndex, err := strconv.Atoi(objs[i].ID.String()); err == nil {
				vectors[i] = f.vectors[vectorIndex]
			}
		} else {
			vectors[i] = randomFloat32Slice(4)
		}
	}
	return vectors, nil, nil
}

func generateObjects(n int) []*models.Object {
	return generateObjectsWithErrorAt(n, -1)
}

func generateObjectsWithErrorAt(n int, errorAt ...int) []*models.Object {
	objs := make([]*models.Object, n)
	for i := range objs {
		id := strfmt.UUID(fmt.Sprintf("%v", i))
		if slices.Contains(errorAt, i) {
			id = "error"
		}
		objs[i] = &models.Object{ID: id}
	}
	return objs
}

func generateSkipObjects(n int) []bool {
	skipObjects := make([]bool, n)
	return skipObjects
}

func randomSkipObjects(n int) []bool {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]bool, n)
	for i := range b {
		b[i] = r.Intn(2) == 1
	}
	return b
}

func randomFloat32Slice(dimensions int) []float32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	min := float32(-10.0)
	max := float32(10.0)
	out := make([]float32, dimensions)
	for i := range out {
		out[i] = min + r.Float32()*(max-min)
	}
	return out
}
