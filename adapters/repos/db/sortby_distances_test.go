//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

func Test_SortBy_Distances(t *testing.T) {
	type testcase struct {
		testName          string
		givenObjects      []*storobj.Object
		givenDistances    []float32
		expectedOrder     []string
		expectedDistances []float32
	}

	tests := []testcase{
		{
			testName: "different distances - sorted by distance ascending",
			givenObjects: []*storobj.Object{
				{Object: models.Object{ID: strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b")}},
				{Object: models.Object{ID: strfmt.UUID("31bdf9ef-d1c0-4b43-8331-1a89a48c1d2b")}},
				{Object: models.Object{ID: strfmt.UUID("4432797a-ef18-429f-83dc-d971dd9e4dd0")}},
				{Object: models.Object{ID: strfmt.UUID("8ef8c6fd-93b5-4452-b3c3-cef1cd0a18ed")}},
				{Object: models.Object{ID: strfmt.UUID("d79f0d2d-ebc5-4dad-b3df-323bc1e6f183")}},
			},
			givenDistances: []float32{0.5, 0.1, 0.9, 0.3, 0.7},
			expectedOrder: []string{
				"31bdf9ef-d1c0-4b43-8331-1a89a48c1d2b", // 0.1
				"8ef8c6fd-93b5-4452-b3c3-cef1cd0a18ed", // 0.3
				"40d3be3e-2ecc-49c8-b37c-d8983164848b", // 0.5
				"d79f0d2d-ebc5-4dad-b3df-323bc1e6f183", // 0.7
				"4432797a-ef18-429f-83dc-d971dd9e4dd0", // 0.9
			},
			expectedDistances: []float32{0.1, 0.3, 0.5, 0.7, 0.9},
		},
		{
			testName: "identical distances - sorted by ID ascending (deterministic tie-breaker)",
			givenObjects: []*storobj.Object{
				{Object: models.Object{ID: strfmt.UUID("dddddddd-0000-0000-0000-000000000000")}},
				{Object: models.Object{ID: strfmt.UUID("bbbbbbbb-0000-0000-0000-000000000000")}},
				{Object: models.Object{ID: strfmt.UUID("cccccccc-0000-0000-0000-000000000000")}},
				{Object: models.Object{ID: strfmt.UUID("aaaaaaaa-0000-0000-0000-000000000000")}},
			},
			givenDistances: []float32{0.5, 0.5, 0.5, 0.5},
			expectedOrder: []string{
				"aaaaaaaa-0000-0000-0000-000000000000",
				"bbbbbbbb-0000-0000-0000-000000000000",
				"cccccccc-0000-0000-0000-000000000000",
				"dddddddd-0000-0000-0000-000000000000",
			},
			expectedDistances: []float32{0.5, 0.5, 0.5, 0.5},
		},
		{
			testName: "mixed equal and different distances - distance first, then ID",
			givenObjects: []*storobj.Object{
				{Object: models.Object{ID: strfmt.UUID("cccccccc-0000-0000-0000-000000000000")}},
				{Object: models.Object{ID: strfmt.UUID("aaaaaaaa-0000-0000-0000-000000000000")}},
				{Object: models.Object{ID: strfmt.UUID("dddddddd-0000-0000-0000-000000000000")}},
				{Object: models.Object{ID: strfmt.UUID("bbbbbbbb-0000-0000-0000-000000000000")}},
				{Object: models.Object{ID: strfmt.UUID("eeeeeeee-0000-0000-0000-000000000000")}},
			},
			givenDistances: []float32{0.3, 0.3, 0.1, 0.3, 0.5},
			expectedOrder: []string{
				"dddddddd-0000-0000-0000-000000000000", // 0.1 (smallest distance)
				"aaaaaaaa-0000-0000-0000-000000000000", // 0.3 (tie, smallest ID)
				"bbbbbbbb-0000-0000-0000-000000000000", // 0.3 (tie, second smallest ID)
				"cccccccc-0000-0000-0000-000000000000", // 0.3 (tie, third smallest ID)
				"eeeeeeee-0000-0000-0000-000000000000", // 0.5 (largest distance)
			},
			expectedDistances: []float32{0.1, 0.3, 0.3, 0.3, 0.5},
		},
		{
			testName: "single result",
			givenObjects: []*storobj.Object{
				{Object: models.Object{ID: strfmt.UUID("4a483f11-7b2f-452b-be49-f7844dbc5693")}},
			},
			givenDistances: []float32{0.42},
			expectedOrder: []string{
				"4a483f11-7b2f-452b-be49-f7844dbc5693",
			},
			expectedDistances: []float32{0.42},
		},
		{
			testName:          "empty results",
			givenObjects:      []*storobj.Object{},
			givenDistances:    []float32{},
			expectedOrder:     []string{},
			expectedDistances: []float32{},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			objects, distances := newDistancesSorter().sort(test.givenObjects, test.givenDistances)

			assert.Equal(t, len(test.expectedOrder), len(objects), "object count mismatch")
			assert.Equal(t, len(test.expectedDistances), len(distances), "distance count mismatch")

			for i := range objects {
				assert.Equal(t, test.expectedOrder[i], objects[i].ID().String(), "ID mismatch at position %d", i)
				assert.Equal(t, test.expectedDistances[i], distances[i], "distance mismatch at position %d", i)
			}
		})
	}
}

func Test_SortBy_Distances_Determinism(t *testing.T) {
	// Run the same sort multiple times to ensure deterministic results
	objects := []*storobj.Object{
		{Object: models.Object{ID: strfmt.UUID("cccccccc-0000-0000-0000-000000000000")}},
		{Object: models.Object{ID: strfmt.UUID("aaaaaaaa-0000-0000-0000-000000000000")}},
		{Object: models.Object{ID: strfmt.UUID("bbbbbbbb-0000-0000-0000-000000000000")}},
	}
	distances := []float32{0.5, 0.5, 0.5}

	expectedOrder := []string{
		"aaaaaaaa-0000-0000-0000-000000000000",
		"bbbbbbbb-0000-0000-0000-000000000000",
		"cccccccc-0000-0000-0000-000000000000",
	}

	// Run 100 times to verify determinism
	for i := 0; i < 100; i++ {
		// Create fresh copies for each iteration
		objsCopy := make([]*storobj.Object, len(objects))
		distsCopy := make([]float32, len(distances))
		for j := range objects {
			objsCopy[j] = &storobj.Object{Object: models.Object{ID: objects[j].Object.ID}}
			distsCopy[j] = distances[j]
		}

		sorted, _ := newDistancesSorter().sort(objsCopy, distsCopy)

		for j := range sorted {
			assert.Equal(t, expectedOrder[j], sorted[j].ID().String(),
				"iteration %d: expected deterministic order, got different result at position %d", i, j)
		}
	}
}
