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

package grouper

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/schema/crossref"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

func TestGrouper_ModeClosest(t *testing.T) {
	in := []search.Result{
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name": "A1",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name": "A2",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.93},
			Schema: map[string]interface{}{
				"name": "A3",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.93, 0.1},
			Schema: map[string]interface{}{
				"name": "B2",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.92, 0.1},
			Schema: map[string]interface{}{
				"name": "B3",
			},
		},
	}

	expectedOut := []search.Result{
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name": "A1",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
	}

	log, _ := test.NewNullLogger()
	res, err := New(log).Group(in, "closest", 0.2)
	require.Nil(t, err)
	assert.Equal(t, expectedOut, res)
	for i := range res {
		assert.Equal(t, expectedOut[i].ClassName, res[i].ClassName)
	}
}

func TestGrouper_ModeMerge(t *testing.T) {
	in := []search.Result{
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name":    "A1",
				"count":   10.0,
				"illegal": true,
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(20),
					Longitude: ptFloat32(20),
				},
				"relatedTo": []interface{}{
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  strfmt.UUID("1"),
							"foo": "bar1",
						},
					},
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  strfmt.UUID("2"),
							"foo": "bar2",
						},
					},
				},
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name":    "A2",
				"count":   11.0,
				"illegal": true,
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name":    "A2",
				"count":   11.0,
				"illegal": true,
				"relatedTo": []interface{}{
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  strfmt.UUID("3"),
							"foo": "bar3",
						},
					},
				},
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.93},
			Schema: map[string]interface{}{
				"name":    "A3",
				"count":   12.0,
				"illegal": false,
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(22),
					Longitude: ptFloat32(18),
				},
				"relatedTo": []interface{}{
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  strfmt.UUID("2"),
							"foo": "bar2",
						},
					},
				},
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.93, 0.1},
			Schema: map[string]interface{}{
				"name": "B2",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.92, 0.1},
			Schema: map[string]interface{}{
				"name": "B3",
			},
		},
	}

	expectedOut := []search.Result{
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.95750004}, // centroid position of all inputs
			Schema: map[string]interface{}{
				"name":    "A1 (A2, A3)", // note that A2 is only contained once, even though its twice in the input set
				"count":   11.0,          // mean of all inputs
				"illegal": true,          // the most common input value, with a bias towards true on equal count
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(21),
					Longitude: ptFloat32(19),
				},
				"relatedTo": []interface{}{
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  strfmt.UUID("1"),
							"foo": "bar1",
						},
					},
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  strfmt.UUID("2"),
							"foo": "bar2",
						},
					},
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  strfmt.UUID("3"),
							"foo": "bar3",
						},
					},
				},
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.9433334, 0.1},
			Schema: map[string]interface{}{
				"name": "B1 (B2, B3)",
			},
		},
	}

	log, _ := test.NewNullLogger()
	res, err := New(log).Group(in, "merge", 0.2)
	require.Nil(t, err)
	assert.Equal(t, expectedOut, res)
	for i := range res {
		assert.Equal(t, expectedOut[i].ClassName, res[i].ClassName)
	}
}

// Since reference properties can be represented both as models.MultipleRef
// and []interface{}, we need to test for both cases. TestGrouper_ModeMerge
// above tests the case of []interface{}, so this test handles the other case.
// see https://github.com/weaviate/weaviate/pull/2320 for more info
func Test_Grouper_ModeMerge_MultipleRef(t *testing.T) {
	in := []search.Result{
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name":    "A1",
				"count":   10.0,
				"illegal": true,
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(20),
					Longitude: ptFloat32(20),
				},
				"relatedTo": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(crossref.NewLocalhost("Foo", "3dc4417d-1508-4914-9929-8add49684b9f").String()),
						Class:  "Foo",
					},
					&models.SingleRef{
						Beacon: strfmt.URI(crossref.NewLocalhost("Foo", "f1d6df98-33a7-40bb-bcb4-57c1f35d31ab").String()),
						Class:  "Foo",
					},
				},
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name":    "A2",
				"count":   11.0,
				"illegal": true,
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name":    "A2",
				"count":   11.0,
				"illegal": true,
				"relatedTo": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(crossref.NewLocalhost("Foo", "f280a7f7-7fab-46ed-b895-1490512660ae").String()),
						Class:  "Foo",
					},
				},
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.93},
			Schema: map[string]interface{}{
				"name":    "A3",
				"count":   12.0,
				"illegal": false,
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(22),
					Longitude: ptFloat32(18),
				},
				"relatedTo": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(crossref.NewLocalhost("Foo", "f1d6df98-33a7-40bb-bcb4-57c1f35d31ab").String()),
						Class:  "Foo",
					},
				},
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.93, 0.1},
			Schema: map[string]interface{}{
				"name": "B2",
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.92, 0.1},
			Schema: map[string]interface{}{
				"name": "B3",
			},
		},
	}

	expectedOut := []search.Result{
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.95750004}, // centroid position of all inputs
			Schema: map[string]interface{}{
				"name":    "A1 (A2, A3)", // note that A2 is only contained once, even though its twice in the input set
				"count":   11.0,          // mean of all inputs
				"illegal": true,          // the most common input value, with a bias towards true on equal count
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(21),
					Longitude: ptFloat32(19),
				},
				"relatedTo": []interface{}{
					&models.SingleRef{
						Beacon: strfmt.URI(crossref.NewLocalhost("Foo", "3dc4417d-1508-4914-9929-8add49684b9f").String()),
						Class:  "Foo",
					},
					&models.SingleRef{
						Beacon: strfmt.URI(crossref.NewLocalhost("Foo", "f1d6df98-33a7-40bb-bcb4-57c1f35d31ab").String()),
						Class:  "Foo",
					},
					&models.SingleRef{
						Beacon: strfmt.URI(crossref.NewLocalhost("Foo", "f280a7f7-7fab-46ed-b895-1490512660ae").String()),
						Class:  "Foo",
					},
				},
			},
		},
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.9433334, 0.1},
			Schema: map[string]interface{}{
				"name": "B1 (B2, B3)",
			},
		},
	}

	log, _ := test.NewNullLogger()
	res, err := New(log).Group(in, "merge", 0.2)
	require.Nil(t, err)
	assert.Equal(t, expectedOut, res)
	for i := range res {
		assert.Equal(t, expectedOut[i].ClassName, res[i].ClassName)
	}
}

func TestGrouper_ModeMergeFailWithIDTypeOtherThenUUID(t *testing.T) {
	in := []search.Result{
		{
			ClassName: "Foo",
			Vector:    []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name":    "A1",
				"count":   10.0,
				"illegal": true,
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(20),
					Longitude: ptFloat32(20),
				},
				"relatedTo": []interface{}{
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  "1",
							"foo": "bar1",
						},
					},
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"id":  "2",
							"foo": "bar2",
						},
					},
				},
			},
		},
	}

	log, _ := test.NewNullLogger()
	res, err := New(log).Group(in, "merge", 0.2)
	require.NotNil(t, err)
	assert.Nil(t, res)
	assert.EqualError(t, err,
		"group 0: merge values: prop 'relatedTo': element 0: "+
			"found a search.LocalRef, 'id' field type expected to be strfmt.UUID but got string")
}
