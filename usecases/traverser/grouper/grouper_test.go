//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package grouper

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGrouper_ModeClosest(t *testing.T) {
	in := []search.Result{
		search.Result{
			Vector: []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name": "A1",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name": "A2",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.93},
			Schema: map[string]interface{}{
				"name": "A3",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.93, 0.1},
			Schema: map[string]interface{}{
				"name": "B2",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.92, 0.1},
			Schema: map[string]interface{}{
				"name": "B3",
			},
		},
	}

	expectedOut := []search.Result{
		search.Result{
			Vector: []float32{0.1, 0.1, 0.98},
			Schema: map[string]interface{}{
				"name": "A1",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
	}

	log, _ := test.NewNullLogger()
	res, err := New(log).Group(in, "closest", 0.2)
	require.Nil(t, err)
	assert.Equal(t, expectedOut, res)
}

func TestGrouper_ModeMerge(t *testing.T) {
	in := []search.Result{
		search.Result{
			Vector: []float32{0.1, 0.1, 0.98},
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
							"uuid": "1",
							"foo":  "bar1",
						},
					},
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"uuid": "2",
							"foo":  "bar2",
						},
					},
				},
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name":    "A2",
				"count":   11.0,
				"illegal": true,
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.96},
			Schema: map[string]interface{}{
				"name":    "A2",
				"count":   11.0,
				"illegal": true,
				"relatedTo": []interface{}{
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"uuid": "3",
							"foo":  "bar3",
						},
					},
				},
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.1, 0.93},
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
							"uuid": "2",
							"foo":  "bar2",
						},
					},
				},
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.98, 0.1},
			Schema: map[string]interface{}{
				"name": "B1",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.93, 0.1},
			Schema: map[string]interface{}{
				"name": "B2",
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.92, 0.1},
			Schema: map[string]interface{}{
				"name": "B3",
			},
		},
	}

	expectedOut := []search.Result{
		search.Result{
			Vector: []float32{0.1, 0.1, 0.95750004}, // centroid position of all inputs
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
							"uuid": "1",
							"foo":  "bar1",
						},
					},
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"uuid": "2",
							"foo":  "bar2",
						},
					},
					search.LocalRef{
						Class: "Foo",
						Fields: map[string]interface{}{
							"uuid": "3",
							"foo":  "bar3",
						},
					},
				},
			},
		},
		search.Result{
			Vector: []float32{0.1, 0.9433334, 0.1},
			Schema: map[string]interface{}{
				"name": "B1 (B2, B3)",
			},
		},
	}

	log, _ := test.NewNullLogger()
	res, err := New(log).Group(in, "merge", 0.2)
	require.Nil(t, err)
	assert.Equal(t, expectedOut, res)
}
