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

package clusterapi

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
)

func Test_objectListPayload_Marshal(t *testing.T) {
	now := time.Now()
	vec1 := []float32{1, 2, 3, 4, 5}
	vec2 := []float32{10, 20, 30, 40, 50}
	id1 := strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336")
	id2 := strfmt.UUID("88750a99-a72d-46c2-a582-89f02654391d")

	objs := []*storobj.Object{
		{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 id1,
				Class:              "SomeClass",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(), // time-traveling ;)
				Properties: map[string]interface{}{
					"propA":    "this is prop A",
					"propB":    "this is prop B",
					"someDate": now.Format(time.RFC3339Nano),
					"aNumber":  1e+06,
					"crossRef": models.MultipleRef{
						crossref.NewLocalhost("OtherClass", id1).
							SingleRef(),
					},
				},
				Additional: map[string]interface{}{
					"score": 0.055465422484,
				},
			},
			Vector:    vec1,
			VectorLen: 5,
		},
		nil,
		{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 id2,
				Class:              "SomeClass",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(), // time-traveling ;)
				Properties: map[string]interface{}{
					"propA":    "this is prop A",
					"propB":    "this is prop B",
					"someDate": now.Format(time.RFC3339Nano),
					"aNumber":  1e+06,
					"crossRef": models.MultipleRef{
						crossref.NewLocalhost("OtherClass", id2).
							SingleRef(),
					},
				},
				Additional: map[string]interface{}{
					"score": 0.055465422484,
				},
			},
			Vector:    vec2,
			VectorLen: 5,
		},
	}

	payload := objectListPayload{}
	b, err := payload.Marshal(objs)
	require.Nil(t, err)

	received, err := payload.Unmarshal(b)
	require.Nil(t, err)
	assert.Len(t, received, 2)
	assert.EqualValues(t, objs[0].Object, received[0].Object)
	assert.EqualValues(t, objs[0].ID(), received[0].ID())
	assert.EqualValues(t, objs[2].Object, received[1].Object)
	assert.EqualValues(t, objs[2].ID(), received[1].ID())
}
