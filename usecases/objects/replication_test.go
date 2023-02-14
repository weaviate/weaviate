//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func Test_VObject_MarshalBinary(t *testing.T) {
	now := time.Now()
	vec := []float32{1, 2, 3, 4, 5}

	obj := models.Object{
		ID:                 strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336"),
		Class:              "SomeClass",
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(), // time-traveling ;)
		Properties: map[string]interface{}{
			"propA":    "this is prop A",
			"propB":    "this is prop B",
			"someDate": now.Format(time.RFC3339Nano),
			"aNumber":  1e+06,
			"crossRef": map[string]interface{}{
				"beacon": "weaviate://localhost/OtherClass/c82d011c-f05a-43de-8a8a-ee9c814d4cfb",
			},
		},
		Vector: vec,
		Additional: map[string]interface{}{
			"score": 0.055465422484,
		},
	}

	t.Run("assert BinaryMarshaler implementation correctness", func(t *testing.T) {
		expected := VObject{
			LatestObject:    &obj,
			StaleUpdateTime: now.UnixMilli(),
			Version:         1,
		}

		b, err := expected.MarshalBinary()
		require.Nil(t, err)

		var received VObject
		err = received.UnmarshalBinary(b)
		require.Nil(t, err)

		assert.EqualValues(t, expected, received)
	})
}
