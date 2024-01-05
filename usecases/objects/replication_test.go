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

package objects

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

	t.Run("when object is present", func(t *testing.T) {
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

	t.Run("when object is present", func(t *testing.T) {
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

	t.Run("when object is nil", func(t *testing.T) {
		expected := VObject{
			LatestObject:    nil,
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

func Test_Replica_MarshalBinary(t *testing.T) {
	now := time.Now()
	vec := []float32{1, 2, 3, 4, 5}
	id := strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336")

	obj := storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              "SomeClass",
			CreationTimeUnix:   now.UnixMilli(),
			LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(), // time-traveling ;)
			Properties: map[string]interface{}{
				"propA":    "this is prop A",
				"propB":    "this is prop B",
				"someDate": now.Format(time.RFC3339Nano),
				"aNumber":  1e+06,
				"crossRef": models.MultipleRef{
					crossref.NewLocalhost("OtherClass", id).
						SingleRef(),
				},
			},
			Additional: map[string]interface{}{
				"score": 0.055465422484,
			},
		},
		Vector:    vec,
		VectorLen: 5,
	}

	t.Run("when object is present", func(t *testing.T) {
		expected := Replica{
			Object: &obj,
			ID:     obj.ID(),
		}

		b, err := expected.MarshalBinary()
		require.Nil(t, err)

		var received Replica
		err = received.UnmarshalBinary(b)
		require.Nil(t, err)

		assert.EqualValues(t, expected.Object, received.Object)
		assert.EqualValues(t, expected.ID, received.ID)
		assert.EqualValues(t, expected.Deleted, received.Deleted)
	})

	t.Run("when object is nil", func(t *testing.T) {
		expected := Replica{
			Object: nil,
			ID:     obj.ID(),
		}

		b, err := expected.MarshalBinary()
		require.Nil(t, err)

		var received Replica
		err = received.UnmarshalBinary(b)
		require.Nil(t, err)

		assert.EqualValues(t, expected.Object, received.Object)
		assert.EqualValues(t, expected.ID, received.ID)
		assert.EqualValues(t, expected.Deleted, received.Deleted)
	})
}

func Test_Replicas_MarshalBinary(t *testing.T) {
	now := time.Now()
	vec1 := []float32{1, 2, 3, 4, 5}
	vec2 := []float32{10, 20, 30, 40, 50}
	id1 := strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336")
	id2 := strfmt.UUID("88750a99-a72d-46c2-a582-89f02654391d")

	obj1 := storobj.Object{
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
	}

	obj2 := storobj.Object{
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
	}

	t.Run("when objects are present", func(t *testing.T) {
		expected := Replicas{
			{
				Object: &obj1,
				ID:     obj1.ID(),
			},
			{
				Object: &obj2,
				ID:     obj2.ID(),
			},
		}

		b, err := expected.MarshalBinary()
		require.Nil(t, err)

		var received Replicas
		err = received.UnmarshalBinary(b)
		require.Nil(t, err)

		assert.Len(t, received, 2)
		assert.EqualValues(t, expected[0].Object, received[0].Object)
		assert.EqualValues(t, expected[0].ID, received[0].ID)
		assert.EqualValues(t, expected[0].Deleted, received[0].Deleted)
		assert.EqualValues(t, expected[1].Object, received[1].Object)
		assert.EqualValues(t, expected[1].ID, received[1].ID)
		assert.EqualValues(t, expected[1].Deleted, received[1].Deleted)
	})

	t.Run("when there is a nil object", func(t *testing.T) {
		expected := Replicas{
			{
				Object: &obj1,
				ID:     obj1.ID(),
			},
			{
				Object: nil,
				ID:     obj2.ID(),
			},
		}

		b, err := expected.MarshalBinary()
		require.Nil(t, err)

		var received Replicas
		err = received.UnmarshalBinary(b)
		require.Nil(t, err)

		assert.Len(t, received, 2)
		assert.EqualValues(t, expected[0].Object, received[0].Object)
		assert.EqualValues(t, expected[0].ID, received[0].ID)
		assert.EqualValues(t, expected[0].Deleted, received[0].Deleted)
		assert.EqualValues(t, expected[1].Object, received[1].Object)
		assert.EqualValues(t, expected[1].ID, received[1].ID)
		assert.EqualValues(t, expected[1].Deleted, received[1].Deleted)
	})
}
