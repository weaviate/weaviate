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

package storobj

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

// TestPortability_ClassNamePrecedence locks the WS7 contract for who decides
// the value of Object.Class on decode:
//
//  1. When the caller supplies a non-empty className, the caller wins and
//     the on-disk class-name bytes are skipped. This is the path taken by
//     every objects-bucket reader inside a shard, and it is what makes
//     namespace-data portable: a non-namespaced cluster reading bytes that
//     were marshalled with a qualified `<ns>:<class>` Class produces the
//     plain class on read, without rewriting on-disk bytes.
//
//  2. When the caller supplies an empty className, the decoder falls back
//     to the on-disk value. This preserves behaviour for callers that have
//     no canonical class to supply (replica/wire-receive paths where the
//     class is carried out-of-band by the surrounding protocol).
//
// Either way, ID, vectors, and properties round-trip faithfully.
func TestPortability_ClassNamePrecedence(t *testing.T) {
	build := func(class string) *Object {
		return FromObject(
			&models.Object{
				Class:              class,
				CreationTimeUnix:   123456,
				LastUpdateTimeUnix: 56789,
				ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
				Properties: map[string]interface{}{
					"title": "The Matrix",
				},
			},
			[]float32{1, 2, 3},
			nil,
			nil,
		)
	}

	cases := []struct {
		name        string
		marshaledAs string
		decodedAs   string
		want        string
	}{
		{
			name:        "caller wins — namespaced bytes read by non-namespaced cluster",
			marshaledAs: "my_ns:Movies",
			decodedAs:   "Movies",
			want:        "Movies",
		},
		{
			name:        "caller wins — non-namespaced bytes read by namespaced cluster",
			marshaledAs: "Movies",
			decodedAs:   "other_ns:Movies",
			want:        "other_ns:Movies",
		},
		{
			name:        "fallback to on-disk when caller supplies empty",
			marshaledAs: "Movies",
			decodedAs:   "",
			want:        "Movies",
		},
		{
			name:        "fallback preserves a qualified on-disk class",
			marshaledAs: "my_ns:Movies",
			decodedAs:   "",
			want:        "my_ns:Movies",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := build(tc.marshaledAs)
			before.DocID = 7

			data, err := before.MarshalBinary()
			require.NoError(t, err)

			t.Run("FromBinaryWithClassName", func(t *testing.T) {
				after, err := FromBinaryWithClassName(data, tc.decodedAs)
				require.NoError(t, err)
				assert.Equal(t, tc.want, after.Object.Class,
					"caller-supplied className wins; empty falls back to on-disk")
				assert.Equal(t, before.ID(), after.ID())
				assert.Equal(t, before.DocID, after.DocID)
				assert.Equal(t, before.Vector, after.Vector)
				assert.Equal(t, before.Properties(), after.Properties())
			})

			t.Run("FromBinaryOptionalWithClassName", func(t *testing.T) {
				after, err := FromBinaryOptionalWithClassName(data, tc.decodedAs,
					additional.Properties{Vector: true}, nil)
				require.NoError(t, err)
				assert.Equal(t, tc.want, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
				assert.Equal(t, before.Vector, after.Vector)
			})

			t.Run("FromBinaryUUIDOnlyWithClassName", func(t *testing.T) {
				after, err := FromBinaryUUIDOnlyWithClassName(data, tc.decodedAs)
				require.NoError(t, err)
				assert.Equal(t, tc.want, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
			})
		})
	}
}

// TestPortability_EmptyOnDiskClassName covers the inverse of the fallback
// case: when the on-disk class-name bytes are empty (length 0), a non-empty
// caller-supplied className must be used to populate Object.Class. This is
// the shape produced when a writer marshals a storobj with Object.Class == ""
// (e.g. wire-receive paths that don't have a class to stamp at marshal time);
// the decoder must accept the caller's value rather than producing an empty
// Class.
func TestPortability_EmptyOnDiskClassName(t *testing.T) {
	before := FromObject(
		&models.Object{
			Class:              "", // intentionally empty on-disk
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Properties: map[string]interface{}{
				"title": "The Matrix",
			},
		},
		[]float32{1, 2, 3},
		nil,
		nil,
	)
	before.DocID = 7

	data, err := before.MarshalBinary()
	require.NoError(t, err)

	cases := []struct {
		name      string
		className string
	}{
		{name: "plain class name", className: "Movies"},
		{name: "namespace-qualified class name", className: "my_ns:Movies"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("FromBinaryWithClassName", func(t *testing.T) {
				after, err := FromBinaryWithClassName(data, tc.className)
				require.NoError(t, err)
				assert.Equal(t, tc.className, after.Object.Class,
					"caller-supplied className must populate Object.Class when on-disk bytes are empty")
				assert.Equal(t, before.ID(), after.ID())
				assert.Equal(t, before.DocID, after.DocID)
				assert.Equal(t, before.Vector, after.Vector)
				assert.Equal(t, before.Properties(), after.Properties())
			})

			t.Run("FromBinaryOptionalWithClassName", func(t *testing.T) {
				after, err := FromBinaryOptionalWithClassName(data, tc.className,
					additional.Properties{Vector: true}, nil)
				require.NoError(t, err)
				assert.Equal(t, tc.className, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
				assert.Equal(t, before.Vector, after.Vector)
			})

			t.Run("FromBinaryUUIDOnlyWithClassName", func(t *testing.T) {
				after, err := FromBinaryUUIDOnlyWithClassName(data, tc.className)
				require.NoError(t, err)
				assert.Equal(t, tc.className, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
			})
		})
	}
}
