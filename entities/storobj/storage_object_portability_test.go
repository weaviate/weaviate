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

// TestPortability_ClassNamePrecedence locks the contract of the *Disk
// decoder family on *storobj.Object:
//
//  1. A non-empty className stamps Object.Class on the decoded payload and
//     the on-disk class-name bytes are skipped — the caller wins regardless
//     of what the marshalled bytes carry.
//  2. An empty className is a contract violation on the *Disk decoders and
//     produces an error.
//
// ID, vectors, and properties round-trip faithfully whenever the decode
// succeeds.
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
		want        string // expected Object.Class on success; ignored when expectedErr is true
		expectedErr bool
	}{
		{
			name:        "caller class overrides on-disk class",
			marshaledAs: "OnDiskClass",
			decodedAs:   "CallerClass",
			want:        "CallerClass",
		},
		{
			name:        "caller class overrides matching on-disk class",
			marshaledAs: "Movies",
			decodedAs:   "Movies",
			want:        "Movies",
		},
		{
			name:        "empty caller class name returns an error",
			marshaledAs: "Movies",
			decodedAs:   "",
			expectedErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := build(tc.marshaledAs)
			before.DocID = 7

			data, err := before.MarshalBinary()
			require.NoError(t, err)

			t.Run("FromBinaryDisk", func(t *testing.T) {
				after, err := FromBinaryDisk(data, tc.decodedAs)
				if tc.expectedErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.want, after.Object.Class,
					"non-empty caller className must override the on-disk value")
				assert.Equal(t, before.ID(), after.ID())
				assert.Equal(t, before.DocID, after.DocID)
				assert.Equal(t, before.Vector, after.Vector)
				assert.Equal(t, before.Properties(), after.Properties())
			})

			t.Run("FromBinaryOptionalDisk", func(t *testing.T) {
				after, err := FromBinaryOptionalDisk(data, tc.decodedAs,
					additional.Properties{Vector: true}, nil)
				if tc.expectedErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.want, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
				assert.Equal(t, before.Vector, after.Vector)
			})

			t.Run("FromBinaryUUIDOnlyDisk", func(t *testing.T) {
				after, err := FromBinaryUUIDOnlyDisk(data, tc.decodedAs)
				if tc.expectedErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.want, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
			})
		})
	}
}

// TestEmptyOnDiskClassName asserts that a payload whose on-disk class-name
// field is empty (length 0) decodes with Object.Class set to the
// caller-supplied className across all *Disk entry points.
func TestEmptyOnDiskClassName(t *testing.T) {
	before := FromObject(
		&models.Object{
			Class:              "",
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
		{name: "short class name", className: "Movies"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("FromBinaryDisk", func(t *testing.T) {
				after, err := FromBinaryDisk(data, tc.className)
				require.NoError(t, err)
				assert.Equal(t, tc.className, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
				assert.Equal(t, before.DocID, after.DocID)
				assert.Equal(t, before.Vector, after.Vector)
				assert.Equal(t, before.Properties(), after.Properties())
			})

			t.Run("FromBinaryOptionalDisk", func(t *testing.T) {
				after, err := FromBinaryOptionalDisk(data, tc.className,
					additional.Properties{Vector: true}, nil)
				require.NoError(t, err)
				assert.Equal(t, tc.className, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
				assert.Equal(t, before.Vector, after.Vector)
			})

			t.Run("FromBinaryUUIDOnlyDisk", func(t *testing.T) {
				after, err := FromBinaryUUIDOnlyDisk(data, tc.className)
				require.NoError(t, err)
				assert.Equal(t, tc.className, after.Object.Class)
				assert.Equal(t, before.ID(), after.ID())
			})
		})
	}
}
