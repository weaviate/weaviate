//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package crossref

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ParsingFromString(t *testing.T) {
	t.Run("from a local object ref that is well-formed", func(t *testing.T) {
		uri := "weaviate://localhost/c2cd3f91-0160-477e-869a-8da8829e0a4d"
		ref, err := Parse(uri)

		require.Nil(t, err, "should not error")

		t.Run("is a local ref", func(t *testing.T) {
			assert.Equal(t, ref.Local, true)
		})

		t.Run("peerName points to localhost", func(t *testing.T) {
			assert.Equal(t, ref.PeerName, "localhost")
		})

		t.Run("id points correctly", func(t *testing.T) {
			assert.Equal(t, ref.TargetID, strfmt.UUID("c2cd3f91-0160-477e-869a-8da8829e0a4d"))
		})
	})

	t.Run("from a local action ref that is well-formed", func(t *testing.T) {
		uri := "weaviate://localhost/c2cd3f91-0160-477e-869a-8da8829e0a4d"
		ref, err := Parse(uri)

		require.Nil(t, err, "should not error")

		t.Run("is a local ref", func(t *testing.T) {
			assert.Equal(t, ref.Local, true)
		})

		t.Run("peerName points to localhost", func(t *testing.T) {
			assert.Equal(t, ref.PeerName, "localhost")
		})

		t.Run("id points correctly", func(t *testing.T) {
			assert.Equal(t, ref.TargetID, strfmt.UUID("c2cd3f91-0160-477e-869a-8da8829e0a4d"))
		})
	})

	t.Run("from a network action ref that is well-formed", func(t *testing.T) {
		uri := "weaviate://another-weaviate/c2cd3f91-0160-477e-869a-8da8829e0a4d"
		ref, err := Parse(uri)

		require.Nil(t, err, "should not error")

		t.Run("is a local ref", func(t *testing.T) {
			assert.Equal(t, ref.Local, false)
		})

		t.Run("peerName points to localhost", func(t *testing.T) {
			assert.Equal(t, ref.PeerName, "another-weaviate")
		})

		t.Run("id points correctly", func(t *testing.T) {
			assert.Equal(t, ref.TargetID, strfmt.UUID("c2cd3f91-0160-477e-869a-8da8829e0a4d"))
		})
	})

	t.Run("with formatting errors", func(t *testing.T) {
		type testCaseError struct {
			name string
			uri  string
		}

		tests := []testCaseError{
			{
				name: "with an invalid URL",
				uri:  "i:am:not:a:url",
			},
			{
				name: "with too few path segments",
				uri:  "weaviate://localhost",
			},
			{
				name: "with too many path segments",
				uri:  "weaviate://localhost/c2cd3f91-0160-477e-869a-8da8829e0a4d/i-shouldnt-be-here",
			},
			{
				name: "with an invalid uuid",
				uri:  "weaviate://localhost/c2cd3f91-iSneakedInHere-477e-869a-8da8829e0a4d",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				_, err := Parse(test.uri)
				assert.NotNil(t, err, test.name)
			})
		}
	})
}

func Test_ParsingFromSingleRef(t *testing.T) {
	t.Run("from a local object ref that is well-formed", func(t *testing.T) {
		uri := strfmt.URI("weaviate://localhost/c2cd3f91-0160-477e-869a-8da8829e0a4d")
		singleRef := &models.SingleRef{
			Beacon: uri,
		}
		ref, err := ParseSingleRef(singleRef)

		require.Nil(t, err, "should not error")

		t.Run("is a local ref", func(t *testing.T) {
			assert.Equal(t, ref.Local, true)
		})

		t.Run("peerName points to localhost", func(t *testing.T) {
			assert.Equal(t, ref.PeerName, "localhost")
		})

		t.Run("id points correctly", func(t *testing.T) {
			assert.Equal(t, ref.TargetID, strfmt.UUID("c2cd3f91-0160-477e-869a-8da8829e0a4d"))
		})
	})
}

func Test_GenerateString(t *testing.T) {
	uri := "weaviate://localhost/c2cd3f91-0160-477e-869a-8da8829e0a4d"
	ref, err := Parse(uri)

	require.Nil(t, err, "should not error")
	assert.Equal(t, uri, ref.String(), "should be the same as the input string")
}

func Test_SingleRef(t *testing.T) {
	uri := "weaviate://localhost/c2cd3f91-0160-477e-869a-8da8829e0a4d"
	ref, err := Parse(uri)
	expectedResult := &models.SingleRef{
		Beacon: strfmt.URI(uri),
	}

	require.Nil(t, err, "should not error")
	assert.Equal(t, expectedResult, ref.SingleRef(), "should create a singleRef (api construct)")
}
