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

package crossref

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_Source_ParsingFromString(t *testing.T) {
	t.Run("from a local object ref that is well-formed", func(t *testing.T) {
		uri := "weaviate://localhost/MyClassName/c2cd3f91-0160-477e-869a-8da8829e0a4d/myRefProp"
		ref, err := ParseSource(uri)

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

		t.Run("the class name is correct", func(t *testing.T) {
			assert.Equal(t, ref.Class, schema.ClassName("MyClassName"))
		})

		t.Run("the property name is correct", func(t *testing.T) {
			assert.Equal(t, ref.Property, schema.PropertyName("myRefProp"))
		})

		t.Run("assembling a new source and comparing if the match", func(t *testing.T) {
			alt := NewSource("MyClassName", "myRefProp",
				"c2cd3f91-0160-477e-869a-8da8829e0a4d")
			assert.Equal(t, ref, alt)
		})
	})

	t.Run("from a local action ref that is well-formed", func(t *testing.T) {
		uri := "weaviate://localhost/MyActionClass/c2cd3f91-0160-477e-869a-8da8829e0a4d/myRefProp"
		ref, err := ParseSource(uri)

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

		t.Run("the class name is correct", func(t *testing.T) {
			assert.Equal(t, ref.Class, schema.ClassName("MyActionClass"))
		})

		t.Run("the property name is correct", func(t *testing.T) {
			assert.Equal(t, ref.Property, schema.PropertyName("myRefProp"))
		})

		t.Run("assembling a new source and comparing if the match", func(t *testing.T) {
			alt := NewSource("MyActionClass", "myRefProp",
				"c2cd3f91-0160-477e-869a-8da8829e0a4d")
			assert.Equal(t, ref, alt)
		})
	})

	t.Run("from a network action ref that is well-formed", func(t *testing.T) {
		uri := "weaviate://another-weaviate/SomeActionClass/c2cd3f91-0160-477e-869a-8da8829e0a4d/myRefProp"
		ref, err := ParseSource(uri)

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

		t.Run("the class name is correct", func(t *testing.T) {
			assert.Equal(t, ref.Class, schema.ClassName("SomeActionClass"))
		})

		t.Run("the property name is correct", func(t *testing.T) {
			assert.Equal(t, ref.Property, schema.PropertyName("myRefProp"))
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
				uri:  "weaviate://localhost/SomeClass",
			},
			{
				name: "with too many path segments",
				uri:  "weaviate://localhost/SomeClass/c2cd3f91-0160-477e-869a-8da8829e0a4d/myRefProp/somethingElse",
			},
			{
				name: "without a property",
				uri:  "weaviate://localhost/SomeClass/c2cd3f91-0160-477e-869a-8da8829e0a4d/",
			},
			{
				name: "with an invalid uuid",
				uri:  "weaviate://localhost/SomeClass/c2cd3f91-iSneakedInHere-477e-869a-8da8829e0a4d",
			},
			{
				name: "with an invalid kind", // was /humans/SomeClass
				uri:  "weaviate://localhost/SomeClass/c2cd3f91-0160-477e-869a-8da8829e0a4d",
			},
			{
				name: "with a lowercased class name",
				uri:  "weaviate://localhost/someClass/c2cd3f91-0160-477e-869a-8da8829e0a4d/myRefProp",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				_, err := ParseSource(test.uri)
				assert.NotNil(t, err, test.name)
			})
		}
	})
}

func Test_Source_GenerateString(t *testing.T) {
	uri := "weaviate://localhost/MyClass/c2cd3f91-0160-477e-869a-8da8829e0a4d/myRefProp"
	ref, err := ParseSource(uri)

	require.Nil(t, err, "should not error")
	assert.Equal(t, uri, ref.String(), "should be the same as the input string")
}
