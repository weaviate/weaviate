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

package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ParsePath(t *testing.T) {
	t.Run("with a primitive prop", func(t *testing.T) {
		rootClass := "City"
		segments := []interface{}{"population"}
		expectedPath := &Path{
			Class:    "City",
			Property: "population",
		}

		path, err := ParsePath(segments, rootClass)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedPath, path, "should parse the path correctly")
	})

	t.Run("with len prop", func(t *testing.T) {
		rootClass := "City"
		segments := []interface{}{"len(population)"}
		expectedPath := &Path{
			Class:    "City",
			Property: "len(population)",
		}

		path, err := ParsePath(segments, rootClass)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedPath, path, "should parse the path correctly")
	})

	t.Run("with nested refs", func(t *testing.T) {
		rootClass := "City"
		segments := []interface{}{"inCountry", "Country", "inContinent", "Continent", "onPlanet", "Planet", "name"}
		expectedPath := &Path{
			Class:    "City",
			Property: "inCountry",
			Child: &Path{
				Class:    "Country",
				Property: "inContinent",
				Child: &Path{
					Class:    "Continent",
					Property: "onPlanet",
					Child: &Path{
						Class:    "Planet",
						Property: "name",
					},
				},
			},
		}

		path, err := ParsePath(segments, rootClass)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedPath, path, "should parse the path correctly")

		// Extract innermost path element
		innerMost := path.GetInnerMost()
		assert.Equal(t, innerMost, &Path{Class: "Planet", Property: "name"})

		// Print Slice
	})

	t.Run("with non-valid prop", func(t *testing.T) {
		rootClass := "City"
		segments := []interface{}{"populatS356()ion"}
		_, err := ParsePath(segments, rootClass)
		require.NotNil(t, err, "should error")
	})

	t.Run("with non-valid len prop", func(t *testing.T) {
		rootClass := "City"
		segments := []interface{}{"len(populatS356()ion)"}
		_, err := ParsePath(segments, rootClass)
		require.NotNil(t, err, "should error")
	})
}

func Test_SlicePath(t *testing.T) {
	t.Run("with a primitive prop", func(t *testing.T) {
		path := &Path{
			Class:    "City",
			Property: "population",
		}
		expectedSegments := []interface{}{"population"}

		segments := path.SliceInterface()

		assert.Equal(t, expectedSegments, segments, "should slice the path correctly")
	})

	t.Run("with nested refs", func(t *testing.T) {
		path := &Path{
			Class:    "City",
			Property: "inCountry",
			Child: &Path{
				Class:    "Country",
				Property: "inContinent",
				Child: &Path{
					Class:    "Continent",
					Property: "onPlanet",
					Child: &Path{
						Class:    "Planet",
						Property: "name",
					},
				},
			},
		}

		t.Run("as []interface{}", func(t *testing.T) {
			expectedSegments := []interface{}{"inCountry", "Country", "inContinent", "Continent", "onPlanet", "Planet", "name"}
			segments := path.SliceInterface()
			assert.Equal(t, expectedSegments, segments, "should slice the path correctly")
		})

		t.Run("as []string titleized", func(t *testing.T) {
			expectedSegments := []string{"inCountry", "Country", "inContinent", "Continent", "onPlanet", "Planet", "name"}
			segments := path.Slice()
			assert.Equal(t, expectedSegments, segments, "should slice the path correctly")
		})

		t.Run("as []string non-titleized", func(t *testing.T) {
			expectedSegments := []string{"inCountry", "Country", "inContinent", "Continent", "onPlanet", "Planet", "name"}
			segments := path.SliceNonTitleized()
			assert.Equal(t, expectedSegments, segments, "should slice the path correctly")
		})
	})
}
