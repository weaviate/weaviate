//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
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

	t.Run("with nested refs", func(t *testing.T) {
		rootClass := "City"
		segments := []interface{}{"InCountry", "Country", "InContinent", "Continent", "OnPlanet", "Planet", "name"}
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
		expectedSegments := []interface{}{"InCountry", "Country", "InContinent", "Continent", "OnPlanet", "Planet", "name"}

		segments := path.SliceInterface()

		assert.Equal(t, expectedSegments, segments, "should slice the path correctly")
	})
}
