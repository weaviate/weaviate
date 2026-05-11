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

package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ParsePath(t *testing.T) {
	tests := []struct {
		name      string
		rootClass string
		segments  []interface{}
		expected  *Path
		expectErr bool
	}{
		{
			name:      "primitive prop",
			rootClass: "City",
			segments:  []interface{}{"population"},
			expected:  &Path{Class: "City", Property: "population"},
		},
		{
			name:      "len prop",
			rootClass: "City",
			segments:  []interface{}{"len(population)"},
			expected:  &Path{Class: "City", Property: "len(population)"},
		},
		{
			name:      "nested refs",
			rootClass: "City",
			segments:  []interface{}{"inCountry", "Country", "inContinent", "Continent", "onPlanet", "Planet", "name"},
			expected: &Path{
				Class:    "City",
				Property: "inCountry",
				Child: &Path{
					Class:    "Country",
					Property: "inContinent",
					Child: &Path{
						Class:    "Continent",
						Property: "onPlanet",
						Child:    &Path{Class: "Planet", Property: "name"},
					},
				},
			},
		},
		{
			name:      "non-valid prop errors",
			rootClass: "City",
			segments:  []interface{}{"populatS356()ion"},
			expectErr: true,
		},
		{
			name:      "non-valid len prop errors",
			rootClass: "City",
			segments:  []interface{}{"len(populatS356()ion)"},
			expectErr: true,
		},
		// namespace-qualified class names produced by namespacing.Resolve.
		// The parser accepts "<ns>:<Class>" at every path position (root +
		// nested) and preserves the qualification end to end so downstream
		// schema lookups hit the correct internal class.
		{
			name:      "namespace-qualified root class",
			rootClass: "customer1:City",
			segments:  []interface{}{"population"},
			expected:  &Path{Class: "customer1:City", Property: "population"},
		},
		{
			name:      "namespace-qualified nested ref classes",
			rootClass: "customer1:City",
			segments:  []interface{}{"inCountry", "customer1:Country", "inContinent", "customer1:Continent", "name"},
			expected: &Path{
				Class:    "customer1:City",
				Property: "inCountry",
				Child: &Path{
					Class:    "customer1:Country",
					Property: "inContinent",
					Child:    &Path{Class: "customer1:Continent", Property: "name"},
				},
			},
		},
		{
			name:      "mixed: qualified root, plain nested",
			rootClass: "customer1:City",
			segments:  []interface{}{"inCountry", "Country", "name"},
			expected: &Path{
				Class:    "customer1:City",
				Property: "inCountry",
				Child:    &Path{Class: "Country", Property: "name"},
			},
		},
		{
			name:      "mixed: plain root, qualified nested",
			rootClass: "City",
			segments:  []interface{}{"inCountry", "customer1:Country", "name"},
			expected: &Path{
				Class:    "City",
				Property: "inCountry",
				Child:    &Path{Class: "customer1:Country", Property: "name"},
			},
		},
		// Malformed namespace prefix on the root.
		{
			name:      "root with empty namespace errors",
			rootClass: ":City",
			segments:  []interface{}{"name"},
			expectErr: true,
		},
		{
			name:      "root with uppercase namespace errors",
			rootClass: "Cust:City",
			segments:  []interface{}{"name"},
			expectErr: true,
		},
		{
			name:      "root with empty class errors",
			rootClass: "customer1:",
			segments:  []interface{}{"name"},
			expectErr: true,
		},
		// Malformed namespace prefix on a nested ref segment.
		{
			name:      "nested ref with empty namespace errors",
			rootClass: "customer1:City",
			segments:  []interface{}{"inCountry", ":Country", "name"},
			expectErr: true,
		},
		{
			name:      "nested ref with uppercase namespace errors",
			rootClass: "customer1:City",
			segments:  []interface{}{"inCountry", "Cust:Country", "name"},
			expectErr: true,
		},
		{
			name:      "nested ref with empty class errors",
			rootClass: "customer1:City",
			segments:  []interface{}{"inCountry", "customer1:", "name"},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := ParsePath(tt.segments, tt.rootClass)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, path)
		})
	}
}

func Test_PathGetInnerMost(t *testing.T) {
	rootClass := "City"
	segments := []interface{}{"inCountry", "Country", "inContinent", "Continent", "onPlanet", "Planet", "name"}
	path, err := ParsePath(segments, rootClass)
	require.NoError(t, err)
	assert.Equal(t, &Path{Class: "Planet", Property: "name"}, path.GetInnerMost())
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
			segments := path.Slice()
			assert.Equal(t, expectedSegments, segments, "should slice the path correctly")
		})
	})
}
