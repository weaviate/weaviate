package inverted

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PropertyLengthTracker(t *testing.T) {
	t.Run("single prop", func(t *testing.T) {
		type test struct {
			values []float32
			name   string
		}

		tests := []test{
			{
				values: []float32{2, 2, 3, 100, 100, 500, 7},
				name:   "mixed values",
			}, {
				values: []float32{
					1000, 1200, 1000, 1300, 800, 2000, 2050,
					2070, 900,
				},
				name: "high values",
			}, {
				values: []float32{
					60000, 50000, 65000,
				},
				name: "very high values",
			}, {
				values: []float32{
					1, 2, 4, 3, 4, 2, 1, 5, 6, 7, 8, 2, 7, 2, 3, 5,
					6, 3, 5, 9, 3, 4, 8,
				},
				name: "very low values",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				tracker := NewPropertyLengthTracker("")

				actualMean := float32(0)
				for _, v := range test.values {
					tracker.TrackProperty("my-very-first-prop", v)
					actualMean += v
				}
				actualMean = actualMean / float32(len(test.values))

				res, err := tracker.PropertyMean("my-very-first-prop")
				require.Nil(t, err)

				assert.InEpsilon(t, actualMean, res, 0.1)
			})
		}
	})

	t.Run("multiple properties (can all fit on one page)", func(t *testing.T) {
		type prop struct {
			values   []float32
			propName string
		}

		props := []prop{
			{
				values:   []float32{2, 2, 3, 100, 100, 500, 7},
				propName: "property-numero-uno",
			}, {
				values: []float32{
					1000, 1200, 1000, 1300, 800, 2000, 2050,
					2070, 900,
				},
				propName: "the-second-of-the-properties",
			}, {
				values: []float32{
					60000, 50000, 65000,
				},
				propName: "property_nummer_DREI",
			},
		}

		// This time we use a single tracker
		tracker := NewPropertyLengthTracker("")

		for _, prop := range props {
			for _, v := range prop.values {
				tracker.TrackProperty(prop.propName, v)
			}
		}

		for _, prop := range props {
			actualMean := float32(0)
			for _, v := range prop.values {
				actualMean += v
			}
			actualMean = actualMean / float32(len(prop.values))

			res, err := tracker.PropertyMean(prop.propName)
			require.Nil(t, err)

			assert.InEpsilon(t, actualMean, res, 0.1)
		}
	})

	t.Run("with more properties that can fit on one page", func(t *testing.T) {
		type prop struct {
			values   []float32
			propName string
		}

		// the most props we could ever fit on a single page is 16 if there was no
		// index, which is impossible. This means the practical max is 15, so at
		// least 5 props should overflow to the second page.
		propCount := 20
		props := make([]prop, propCount)

		for i := range props {
			props[i] = prop{
				values:   []float32{1, 4, 3, 17},
				propName: fmt.Sprintf("prop_%d", i),
			}
		}

		// This time we use a single tracker
		tracker := NewPropertyLengthTracker("")

		for _, prop := range props {
			for _, v := range prop.values {
				tracker.TrackProperty(prop.propName, v)
			}
		}

		for _, prop := range props {
			actualMean := float32(0)
			for _, v := range prop.values {
				actualMean += v
			}
			actualMean = actualMean / float32(len(prop.values))

			res, err := tracker.PropertyMean(prop.propName)
			require.Nil(t, err)

			assert.InEpsilon(t, actualMean, res, 0.1)
		}

		// modify a prop on page 2 and verify
		tracker.TrackProperty("prop_19", 24)
		actualMeanForProp20 := float32(1+4+3+17+25) / 5.0
		res, err := tracker.PropertyMean("prop_19")
		require.Nil(t, err)

		assert.InEpsilon(t, actualMeanForProp20, res, 0.1)
	})
}
