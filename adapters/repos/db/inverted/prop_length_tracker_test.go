package inverted

import (
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
}
