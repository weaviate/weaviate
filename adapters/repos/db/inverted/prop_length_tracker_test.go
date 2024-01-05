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

package inverted

import (
	"fmt"
	"path"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PropertyLengthTracker(t *testing.T) {
	dirName := t.TempDir()
	trackerPath := path.Join(dirName, "my_test_shard")
	l := logrus.New()

	// This test suite doesn't actually test persistence, there is a separate
	// one. However, we still need to supply a valid path. Since nothing is ever
	// written, we can use the same one for each sub-test without them
	// accidentally sharing state.

	t.Run("single prop", func(t *testing.T) {
		type test struct {
			values       []float32
			name         string
			floatCompare bool
		}

		tests := []test{
			{
				values:       []float32{2, 2, 3, 100, 100, 500, 7},
				name:         "mixed_values",
				floatCompare: true,
			},
			{
				values: []float32{
					1000, 1200, 1000, 1300, 800, 2000, 2050,
					2070, 900,
				},
				name:         "high_values",
				floatCompare: true,
			},
			{
				values: []float32{
					60000, 50000, 65000,
				},
				name:         "very_high_values",
				floatCompare: true,
			},
			{
				values: []float32{
					1, 2, 4, 3, 4, 2, 1, 5, 6, 7, 8, 2, 7, 2, 3, 5,
					6, 3, 5, 9, 3, 4, 8,
				},
				name:         "very_low_values",
				floatCompare: true,
			},
			{
				values:       []float32{0, 0},
				name:         "zeros",
				floatCompare: false,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				tracker, err := NewJsonShardMetaData(trackerPath+test.name, l)
				require.Nil(t, err)

				actualMean := float32(0)
				for _, v := range test.values {
					tracker.TrackProperty("my-very-first-prop", v)
					actualMean += v
				}
				actualMean = actualMean / float32(len(test.values))

				res, err := tracker.PropertyMean("my-very-first-prop")
				require.Nil(t, err)

				if test.floatCompare {
					assert.InEpsilon(t, actualMean, res, 0.1)
				} else {
					assert.Equal(t, actualMean, res)
				}
				require.Nil(t, tracker.Close())
			})
		}
	})

	t.Run("test untrack", func(t *testing.T) {
		tracker, err := NewJsonShardMetaData(trackerPath, l)
		require.Nil(t, err)

		tracker.TrackProperty("test-prop", 1)
		tracker.TrackProperty("test-prop", 2)
		tracker.TrackProperty("test-prop", 3)
		tracker.Flush(false)

		sum, count, mean, err := tracker.PropertyTally("test-prop")
		require.Nil(t, err)
		assert.Equal(t, 6, sum)
		assert.Equal(t, 3, count)
		assert.InEpsilon(t, 2, mean, 0.1)

		tracker.UnTrackProperty("test-prop", 2)
		sum, count, mean, err = tracker.PropertyTally("test-prop")
		require.Nil(t, err)
		assert.Equal(t, 4, sum)
		assert.Equal(t, 2, count)
		assert.InEpsilon(t, 2, mean, 0.1)

		tracker.UnTrackProperty("test-prop", 1)
		sum, count, mean, err = tracker.PropertyTally("test-prop")
		require.Nil(t, err)
		assert.Equal(t, 3, sum)
		assert.Equal(t, 1, count)
		assert.InEpsilon(t, 3, mean, 0.1)

		require.Nil(t, tracker.Close())
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
		tracker, err := NewJsonShardMetaData(trackerPath, l)
		require.Nil(t, err)

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

		require.Nil(t, tracker.Close())
	})

	t.Run("with more properties that can fit on one page", func(t *testing.T) {
		// This time we use a single tracker
		tracker, err := NewJsonShardMetaData(trackerPath, l)
		require.Nil(t, err)

		create20PropsAndVerify(t, tracker)

		require.Nil(t, tracker.Close())
	})
}

func create20PropsAndVerify(t *testing.T, tracker *JsonShardMetaData) {
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
}

func Test_PropertyLengthTracker_Persistence(t *testing.T) {
	dirName := t.TempDir()

	path := path.Join(dirName, "my_test_shard")

	var tracker *JsonShardMetaData
	l := logrus.New()

	t.Run("initializing an empty tracker, no file present", func(t *testing.T) {
		tr, err := NewJsonShardMetaData(path, l)
		require.Nil(t, err)
		tracker = tr
	})

	t.Run("importing multi-page data and verifying", func(t *testing.T) {
		create20PropsAndVerify(t, tracker)
	})

	t.Run("commit the state to disk", func(t *testing.T) {
		require.Nil(t, tracker.Flush(false))
	})

	t.Run("shut down the tracker", func(t *testing.T) {
		require.Nil(t, tracker.Close())
	})

	var secondTracker *JsonShardMetaData
	t.Run("initializing a new tracker from the same file", func(t *testing.T) {
		tr, err := NewJsonShardMetaData(path, l)
		require.Nil(t, err)
		secondTracker = tr
	})

	t.Run("verify data is correct after read from disk", func(t *testing.T) {
		// root page
		actualMeanForProp0 := float32(1+4+3+17) / 4.0
		res, err := secondTracker.PropertyMean("prop_0")
		require.Nil(t, err)
		assert.InEpsilon(t, actualMeanForProp0, res, 0.1)

		// later page
		actualMeanForProp20 := float32(1+4+3+17+25) / 5.0
		res, err = secondTracker.PropertyMean("prop_19")
		require.Nil(t, err)
		assert.InEpsilon(t, actualMeanForProp20, res, 0.1)
	})
}

// Testing the switch from the old property length tracker to the new one
func TestFormatConversion(t *testing.T) {
	dirName := t.TempDir()

	path := path.Join(dirName, "my_test_shard")

	var tracker *PropertyLengthTracker

	t.Run("initializing an empty tracker, no file present", func(t *testing.T) {
		tr, err := NewPropertyLengthTracker(path)
		require.Nil(t, err)
		tracker = tr
	})

	t.Run("importing multi-page data and verifying", func(t *testing.T) {
		create20PropsAndVerify_old(t, tracker)
	})

	t.Run("commit the state to disk", func(t *testing.T) {
		require.Nil(t, tracker.Flush())
	})

	t.Run("shut down the tracker", func(t *testing.T) {
		require.Nil(t, tracker.Close())
	})

	var newTracker *JsonShardMetaData
	l := logrus.New()

	t.Run("initializing a new tracker from the same file", func(t *testing.T) {
		tr, err := NewJsonShardMetaData(path, l)
		require.Nil(t, err)
		newTracker = tr
	})

	t.Run("verify data is correct after read from disk", func(t *testing.T) {
		// root page
		actualMeanForProp0 := float32(1+4+3+17) / 4.0
		res, err := newTracker.PropertyMean("prop_0")
		require.Nil(t, err)
		assert.InEpsilon(t, actualMeanForProp0, res, 0.1)

		// later page
		actualMeanForProp20 := float32(1+4+3+17+25) / 5.0
		res, err = newTracker.PropertyMean("prop_19")
		require.Nil(t, err)
		assert.InEpsilon(t, actualMeanForProp20, res, 0.1)

		res, err = newTracker.PropertyMean("prop_22")
		require.Nil(t, err)
		assert.EqualValues(t, res, 0)
		sum, count, average, _ := newTracker.PropertyTally("prop_22")
		assert.EqualValues(t, 0, sum)
		assert.EqualValues(t, 3, count)
		assert.EqualValues(t, 0, average)
	})
}

func create20PropsAndVerify_old(t *testing.T, tracker *PropertyLengthTracker) {
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

	for _, prop := range props {
		for _, v := range prop.values {
			tracker.TrackProperty(prop.propName, v)
		}
	}

	tracker.TrackProperty("prop_22", 0)
	tracker.TrackProperty("prop_22", 0)
	tracker.TrackProperty("prop_22", 0)

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

	res, err = tracker.PropertyMean("prop_22")
	require.Nil(t, err)
	assert.EqualValues(t, res, 0)

	sum, _, average, _ := tracker.PropertyTally("prop_22")
	assert.EqualValues(t, 0, sum)
	// assert.EqualValues(t, 3, count)
	assert.EqualValues(t, 0, average)
}

// Test the old property length tracker

func TestOldPropertyLengthTracker(t *testing.T) {
	dirName := t.TempDir()
	trackerPath := path.Join(dirName, "my_test_shard")

	// This test suite doesn't actually test persistence, there is a separate
	// one. However, we still need to supply a valid path. Since nothing is ever
	// written, we can use the same one for each sub-test without them
	// accidentally sharing state.

	t.Run("single prop", func(t *testing.T) {
		type test struct {
			values       []float32
			name         string
			floatCompare bool
		}

		tests := []test{
			{
				values:       []float32{2, 2, 3, 100, 100, 500, 7},
				name:         "mixed_values",
				floatCompare: true,
			}, {
				values: []float32{
					1000, 1200, 1000, 1300, 800, 2000, 2050,
					2070, 900,
				},
				name:         "high_values",
				floatCompare: true,
			}, {
				values: []float32{
					60000, 50000, 65000,
				},
				name:         "very_high_values",
				floatCompare: true,
			}, {
				values: []float32{
					1, 2, 4, 3, 4, 2, 1, 5, 6, 7, 8, 2, 7, 2, 3, 5,
					6, 3, 5, 9, 3, 4, 8,
				},
				name:         "very_low_values",
				floatCompare: true,
			}, {
				values:       []float32{0, 0},
				name:         "zeros",
				floatCompare: false,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				tracker, err := NewPropertyLengthTracker(trackerPath + test.name)
				require.Nil(t, err)

				actualMean := float32(0)
				for _, v := range test.values {
					tracker.TrackProperty("my-very-first-prop", v)
					actualMean += v
				}
				actualMean = actualMean / float32(len(test.values))

				res, err := tracker.PropertyMean("my-very-first-prop")
				require.Nil(t, err)

				if test.floatCompare {
					assert.InEpsilon(t, actualMean, res, 0.1)
				} else {
					assert.Equal(t, actualMean, res)
				}
				require.Nil(t, tracker.Close())
			})
		}
	})

	t.Run("test untrack", func(t *testing.T) {
		tracker, err := NewPropertyLengthTracker(trackerPath)
		require.Nil(t, err)

		tracker.TrackProperty("test-prop", 1)
		tracker.TrackProperty("test-prop", 2)
		tracker.TrackProperty("test-prop", 3)
		tracker.Flush()

		sum, count, mean, err := tracker.PropertyTally("test-prop")
		require.Nil(t, err)
		assert.Equal(t, 6, sum)
		assert.Equal(t, 3, count)
		assert.InEpsilon(t, 2, mean, 0.1)

		tracker.UnTrackProperty("test-prop", 2)
		sum, count, mean, err = tracker.PropertyTally("test-prop")
		require.Nil(t, err)
		assert.Equal(t, 4, sum)
		assert.Equal(t, 2, count)
		assert.InEpsilon(t, 2, mean, 0.1)

		tracker.UnTrackProperty("test-prop", 1)
		sum, count, mean, err = tracker.PropertyTally("test-prop")
		require.Nil(t, err)
		assert.Equal(t, 3, sum)
		assert.Equal(t, 1, count)
		assert.InEpsilon(t, 3, mean, 0.1)

		require.Nil(t, tracker.Close())
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
		tracker, err := NewPropertyLengthTracker(trackerPath)
		require.Nil(t, err)

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

		require.Nil(t, tracker.Close())
	})

	t.Run("with more properties that can fit on one page", func(t *testing.T) {
		// This time we use a single tracker
		tracker, err := NewPropertyLengthTracker(trackerPath)
		require.Nil(t, err)

		create20PropsAndVerify_old(t, tracker)

		require.Nil(t, tracker.Close())
	})
}

func TestOldPropertyLengthTracker_Persistence(t *testing.T) {
	dirName := t.TempDir()

	path := path.Join(dirName, "my_test_shard")

	var tracker *PropertyLengthTracker

	t.Run("initializing an empty tracker, no file present", func(t *testing.T) {
		tr, err := NewPropertyLengthTracker(path)
		require.Nil(t, err)
		tracker = tr
	})

	t.Run("importing multi-page data and verifying", func(t *testing.T) {
		create20PropsAndVerify_old(t, tracker)
	})

	t.Run("commit the state to disk", func(t *testing.T) {
		require.Nil(t, tracker.Flush())
	})

	t.Run("shut down the tracker", func(t *testing.T) {
		require.Nil(t, tracker.Close())
	})

	var secondTracker *PropertyLengthTracker
	t.Run("initializing a new tracker from the same file", func(t *testing.T) {
		tr, err := NewPropertyLengthTracker(path)
		require.Nil(t, err)
		secondTracker = tr
	})

	t.Run("verify data is correct after read from disk", func(t *testing.T) {
		// root page
		actualMeanForProp0 := float32(1+4+3+17) / 4.0
		res, err := secondTracker.PropertyMean("prop_0")
		require.Nil(t, err)
		assert.InEpsilon(t, actualMeanForProp0, res, 0.1)

		// later page
		actualMeanForProp20 := float32(1+4+3+17+25) / 5.0
		res, err = secondTracker.PropertyMean("prop_19")
		require.Nil(t, err)
		assert.InEpsilon(t, actualMeanForProp20, res, 0.1)
	})

	t.Run("shut down the second tracker", func(t *testing.T) {
		require.Nil(t, secondTracker.Close())
	})
}

func Test_PropertyLengthTracker_Overflow(t *testing.T) {
	dirName := t.TempDir()
	path := path.Join(dirName, "my_test_shard")

	tracker, err := NewPropertyLengthTracker(path)
	require.Nil(t, err)

	for i := 0; i < 16*15; i++ {
		err := tracker.TrackProperty(fmt.Sprintf("prop_%v", i), float32(i))
		require.Nil(t, err)
	}

	// Check that property that would cause the internal counter to overflow is not added
	err = tracker.TrackProperty("OVERFLOW", float32(123))
	require.NotNil(t, err)

	require.Nil(t, tracker.Close())
}

// Test that object racking works
func Test_PropertyLengthTracker_ObjectTracking(t *testing.T) {
	dirName := t.TempDir()

	path := path.Join(dirName, "my_test_shard")

	var tracker *JsonShardMetaData

	l := logrus.New()

	t.Run("initializing an empty tracker, no file present", func(t *testing.T) {
		tr, err := NewJsonShardMetaData(path, l)
		require.Nil(t, err)
		tracker = tr
	})

	t.Run("test object tracking", func(t *testing.T) {
		start := tracker.ObjectTally()
		require.Equal(t, start, 0)

		tracker.TrackObjects(1)
		require.Equal(t, tracker.ObjectTally(), 1)

		tracker.TrackObjects(1)
		require.Equal(t, tracker.ObjectTally(), 2)

		tracker.TrackObjects(-1)
		require.Equal(t, tracker.ObjectTally(), 1)

		tracker.TrackObjects(-1)
		require.Equal(t, tracker.ObjectTally(), 0)

		tracker.TrackObjects(2)
		require.Equal(t, tracker.ObjectTally(), 2)

		err := tracker.Close()
		require.Nil(t, err)

		tr, err := NewJsonShardMetaData(path, l)
		require.Nil(t, err)
		tracker = tr

		require.Equal(t, tracker.ObjectTally(), 2)
	})
}
