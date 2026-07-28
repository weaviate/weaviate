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

package inverted

import (
	"encoding/json"
	"math"
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// enable-* does not backfill the tally, so deleting an object that predates the
// migration subtracts a length never added. The never-tracked guard misses this:
// the bucket map is created by the first post-migration write, and from then on
// the arithmetic is reachable.
func TestUnTrackProperty_ObjectsPredatingTheTallyReadAsAbsent(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tracker, err := NewJsonShardMetaData(path.Join(t.TempDir(), "proplengths"), logger)
	require.NoError(t, err)

	const prop = "title"

	// Post-migration writes: the only objects the tally knows about.
	for range 3 {
		require.NoError(t, tracker.TrackProperty(prop, 8))
	}

	// Deletes of objects written before the migration, carrying real lengths.
	for range 3 {
		require.NoError(t, tracker.UnTrackProperty(prop, 6))
	}
	mean, err := tracker.PropertyMean(prop)
	require.NoError(t, err)
	require.False(t, math.IsInf(float64(mean), 0),
		"a stranded sum over a zero count is ±Inf, which is not NaN and so is averaged in as a real mean")

	// A second round drives the count itself negative. 0/-3 is -0.0 — also not
	// NaN, so it too survives the searcher's validity check and contributes 0 to
	// the average instead of being excluded.
	for range 3 {
		require.NoError(t, tracker.UnTrackProperty(prop, 6))
	}
	mean, err = tracker.PropertyMean(prop)
	require.NoError(t, err)
	require.True(t, math.IsNaN(float64(mean)),
		"an over-subtracted tally must read as absent (NaN), not as a mean the searcher will average in")

	// A property the tally does know about is unaffected by its broken neighbour.
	const healthy = "body"
	for range 4 {
		require.NoError(t, tracker.TrackProperty(healthy, 10))
	}
	healthyMean, err := tracker.PropertyMean(healthy)
	require.NoError(t, err)
	require.InDelta(t, 10.0, float64(healthyMean), 0.001)
}

// The over-subtracted condition persists: once reached, every later untrack
// re-enters it. An unlatched warning is therefore one line per delete for the
// life of the shard, which buries the one occurrence an operator needs.
func TestUnTrackProperty_WarnsOncePerProperty(t *testing.T) {
	logger, hook := test.NewNullLogger()
	tracker, err := NewJsonShardMetaData(path.Join(t.TempDir(), "proplengths"), logger)
	require.NoError(t, err)

	require.NoError(t, tracker.TrackProperty("title", 8))
	for range 20 {
		require.NoError(t, tracker.UnTrackProperty("title", 6))
	}

	warns := 0
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel {
			warns++
		}
	}
	require.Equal(t, 1, warns, "20 untracks past an empty tally must report once, not once per call")
}

// A tally can arrive already impossible from an older binary. sum=-12 over
// count=-2 divides to a plausible 6.0, so nothing downstream can tell it is
// broken -- it has to be caught at load.
func TestNewJsonShardMetaData_ClampsInheritedCorruption(t *testing.T) {
	logger, _ := test.NewNullLogger()
	trackerPath := path.Join(t.TempDir(), "proplengths")

	corrupt := &ShardMetaData{
		BucketedData: map[string]map[int]int{"title": {}},
		SumData:      map[string]int{"title": -12},
		CountData:    map[string]int{"title": -2},
	}
	raw, err := json.Marshal(corrupt)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(trackerPath, raw, 0o644))

	tracker, err := NewJsonShardMetaData(trackerPath, logger)
	require.NoError(t, err)

	mean, err := tracker.PropertyMean("title")
	require.NoError(t, err)
	require.True(t, math.IsNaN(float64(mean)),
		"-12/-2 reads as a plausible 6.0; an inherited impossible tally must read as absent instead")
}
