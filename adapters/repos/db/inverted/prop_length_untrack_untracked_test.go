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
// Table rather than a single case: relaxing the healthy guard to count >= 0
// silences the legitimately-empty row and simultaneously re-admits (50,0),
// whose mean is +Inf. Both directions have to be pinned or the next reader
// makes that one-character change with the suite still green.
func TestNewJsonShardMetaData_ClampsInheritedCorruption(t *testing.T) {
	tests := []struct {
		name        string
		sum, count  int
		wantClamped bool
	}{
		{name: "negative pair divides to a plausible 6.0", sum: -12, count: -2, wantClamped: true},
		{name: "sum without count is +Inf", sum: 50, count: 0, wantClamped: true},
		{name: "negative sum", sum: -12, count: 2, wantClamped: true},
		{name: "negative count", sum: 50, count: -2, wantClamped: true},
		{name: "healthy is left alone", sum: 50, count: 5, wantClamped: false},
		{name: "all objects deleted is legal, not corrupt", sum: 0, count: 0, wantClamped: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			trackerPath := path.Join(t.TempDir(), "proplengths")
			raw, err := json.Marshal(&ShardMetaData{
				BucketedData: map[string]map[int]int{"title": {}},
				SumData:      map[string]int{"title": tt.sum},
				CountData:    map[string]int{"title": tt.count},
			})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(trackerPath, raw, 0o644))

			tracker, err := NewJsonShardMetaData(trackerPath, logger)
			require.NoError(t, err)
			mean, err := tracker.PropertyMean("title")
			require.NoError(t, err)

			warns := 0
			for _, e := range hook.AllEntries() {
				if e.Level == logrus.WarnLevel {
					warns++
				}
			}

			if tt.wantClamped {
				require.True(t, math.IsNaN(float64(mean)),
					"an impossible tally must read as absent, not as a mean the searcher will average in")
				require.Equal(t, 1, warns, "clamping a corrupt tally must say so exactly once")
				return
			}
			require.Equal(t, 0, warns, "a legal tally must not be reported as corrupt")
			if tt.count > 0 {
				require.InDelta(t, float64(tt.sum)/float64(tt.count), float64(mean), 0.001,
					"a healthy tally must survive the load untouched")
			}
		})
	}
}

// A file carrying an explicit null leaves one map nil. The repair writes to
// both, so a nil-map write would panic out of the load -- and because recover
// returns before the repair is flushed, the file would stay as it was and every
// later restart would fail identically. Weaviate never writes null itself, so
// this is only reachable from a file it did not produce, which is exactly what
// the clamp is for.
func TestNewJsonShardMetaData_NullMapDoesNotFailTheLoad(t *testing.T) {
	tests := []struct {
		name, raw string
		wantNaN   bool
	}{
		{"null counts against a corrupt sum", `{"BucketedData":{},"SumData":{"title":-12},"CountData":null}`, true},
		{"null sums against a corrupt count", `{"BucketedData":{},"SumData":null,"CountData":{"title":-2}}`, true},
		{"null counts against a healthy sum", `{"BucketedData":{},"SumData":{"title":50},"CountData":null}`, true},
		{"both null", `{"BucketedData":{},"SumData":null,"CountData":null}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			trackerPath := path.Join(t.TempDir(), "proplengths")
			require.NoError(t, os.WriteFile(trackerPath, []byte(tt.raw), 0o644))

			tracker, err := NewJsonShardMetaData(trackerPath, logger)
			require.NoError(t, err, "a null map must not fail the load: the recover path skips the repair flush, so the failure would repeat on every restart")
			require.NotNil(t, tracker)

			mean, err := tracker.PropertyMean("title")
			require.NoError(t, err)
			if tt.wantNaN {
				require.True(t, math.IsNaN(float64(mean)),
					"a repaired tally must read as absent; NaN < 0 is false, so a >= 0 check would accept +Inf and a stale value alike")
			}

			// Surviving the load is not the fix. The recover path returns before
			// the flush, so a repair that computes but never reaches disk leaves
			// the next restart in the same state -- which was the whole defect.
			onDisk, err := os.ReadFile(trackerPath)
			require.NoError(t, err)
			var reread ShardMetaData
			require.NoError(t, json.Unmarshal(onDisk, &reread))
			require.NotNil(t, reread.CountData, "the repair must reach disk, or every restart re-enters the same state")
			require.NotNil(t, reread.SumData, "the repair must reach disk, or every restart re-enters the same state")

			logger2, hook2 := test.NewNullLogger()
			_, err = NewJsonShardMetaData(trackerPath, logger2)
			require.NoError(t, err)
			warns := 0
			for _, e := range hook2.AllEntries() {
				if e.Level == logrus.WarnLevel {
					warns++
				}
			}
			require.Equal(t, 0, warns, "a reload of the repaired file must be clean: a warning here means the repair did not persist")
		})
	}
}
