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

package aggregator

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	DateYearMonthDayHourMinute = "2022-06-16T17:30:"
	DateNanoSecondsTimeZone    = ".451235Z"
)

func TestDateAggregator(t *testing.T) {
	tests := []struct {
		name           string
		seconds        []string
		expectedMedian string
		expectedMode   string
	}{
		{
			name:           "Single value",
			seconds:        []string{"17"},
			expectedMedian: "17",
			expectedMode:   "17",
		},
		{
			name:           "Even number of values",
			seconds:        []string{"18", "18", "20", "25"},
			expectedMedian: "19",
			expectedMode:   "18",
		},
		{
			name:           "Uneven number of values",
			seconds:        []string{"18", "18", "19", "20", "25"},
			expectedMedian: "19",
			expectedMode:   "18",
		},
	}
	names := []string{"AddTimestamp", "AddRow"}
	for _, tt := range tests {
		for _, name := range names { // test two ways of adding the value to the aggregator
			t.Run(tt.name+" "+name, func(t *testing.T) {
				agg := newDateAggregator()
				for _, second := range tt.seconds {
					fullDate := DateYearMonthDayHourMinute + second + DateNanoSecondsTimeZone
					if name == names[0] {
						err := agg.AddTimestamp(fullDate)
						assert.Nil(t, err)
					} else {
						timeParsed, err := time.Parse(time.RFC3339, fullDate)
						assert.Nil(t, err)
						ts := newTimestamp(timeParsed.UnixNano())
						err = agg.addRow(ts, 1)
						assert.Nil(t, err)
					}
				}
				agg.buildPairsFromCounts() // needed to populate all required info
				assert.Equal(t, DateYearMonthDayHourMinute+tt.expectedMedian+DateNanoSecondsTimeZone, agg.Median())
				if len(tt.expectedMode) > 0 { // if there is no value that appears more often than other values
					assert.Equal(t, DateYearMonthDayHourMinute+tt.expectedMode+DateNanoSecondsTimeZone, agg.Mode())
				}
			})
		}
	}
}

// modeIterations is how many times each case recomputes the mode, enough
// to catch a result still depending on map iteration order.
const modeIterations = 200

// referenceMode is the independent oracle: most frequent value, ties broken
// by the smallest value, computed without a map.
func referenceMode(t *testing.T, values []string) string {
	t.Helper()
	type entry struct {
		ts    timestamp
		count uint64
	}
	var entries []entry
	for _, v := range values {
		parsed, err := time.Parse(time.RFC3339Nano, v)
		require.Nil(t, err)
		ts := timestamp{epochNano: parsed.UnixNano(), rfc3339: v}
		found := false
		for i := range entries {
			if entries[i].ts == ts {
				entries[i].count++
				found = true
				break
			}
		}
		if !found {
			entries = append(entries, entry{ts: ts, count: 1})
		}
	}
	require.NotEmpty(t, entries)

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ts.epochNano != entries[j].ts.epochNano {
			return entries[i].ts.epochNano < entries[j].ts.epochNano
		}
		return entries[i].ts.rfc3339 < entries[j].ts.rfc3339
	})

	best := entries[0]
	for _, e := range entries[1:] {
		if e.count > best.count {
			best = e
		}
	}
	return best.ts.rfc3339
}

func hourlyTimestamps(base time.Time, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, newTimestamp(base.Add(time.Duration(i)*time.Hour).UnixNano()).rfc3339)
	}
	return out
}

func repeatValues(values []string, times int) []string {
	out := make([]string, 0, len(values)*times)
	for i := 0; i < times; i++ {
		out = append(out, values...)
	}
	return out
}

// Pins mode determinism on count ties, previously decided by map iteration order.
func TestDateAggregatorModeIsDeterministicOnTies(t *testing.T) {
	base := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	preEpoch := time.Date(1965, 3, 4, 5, 6, 7, 0, time.UTC)

	sixtyFour := hourlyTimestamps(base, 64)
	preEpochValues := hourlyTimestamps(preEpoch, 32)
	crossEpoch := append(append([]string{}, preEpochValues...), sixtyFour...)

	// Seeded so the counts are identical on every run and in CI.
	rnd := rand.New(rand.NewSource(20260725))
	var randomized []string
	for _, v := range hourlyTimestamps(base, 20) {
		for i := 0; i < 1+rnd.Intn(4); i++ {
			randomized = append(randomized, v)
		}
	}

	tests := []struct {
		name   string
		values []string
		// expectedMode is optional; when empty, only referenceMode is checked.
		expectedMode string
	}{
		{
			name:         "single value",
			values:       sixtyFour[:1],
			expectedMode: sixtyFour[0],
		},
		{
			name:         "sixty four values all tied at count one",
			values:       sixtyFour,
			expectedMode: sixtyFour[0],
		},
		{
			name:         "top count shared by three of many",
			values:       append(repeatValues(sixtyFour[10:13], 3), sixtyFour[20:40]...),
			expectedMode: sixtyFour[10],
		},
		{
			name:         "unique winner, no tie",
			values:       append(repeatValues(sixtyFour[30:31], 5), sixtyFour...),
			expectedMode: sixtyFour[30],
		},
		{
			name:         "pre-epoch values all tied",
			values:       preEpochValues,
			expectedMode: preEpochValues[0],
		},
		{
			name:         "pre- and post-epoch values all tied",
			values:       crossEpoch,
			expectedMode: preEpochValues[0],
		},
		{
			// distinct valueCounter keys with identical epochNano; see timestamp.lessThan
			name: "one instant spelled three ways, all tied",
			values: []string{
				"2026-01-01T01:00:00+01:00",
				"2026-01-01T00:00:00.000Z",
				"2026-01-01T00:00:00Z",
			},
			expectedMode: "2026-01-01T00:00:00.000Z",
		},
		{
			name:   "randomized counts over twenty values",
			values: randomized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := referenceMode(t, tt.values)
			if tt.expectedMode != "" {
				require.Equal(t, tt.expectedMode, want, "reference oracle disagrees with the pinned expectation")
			}

			build := func() (string, string) {
				agg := newDateAggregator()
				for _, v := range tt.values {
					require.Nil(t, agg.AddTimestamp(v))
				}
				agg.buildPairsFromCounts()
				return agg.Mode(), agg.Median()
			}

			firstMode, firstMedian := build()
			require.Equal(t, want, firstMode)

			for i := 1; i <= modeIterations; i++ {
				mode, median := build()
				require.Equal(t, want, mode, "mode diverged on iteration %d", i)
				require.Equal(t, firstMedian, median, "median diverged on iteration %d", i)
			}
		})
	}
}

// Pins mode determinism through ShardCombiner.mergeDateProp's shard merge.
func TestDateAggregatorModeIsDeterministicAcrossShardMerge(t *testing.T) {
	values := hourlyTimestamps(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC), 24)

	build := func() (string, string) {
		shards := make([]*dateAggregator, 3)
		for s := range shards {
			agg := newDateAggregator()
			for _, v := range values {
				require.Nil(t, agg.AddTimestamp(v))
			}
			agg.buildPairsFromCounts()
			shards[s] = agg
		}

		combined := shards[0]
		for _, src := range shards[1:] {
			for _, pair := range src.pairs {
				for i := uint64(0); i < pair.count; i++ {
					require.Nil(t, combined.AddTimestamp(pair.value.rfc3339))
				}
			}
			combined.buildPairsFromCounts()
		}
		return combined.Mode(), combined.Median()
	}

	firstMode, firstMedian := build()
	require.Equal(t, values[0], firstMode)

	for i := 1; i <= modeIterations; i++ {
		mode, median := build()
		require.Equal(t, firstMode, mode, "mode diverged on iteration %d", i)
		require.Equal(t, firstMedian, median, "median diverged on iteration %d", i)
	}
}

// Pins that date and numerical mode tiebreaks agree on the same counts.
func TestDateAndNumericalAggregatorsAgreeOnTiebreak(t *testing.T) {
	base := time.Date(2026, 9, 8, 7, 6, 5, 0, time.UTC)
	const n = 32

	countsWith := func(mutate func(counts []uint64)) []uint64 {
		counts := make([]uint64, n)
		for i := range counts {
			counts[i] = 1
		}
		mutate(counts)
		return counts
	}

	tests := []struct {
		name      string
		counts    []uint64
		wantIndex int
	}{
		{
			name:      "every value tied at count one",
			counts:    countsWith(func(c []uint64) {}),
			wantIndex: 0,
		},
		{
			name:      "top count shared by three, lowest index wins",
			counts:    countsWith(func(c []uint64) { c[7], c[18], c[29] = 4, 4, 4 }),
			wantIndex: 7,
		},
		{
			name:      "unique winner",
			counts:    countsWith(func(c []uint64) { c[9], c[21] = 9, 4 }),
			wantIndex: 9,
		},
		{
			name:      "last value ties with the first",
			counts:    countsWith(func(c []uint64) { c[0], c[n-1] = 3, 3 }),
			wantIndex: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timestamps := make([]timestamp, n)
			for j := range timestamps {
				timestamps[j] = newTimestamp(base.Add(time.Duration(j) * time.Hour).UnixNano())
			}

			for i := 1; i <= modeIterations; i++ {
				dateAgg := newDateAggregator()
				numAgg := newNumericalAggregator()
				for j := 0; j < n; j++ {
					require.Nil(t, dateAgg.addRow(timestamps[j], tt.counts[j]))
					// index mirrors timestamp ordering so both aggregators tie-break the same way
					require.Nil(t, numAgg.AddNumberRow(float64(j), tt.counts[j]))
				}
				dateAgg.buildPairsFromCounts()
				numAgg.buildPairsFromCounts()

				numIndex := int(numAgg.Mode())
				require.Equal(t, tt.wantIndex, numIndex,
					fmt.Sprintf("numerical mode picked the wrong value on iteration %d", i))
				require.Equal(t, timestamps[numIndex].rfc3339, dateAgg.Mode(),
					fmt.Sprintf("date and numerical aggregators disagree on iteration %d", i))
			}
		})
	}
}
