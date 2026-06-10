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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	DateYearMonthDayHourMinute = "2022-06-16T17:30:"
	DateNanoSecondsTimeZone    = ".451235Z"
)

// TestDateAggregatorCanonicalizesTimezones pins that AddTimestamp stores
// the canonical UTC representation instead of echoing the input string, so
// every aggregation path (filtered object scan, unfiltered array scan,
// columnar row-based) returns identical UTC strings regardless of the
// timezone offset the value was stored with.
func TestDateAggregatorCanonicalizesTimezones(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "positive offset",
			input:    "2022-06-16T22:30:17.451235+05:00",
			expected: "2022-06-16T17:30:17.451235Z",
		},
		{
			name:     "negative offset",
			input:    "2022-06-16T10:00:17.451235-07:30",
			expected: "2022-06-16T17:30:17.451235Z",
		},
		{
			name:     "already UTC stays unchanged",
			input:    "2022-06-16T17:30:17.451235Z",
			expected: "2022-06-16T17:30:17.451235Z",
		},
		{
			// pre-1678 dates overflow UnixNano (2^64 ns ≈ 584.5 years), so
			// the display string must be formatted from the parsed time.Time,
			// never from the epoch round-trip — caught by TestGRPC_Aggregate
			// (1400-01-01 rendered as 1984-07-21).
			name:     "pre-1678 date beyond UnixNano range",
			input:    "1400-01-01T00:00:00+02:00",
			expected: "1399-12-31T22:00:00Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := newDateAggregator()
			assert.Nil(t, agg.AddTimestamp(tt.input))
			agg.buildPairsFromCounts()
			assert.Equal(t, tt.expected, agg.Min())
			assert.Equal(t, tt.expected, agg.Max())
			assert.Equal(t, tt.expected, agg.Mode())
			assert.Equal(t, tt.expected, agg.Median())
		})
	}
}

func TestDateAggregatorPreEpoch(t *testing.T) {
	// Regression test for https://github.com/weaviate/weaviate/issues/11125:
	// maximum returned empty string for pre-1970 DATE values because the
	// initial max sentinel was 0 (epoch), causing negative unix nanoseconds to
	// never satisfy the > comparison.
	earlier := "1969-12-31T23:59:58Z"
	later := "1969-12-31T23:59:59Z"

	agg := newDateAggregator()
	assert.Nil(t, agg.AddTimestamp(earlier))
	assert.Nil(t, agg.AddTimestamp(later))
	agg.buildPairsFromCounts()

	assert.Equal(t, later, agg.Max(), "maximum of two pre-1970 dates should be the later one")
	assert.Equal(t, earlier, agg.Min(), "minimum of two pre-1970 dates should be the earlier one")
}

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
