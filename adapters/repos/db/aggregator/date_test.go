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
