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
	}
	names := []string{"AddTimestamp", "AddRow"}
	for _, tt := range tests {
		for i := 0; i < 2; i++ { // test two ways of adding the value to the aggregator
			t.Run(tt.name+" "+names[i], func(t *testing.T) {
				agg := newDateAggregator()
				for _, second := range tt.seconds {
					fullDate := DateYearMonthDayHourMinute + second + DateNanoSecondsTimeZone
					if i == 0 {
						agg.AddTimestamp(fullDate)
					} else {
						timeParsed, ok := time.Parse(time.RFC3339, fullDate)
						assert.Nil(t, ok)
						ts := newTimestamp(timeParsed.UnixNano())
						agg.addRow(ts, 1)
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
