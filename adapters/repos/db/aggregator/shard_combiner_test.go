//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregator

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/stretchr/testify/assert"
)

const (
	YearMonthDayHourMinute = "2022-06-16T18:30:"
	NanoSecondsTimeZone    = ".451235Z"
)

type TestStruct struct {
	name            string
	dates1          []string
	dates2          []string
	expectedMedian  string
	expectedMaximum string
	expectedMode    string
	expectedMinimum string
}

func TestShardCombinerMergeDates(t *testing.T) {
	tests := []TestStruct{
		{
			name:            "Many values",
			dates1:          []string{"55", "26", "10"},
			dates2:          []string{"15", "26", "45", "26"},
			expectedMaximum: "55",
			expectedMinimum: "10",
			expectedMedian:  "26",
			expectedMode:    "26",
		},
		{
			name:            "Struct with single element",
			dates1:          []string{"45"},
			dates2:          []string{"00", "26", "45", "27"},
			expectedMaximum: "45",
			expectedMinimum: "00",
			expectedMedian:  "27",
			expectedMode:    "45",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDates(t, tt.dates1, tt.dates2, tt)
			testDates(t, tt.dates2, tt.dates1, tt)
		})
	}
}

func testDates(t *testing.T, dates1, dates2 []string, tt TestStruct) {
	sc := NewShardCombiner()
	dateMap1 := createDateAgg(dates1)
	dateMap2 := createDateAgg(dates2)

	sc.mergeDateProp(dateMap1, dateMap2)
	sc.finalizeDateProp(dateMap1)
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMinimum+NanoSecondsTimeZone, dateMap1["minimum"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMaximum+NanoSecondsTimeZone, dateMap1["maximum"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMedian+NanoSecondsTimeZone, dateMap1["median"])
	assert.Equal(t, int64(len(tt.dates1)+len(tt.dates2)), dateMap1["count"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMode+NanoSecondsTimeZone, dateMap1["mode"])
}

func createDateAgg(dates []string) map[string]interface{} {
	agg := newDateAggregator()
	for _, date := range dates {
		agg.AddTimestamp(YearMonthDayHourMinute + date + NanoSecondsTimeZone)
	}
	agg.buildPairsFromCounts() // needed to populate all required info

	prop := aggregation.Property{}
	aggs := []aggregation.Aggregator{aggregation.MedianAggregator, aggregation.MinimumAggregator, aggregation.MaximumAggregator, aggregation.CountAggregator, aggregation.ModeAggregator}
	addDateAggregations(&prop, aggs, agg)
	return prop.DateAggregations
}
