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

	"github.com/go-openapi/strfmt"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/aggregation"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestTextAggregator_TopOccurrencesCalculation(t *testing.T) {
	testCases := []struct {
		name                   string
		texts                  []string
		expectedCount          int
		expectedTopOccurrences []aggregation.TextOccurrence
	}{
		{
			name:          "All texts occurring once",
			texts:         []string{"b_occurs1", "c_occurs1", "g_occurs1", "f_occurs1", "a_occurs1", "d_occurs1", "e_occurs1"},
			expectedCount: 7,
			expectedTopOccurrences: []aggregation.TextOccurrence{
				{Value: "a_occurs1", Occurs: 1},
				{Value: "b_occurs1", Occurs: 1},
				{Value: "c_occurs1", Occurs: 1},
				{Value: "d_occurs1", Occurs: 1},
				{Value: "e_occurs1", Occurs: 1},
			},
		},
		{
			name: "All texts occurring different number of times",
			texts: []string{
				"b_occurs2", "e_occurs5", "d_occurs4", "c_occurs3", "g_occurs7", "e_occurs5", "d_occurs4",
				"f_occurs6", "g_occurs7", "c_occurs3", "b_occurs2", "g_occurs7", "f_occurs6", "g_occurs7", "d_occurs4",
				"a_occurs1", "f_occurs6", "g_occurs7", "g_occurs7", "f_occurs6", "d_occurs4", "e_occurs5", "g_occurs7",
				"c_occurs3", "f_occurs6", "e_occurs5", "f_occurs6", "e_occurs5",
			},
			expectedCount: 28,
			expectedTopOccurrences: []aggregation.TextOccurrence{
				{Value: "g_occurs7", Occurs: 7},
				{Value: "f_occurs6", Occurs: 6},
				{Value: "e_occurs5", Occurs: 5},
				{Value: "d_occurs4", Occurs: 4},
				{Value: "c_occurs3", Occurs: 3},
			},
		},
		{
			name: "Some texts occurring same number of times",
			texts: []string{
				"a_occurs4", "b_occurs3", "g_occurs4", "f_occurs3", "a_occurs4", "e_occurs2", "a_occurs4",
				"c_occurs2", "g_occurs4", "f_occurs3", "b_occurs3", "c_occurs2", "a_occurs4", "f_occurs3", "g_occurs4",
				"b_occurs3", "d_occurs1", "e_occurs2", "g_occurs4",
			},
			expectedCount: 19,
			expectedTopOccurrences: []aggregation.TextOccurrence{
				{Value: "a_occurs4", Occurs: 4},
				{Value: "g_occurs4", Occurs: 4},
				{Value: "b_occurs3", Occurs: 3},
				{Value: "f_occurs3", Occurs: 3},
				{Value: "c_occurs2", Occurs: 2},
			},
		},
		{
			name:          "Fewer texts than limit",
			texts:         []string{"b_occurs3", "d_occurs3", "c_occurs1", "d_occurs3", "b_occurs3", "b_occurs3", "a_occurs1", "d_occurs3"},
			expectedCount: 8,
			expectedTopOccurrences: []aggregation.TextOccurrence{
				{Value: "b_occurs3", Occurs: 3},
				{Value: "d_occurs3", Occurs: 3},
				{Value: "a_occurs1", Occurs: 1},
				{Value: "c_occurs1", Occurs: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg := newTextAggregator([]aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(nil)}, true)
			for _, text := range tc.texts {
				agg.AddText(text)
			}

			res := agg.Res()
			assert.Equal(t, tc.expectedCount, res.Count)
			assert.Equal(t, tc.expectedTopOccurrences, res.Items)
		})
	}
}

func TestTextAggregator_TracksOccurrencesOnlyWhenRequested(t *testing.T) {
	limit := 1
	all := []aggregation.TextOccurrence{{Value: "a", Occurs: 2}, {Value: "b", Occurs: 1}}
	testCases := []struct {
		name          string
		aggs          []aggregation.Aggregator
		onlyRequested bool
		wantItems     []aggregation.TextOccurrence
	}{
		{name: "count only", aggs: []aggregation.Aggregator{aggregation.CountAggregator}, onlyRequested: true},
		{name: "no aggregators", onlyRequested: true},
		{
			name:          "top occurrences",
			aggs:          []aggregation.Aggregator{aggregation.CountAggregator, aggregation.NewTopOccurrencesAggregator(&limit)},
			onlyRequested: true,
			wantItems:     []aggregation.TextOccurrence{{Value: "a", Occurs: 2}},
		},
		{name: "count only, caller reads unrequested results", aggs: []aggregation.Aggregator{aggregation.CountAggregator}, wantItems: all},
		{name: "no aggregators, caller reads unrequested results", wantItems: all},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg := newTextAggregator(tc.aggs, tc.onlyRequested)
			for _, text := range []string{"a", "b", "a"} {
				require.NoError(t, agg.AddText(text))
			}

			res := agg.Res()
			assert.Equal(t, 3, res.Count)
			assert.Equal(t, tc.wantItems, res.Items)
		})
	}
}

func TestTextAggregator_RowsCountTheSameWithAndWithoutOccurrences(t *testing.T) {
	rows := []map[string]interface{}{
		{"text": "a", "texts": []interface{}{"a", "b", "c"}},
		{"text": "", "texts": []interface{}{}},
		{"other": "x"},
		{"text": "a", "texts": []interface{}{"a"}},
	}
	encoded := make([][]byte, len(rows))
	for i, props := range rows {
		obj := storobj.FromObject(&models.Object{
			Class:      "C",
			ID:         strfmt.UUID("6b9d8a2c-3a4f-4f4e-9d47-0e5e7c1a2b3c"),
			Properties: props,
		}, []float32{1, 2, 3}, nil, nil)
		b, err := obj.MarshalBinary()
		require.NoError(t, err)
		encoded[i] = b
	}

	testCases := []struct {
		prop      schema.PropertyName
		wantCount int
		wantItems []aggregation.TextOccurrence
	}{
		{prop: "text", wantCount: 3, wantItems: []aggregation.TextOccurrence{{Value: "a", Occurs: 2}, {Value: "", Occurs: 1}}},
		{prop: "texts", wantCount: 4, wantItems: []aggregation.TextOccurrence{{Value: "a", Occurs: 2}, {Value: "b", Occurs: 1}, {Value: "c", Occurs: 1}}},
		{prop: "missing", wantCount: 0},
	}

	for _, tc := range testCases {
		t.Run(string(tc.prop), func(t *testing.T) {
			countOnly := newTextAggregator([]aggregation.Aggregator{aggregation.CountAggregator}, true)
			withOccurrences := newTextAggregator([]aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(nil)}, true)
			for _, row := range encoded {
				require.NoError(t, (&Aggregator{}).parseAndAddTextRow(countOnly, row, tc.prop))
				require.NoError(t, (&Aggregator{}).parseAndAddTextRow(withOccurrences, row, tc.prop))
			}

			assert.Equal(t, aggregation.Text{Count: tc.wantCount}, countOnly.Res())
			assert.Equal(t, aggregation.Text{Count: tc.wantCount, Items: tc.wantItems}, withOccurrences.Res())
		})
	}
}

func TestTextAggregator_CountsConcurrentRows(t *testing.T) {
	obj := storobj.FromObject(&models.Object{
		Class:      "C",
		ID:         strfmt.UUID("6b9d8a2c-3a4f-4f4e-9d47-0e5e7c1a2b3c"),
		Properties: map[string]interface{}{"texts": []interface{}{"a", "b"}},
	}, nil, nil, nil)
	row, err := obj.MarshalBinary()
	require.NoError(t, err)

	const workers, rowsPerWorker = 8, 1000
	agg := newTextAggregator([]aggregation.Aggregator{aggregation.CountAggregator}, true)
	logger, _ := logrustest.NewNullLogger()
	eg := enterrors.NewErrorGroupWrapper(logger)
	for range workers {
		eg.Go(func() error {
			for range rowsPerWorker {
				if err := (&Aggregator{}).parseAndAddTextRow(agg, row, "texts"); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())
	assert.Equal(t, 2*workers*rowsPerWorker, agg.Res().Count)
}
