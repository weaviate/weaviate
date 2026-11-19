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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

// Pins that a filtered aggregation lists every beacon of a reference property in
// pointingTo, decoded as docid.ScanObjectsLSM decodes it.
func TestFilteredAggregator_ReferenceProperty(t *testing.T) {
	const (
		beaconA = "weaviate://localhost/Target/73f4eb5f-5abf-447a-81ca-74b1dd168247"
		beaconB = "weaviate://localhost/Target/73f4eb5f-5abf-447a-81ca-74b1dd168248"
	)

	tests := []struct {
		name           string
		stored         string // the stored JSON of the property
		wantPointingTo []string
	}{
		{
			name:           "several references",
			stored:         `[{"beacon":"` + beaconA + `"},{"beacon":"` + beaconB + `"}]`,
			wantPointingTo: []string{beaconA, beaconB},
		},
		{
			name: "references with href and classification meta",
			stored: `[{"beacon":"` + beaconA + `","href":"/v1/objects/Target/1",` +
				`"classification":{"overallCount":3}},{"beacon":"` + beaconB + `"}]`,
			wantPointingTo: []string{beaconA, beaconB},
		},
		{
			name:           "empty reference list",
			stored:         `[]`,
			wantPointingTo: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			props := map[string]interface{}{}
			require.NoError(t, storobj.UnmarshalProperties([]byte(`{"ref":`+tt.stored+`}`),
				props, [][]string{{"ref"}}))

			prop := propAgg{
				aggType:  aggregation.PropertyTypeReference,
				dataType: schema.DataTypeCRef,
				refAgg:   newRefAggregator(),
			}
			require.NoError(t, (&filteredAggregator{}).addPropValue(prop, props["ref"]))
			assert.ElementsMatch(t, tt.wantPointingTo, prop.refAgg.PointingTo())
		})
	}
}

// Pins that a filtered aggregation skips a property stored as null, which
// UnmarshalProperties decodes as nil.
func TestFilteredAggregator_NullProperty(t *testing.T) {
	const beacon = "weaviate://localhost/Target/73f4eb5f-5abf-447a-81ca-74b1dd168247"

	tests := []struct {
		name           string
		stored         string
		wantPointingTo []string
	}{
		{
			name:           "null property before a reference",
			stored:         `{"text":null,"ref":[{"beacon":"` + beacon + `"}]}`,
			wantPointingTo: []string{beacon},
		},
		{
			name:           "null reference",
			stored:         `{"text":"a","ref":null}`,
			wantPointingTo: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			props := map[string]interface{}{}
			require.NoError(t, storobj.UnmarshalProperties([]byte(tt.stored),
				props, [][]string{{"text"}, {"ref"}}))

			aggs := propAggs{
				"text": {aggType: aggregation.PropertyTypeText, dataType: schema.DataTypeText},
				"ref":  {aggType: aggregation.PropertyTypeReference, dataType: schema.DataTypeCRef},
			}
			for name, agg := range aggs {
				agg.initAggregator()
				aggs[name] = agg
			}

			var properties models.PropertySchema = props
			require.NoError(t, (&filteredAggregator{}).AnalyzeObject(context.Background(), &properties, aggs))
			assert.ElementsMatch(t, tt.wantPointingTo, aggs["ref"].refAgg.PointingTo())
		})
	}
}
