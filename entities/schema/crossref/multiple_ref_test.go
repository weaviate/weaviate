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

package crossref

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestAsMultipleRef(t *testing.T) {
	const beacon = "weaviate://localhost/Animal/11111111-1111-1111-1111-111111111111"

	cases := []struct {
		name    string
		value   interface{}
		want    models.MultipleRef
		wantErr string
	}{
		{name: "typed", value: models.MultipleRef{{Beacon: beacon}}, want: models.MultipleRef{{Beacon: beacon}}},
		{name: "typed empty", value: models.MultipleRef{}, want: models.MultipleRef{}},
		{name: "empty []interface{} (emptied property read back from storage)", value: []interface{}{}, want: models.MultipleRef{}},
		{name: "untyped beacons", value: []interface{}{map[string]interface{}{"beacon": beacon}}, want: models.MultipleRef{{Beacon: beacon}}},
		{name: "untyped non-map element", value: []interface{}{"x"}, wantErr: "element 0 to be a map"},
		{name: "untyped element without beacon", value: []interface{}{map[string]interface{}{"href": "x"}}, wantErr: `element 0 to have a string "beacon"`},
		{name: "not a list", value: "not-a-ref", wantErr: "got string"},
		{name: "nil", value: nil, wantErr: "got <nil>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := AsMultipleRef(tc.value)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestAsMultipleRef_KeepsAllFields: converting an untyped reference must keep
// every SingleRef field, since callers write converted references back.
func TestAsMultipleRef_KeepsAllFields(t *testing.T) {
	const beacon = "weaviate://localhost/Animal/11111111-1111-1111-1111-111111111111"
	closestLosing := 0.7
	got, err := AsMultipleRef([]interface{}{map[string]interface{}{
		"beacon": beacon,
		"class":  "Animal",
		"href":   "/v1/objects/Animal/11111111-1111-1111-1111-111111111111",
		"classification": map[string]interface{}{
			"winningCount":           float64(3),
			"losingCount":            float64(1),
			"overallCount":           float64(4),
			"closestWinningDistance": 0.2,
			"closestLosingDistance":  closestLosing,
		},
		"schema": map[string]interface{}{"name": "Fido"},
	}})
	require.NoError(t, err)
	require.Len(t, got, 1)

	ref := got[0]
	assert.Equal(t, strfmt.URI(beacon), ref.Beacon)
	assert.Equal(t, strfmt.URI("Animal"), ref.Class)
	assert.Equal(t, strfmt.URI("/v1/objects/Animal/11111111-1111-1111-1111-111111111111"), ref.Href)
	require.NotNil(t, ref.Classification)
	assert.Equal(t, int64(3), ref.Classification.WinningCount)
	assert.Equal(t, int64(1), ref.Classification.LosingCount)
	assert.Equal(t, int64(4), ref.Classification.OverallCount)
	assert.InDelta(t, 0.2, ref.Classification.ClosestWinningDistance, 1e-9)
	require.NotNil(t, ref.Classification.ClosestLosingDistance)
	assert.InDelta(t, closestLosing, *ref.Classification.ClosestLosingDistance, 1e-9)
	assert.Equal(t, map[string]interface{}{"name": "Fido"}, ref.Schema)
}
