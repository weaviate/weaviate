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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// Regression: a reference property arriving as generic JSON ([]interface{} of
// beacon maps) — as it does for an object decoded during async-replication
// repair — must be indexed into the filterable bucket, not silently skipped.
func TestAnalyzer_ReferenceFromUntypedBeaconsIsIndexed(t *testing.T) {
	filterable := true
	refProp := &models.Property{
		Name:            "wroteBooks",
		DataType:        []string{"Books"},
		IndexFilterable: &filterable,
	}
	a := NewAnalyzer(nil, "Authors")
	uuid := strfmt.UUID("33333333-3333-3333-3333-333333333333")

	hasRefValueProp := func(props []Property) bool {
		for _, p := range props {
			if p.Name == "wroteBooks" {
				return true
			}
		}
		return false
	}

	t.Run("populated []interface{} beacons are indexed", func(t *testing.T) {
		input := map[string]any{"wroteBooks": []any{
			map[string]interface{}{"beacon": "weaviate://localhost/Books/11111111-1111-1111-1111-111111111111"},
			map[string]interface{}{"beacon": "weaviate://localhost/Books/22222222-2222-2222-2222-222222222222"},
		}}
		props, nestedProps, err := a.Object(input, []*models.Property{refProp}, uuid)
		require.NoError(t, err)
		require.Empty(t, nestedProps)
		require.True(t, hasRefValueProp(props),
			"populated []interface{} ref must be indexed into the filterable bucket")
	})

	t.Run("empty []interface{} is treated as no refs", func(t *testing.T) {
		input := map[string]any{"wroteBooks": []any{}}
		props, nestedProps, err := a.Object(input, []*models.Property{refProp}, uuid)
		require.NoError(t, err)
		require.Empty(t, nestedProps)
		require.False(t, hasRefValueProp(props), "empty ref must not produce a ref value entry")
	})
}
