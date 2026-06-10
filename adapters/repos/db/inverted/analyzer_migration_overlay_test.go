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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// Behavior-pinning test for weaviate/weaviate#11688 (analyzer layer).
//
// The analyzer itself drops a property with no enabled inverted index
// unless a Force* overlay is supplied — BOTH before and after the
// shard-layer fix. The fix lives one layer up: Shard.AnalyzeObject now
// merges the active migration's Force* overlay into live-write analysis
// (see Shard.liveAnalyzerOverlay), so the live path supplies exactly the
// overlay leg 2 below exercises. The three legs document the mechanism:
//
//  1. no overlay → prop dropped (this is what the pre-fix live path did
//     for the whole migration window — the double-write callbacks never
//     saw the prop and every write was lost from the target index)
//  2. ForceRangeable overlay → prop analyzed with HasRangeableIndex=true
//     and ForcedViaOverlay=true (the backfill path always did this; the
//     fixed live path now does too while a migration is active)
//  3. indexFilterable=true → prop analyzed without any overlay (why the
//     bug only hit props with no other enabled index)
func TestAnalyzer_MigratingPropDroppedWithoutOverlay(t *testing.T) {
	vFalse, vTrue := false, true
	props := []*models.Property{{
		Name:              "vint",
		DataType:          schema.DataTypeInt.PropString(),
		IndexFilterable:   &vFalse,
		IndexRangeFilters: &vFalse,
		IndexSearchable:   &vFalse,
	}}
	input := map[string]any{"vint": float64(777777)}

	contains := func(out []Property) bool {
		for i := range out {
			if out[i].Name == "vint" {
				return true
			}
		}
		return false
	}

	t.Run("live write path (no overlay) drops the migrating prop", func(t *testing.T) {
		a := NewAnalyzer(nil, "F10")
		out, _, err := a.Object(input, props, "00000000-0000-0000-0000-000000000001")
		require.NoError(t, err)
		require.False(t, contains(out),
			"expected vint to be DROPPED on the live path — if this fails, the dead-callback hypothesis is falsified")
	})

	t.Run("backfill path (ForceRangeable overlay) analyzes it", func(t *testing.T) {
		a := NewAnalyzer(nil, "F10").WithSchemaOverlay(map[string]PropertyOverlay{
			"vint": {ForceRangeable: true},
		})
		out, _, err := a.Object(input, props, "00000000-0000-0000-0000-000000000001")
		require.NoError(t, err)
		require.True(t, contains(out), "overlay-forced analysis must emit the prop")
		for i := range out {
			if out[i].Name == "vint" {
				require.True(t, out[i].HasRangeableIndex)
				require.True(t, out[i].ForcedViaOverlay,
					"a prop emitted ONLY because of the overlay must be marked so the "+
						"write path can skip aux buckets that don't exist for it")
			}
		}
	})

	t.Run("indexFilterable=true protects the live path", func(t *testing.T) {
		propsFilterable := []*models.Property{{
			Name:              "vint",
			DataType:          schema.DataTypeInt.PropString(),
			IndexFilterable:   &vTrue,
			IndexRangeFilters: &vFalse,
			IndexSearchable:   &vFalse,
		}}
		a := NewAnalyzer(nil, "F10")
		out, _, err := a.Object(input, propsFilterable, "00000000-0000-0000-0000-000000000001")
		require.NoError(t, err)
		require.True(t, contains(out), "filterable-on must keep the prop analyzed (the double-write callback then fires)")
		for i := range out {
			if out[i].Name == "vint" {
				require.False(t, out[i].ForcedViaOverlay,
					"a prop with a live-enabled index is NOT overlay-forced — aux writes must proceed")
			}
		}
	})
}
