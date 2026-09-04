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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/modelsext"
)

func TestVectorIndexSlots_Publish(t *testing.T) {
	var v vectorIndexSlots
	named := &MockVectorIndex{}
	legacy := &MockVectorIndex{}

	require.NoError(t, v.Publish("title", named, nil))
	require.NoError(t, v.Publish("", legacy, nil))
	assert.Equal(t, 2, v.Len())

	err := v.Publish("title", &MockVectorIndex{}, nil)
	require.ErrorIs(t, err, errVectorIndexSlotExists,
		"publishing a taken name must fail, or a concurrent double-create would orphan an index and queue")
	assert.Equal(t, 2, v.Len())
}

func TestVectorIndexSlots_LookupResolvesTheLegacyAlias(t *testing.T) {
	tests := []struct {
		name       string
		published  []string
		lookup     string
		wantFound  bool
		wantSlotAs string
	}{
		{name: "empty name is the legacy slot", published: []string{""}, lookup: "", wantFound: true, wantSlotAs: ""},
		{name: "default alias reaches the legacy slot", published: []string{""}, lookup: modelsext.DefaultNamedVectorName, wantFound: true, wantSlotAs: ""},
		{name: "default alias without a legacy slot is a named lookup", published: []string{"title"}, lookup: modelsext.DefaultNamedVectorName, wantFound: false},
		{name: "named lookup", published: []string{"", "title"}, lookup: "title", wantFound: true, wantSlotAs: "title"},
		{name: "unknown name", published: []string{"", "title"}, lookup: "other", wantFound: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v vectorIndexSlots
			for _, name := range tt.published {
				require.NoError(t, v.Publish(name, &MockVectorIndex{}, nil))
			}
			slot, ok := v.get(tt.lookup)
			require.Equal(t, tt.wantFound, ok)
			if ok {
				assert.Equal(t, tt.wantSlotAs, slot.name)
			}
		})
	}
}

func TestVectorIndexSlots_ForEachVisitsNamedThenLegacy(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("", &MockVectorIndex{}, nil))
	require.NoError(t, v.Publish("a", &MockVectorIndex{}, nil))
	require.NoError(t, v.Publish("b", &MockVectorIndex{}, nil))

	var seen []string
	err := v.ForEach(func(name string, _ VectorIndex, _ *VectorIndexQueue) error {
		seen = append(seen, name)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, seen, 3)
	assert.Equal(t, "", seen[2], "the legacy slot is visited last, as the old accessors did")
	assert.ElementsMatch(t, []string{"a", "b"}, seen[:2])
}

func TestVectorIndexSlots_ForEachStopsAtTheFirstError(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("a", &MockVectorIndex{}, nil))
	require.NoError(t, v.Publish("b", &MockVectorIndex{}, nil))

	calls := 0
	err := v.ForEach(func(string, VectorIndex, *VectorIndexQueue) error {
		calls++
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 1, calls)
}

func TestVectorIndexSlots_ZeroValueIsUsable(t *testing.T) {
	var v vectorIndexSlots
	_, ok := v.get("")
	assert.False(t, ok)
	assert.Equal(t, 0, v.Len())
	require.NoError(t, v.ForEach(func(string, VectorIndex, *VectorIndexQueue) error { return nil }))
}

func TestVectorIndexSlots_Replace(t *testing.T) {
	var v vectorIndexSlots
	old := &MockVectorIndex{}
	replacement := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", old, nil))

	assert.True(t, v.Replace("title", replacement))
	slot, ok := v.get("title")
	require.True(t, ok)
	assert.Same(t, replacement, slot.index)

	assert.False(t, v.Replace("missing", replacement), "replacing an absent slot installs nothing")
	assert.Equal(t, 1, v.Len())
}

func TestVectorIndexSlots_RemoveTakesTheSlotOut(t *testing.T) {
	var v vectorIndexSlots
	index := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", index, nil))

	gotIndex, _, ok := v.remove("title")
	require.True(t, ok)
	assert.Same(t, index, gotIndex)
	_, ok = v.get("title")
	assert.False(t, ok)

	_, _, ok = v.remove("title")
	assert.False(t, ok, "removing twice reports the slot as already gone")
}
