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
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
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

func TestVectorIndexSlots_RemoveWithoutLeasesReturnsAtOnce(t *testing.T) {
	var v vectorIndexSlots
	index := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", index, nil))
	logger, _ := test.NewNullLogger()

	gotIndex, _, ok := v.Remove(context.Background(), "title", logger)
	require.True(t, ok)
	assert.Same(t, index, gotIndex)
	_, ok = v.get("title")
	assert.False(t, ok)

	_, _, ok = v.Remove(context.Background(), "title", logger)
	assert.False(t, ok, "removing twice reports the slot as already gone")
}

func TestVectorIndexSlots_RemoveWaitsForLeases(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("title", &MockVectorIndex{}, nil))
	logger, _ := test.NewNullLogger()

	slot, ok := v.Acquire("title")
	require.True(t, ok)

	removed := make(chan bool, 1)
	go func() {
		_, _, ok := v.Remove(context.Background(), "title", logger)
		removed <- ok
	}()

	select {
	case <-removed:
		t.Fatal("remove completed while a lease was held")
	case <-time.After(100 * time.Millisecond):
	}

	_, ok = v.Acquire("title")
	assert.False(t, ok, "no new lease once a removal has started")

	slot.release()
	select {
	case ok := <-removed:
		assert.True(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("remove did not complete after the lease was released")
	}
}

func TestVectorIndexSlots_RemoveProceedsPastTheDeadline(t *testing.T) {
	v := vectorIndexSlots{drainTimeout: 50 * time.Millisecond}
	require.NoError(t, v.Publish("title", &MockVectorIndex{}, nil))
	logger, hook := test.NewNullLogger()

	slot, ok := v.Acquire("title")
	require.True(t, ok)
	t.Cleanup(slot.release)

	start := time.Now()
	_, _, ok = v.Remove(context.Background(), "title", logger)
	require.True(t, ok, "past the deadline the slot is removed anyway, as the shard-level drop does")
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	_, ok = v.get("title")
	assert.False(t, ok)

	var logged bool
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "drop_vector_index" && entry.Data["in_use"] == int64(1) {
			logged = true
		}
	}
	assert.True(t, logged, "the leftover lease count is logged")
}

func TestVectorIndexSlots_RemoveOfOneSlotDoesNotWaitForAnother(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("a", &MockVectorIndex{}, nil))
	require.NoError(t, v.Publish("b", &MockVectorIndex{}, nil))
	logger, _ := test.NewNullLogger()

	slot, ok := v.Acquire("a")
	require.True(t, ok)
	defer slot.release()

	done := make(chan struct{})
	go func() {
		v.Remove(context.Background(), "b", logger)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("removing b waited for a lease on a")
	}
}

// BenchmarkVectorIndexSlots_Acquire is the cost a write or a search pays per
// use on top of the old map lookup: the lease's increment and decrement.
func BenchmarkVectorIndexSlots_Acquire(b *testing.B) {
	var v vectorIndexSlots
	if err := v.Publish("title", &MockVectorIndex{}, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slot, ok := v.Acquire("title")
			if !ok {
				b.Fatal("slot missing")
			}
			slot.release()
		}
	})
}

// BenchmarkVectorIndexSlots_Get is the old lookup alone, for the comparison.
func BenchmarkVectorIndexSlots_Get(b *testing.B) {
	var v vectorIndexSlots
	if err := v.Publish("title", &MockVectorIndex{}, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, ok := v.get("title"); !ok {
				b.Fatal("slot missing")
			}
		}
	})
}

func TestVectorIndexSlots_AcquireCountsLeases(t *testing.T) {
	var v vectorIndexSlots
	index := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", index, nil))

	slot, ok := v.Acquire("title")
	require.True(t, ok)
	assert.Same(t, index, slot.index)
	assert.Equal(t, int64(1), slot.users.Count())

	_, ok = v.Acquire("title")
	require.True(t, ok)
	assert.Equal(t, int64(2), slot.users.Count())

	slot.release()
	slot.release()
	assert.Equal(t, int64(0), slot.users.Count())

	_, ok = v.Acquire("missing")
	assert.False(t, ok)
}

func TestVectorIndexSlots_AcquireResolvesTheLegacyAlias(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("", &MockVectorIndex{}, nil))

	slot, ok := v.Acquire(modelsext.DefaultNamedVectorName)
	require.True(t, ok)
	defer slot.release()
	assert.Equal(t, "", slot.name)
}

func TestVectorIndexSlots_ReleaseTwicePanics(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("title", &MockVectorIndex{}, nil))
	slot, ok := v.Acquire("title")
	require.True(t, ok)
	slot.release()
	assert.Panics(t, slot.release, "a double release would let a drop proceed under a live user")
}
