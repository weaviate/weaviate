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

func TestVectorIndexSlots_CreateBuildsOnce(t *testing.T) {
	t.Run("an existing name is not built again", func(t *testing.T) {
		var v vectorIndexSlots
		first := &MockVectorIndex{}
		require.NoError(t, v.Publish("title", first, nil))

		built := false
		created, err := v.Create("title", func() (VectorIndex, *VectorIndexQueue, error) {
			built = true
			return &MockVectorIndex{}, nil, nil
		})
		require.NoError(t, err)
		assert.False(t, created)
		assert.False(t, built, "build must not run for a name that is taken")
		slot, ok := v.get("title")
		require.True(t, ok)
		assert.Same(t, first, slot.index)
	})

	t.Run("a failed build publishes nothing", func(t *testing.T) {
		var v vectorIndexSlots
		created, err := v.Create("title", func() (VectorIndex, *VectorIndexQueue, error) {
			return nil, nil, assert.AnError
		})
		require.ErrorIs(t, err, assert.AnError)
		assert.False(t, created)
		assert.Equal(t, 0, v.Len())
	})

	t.Run("two concurrent creates of the same name build it once", func(t *testing.T) {
		var v vectorIndexSlots
		building := make(chan struct{})
		finish := make(chan struct{})
		builds := 0

		firstDone := make(chan struct{})
		go func() {
			defer close(firstDone)
			created, err := v.Create("title", func() (VectorIndex, *VectorIndexQueue, error) {
				builds++
				close(building)
				<-finish
				return &MockVectorIndex{}, nil, nil
			})
			assert.NoError(t, err)
			assert.True(t, created)
		}()
		<-building

		// the second create must wait for the first build, then find the slot
		secondDone := make(chan bool, 1)
		go func() {
			created, err := v.Create("title", func() (VectorIndex, *VectorIndexQueue, error) {
				builds++
				return &MockVectorIndex{}, nil, nil
			})
			assert.NoError(t, err)
			secondDone <- created
		}()
		select {
		case <-secondDone:
			t.Fatal("the second create returned while the first was still building")
		case <-time.After(100 * time.Millisecond):
		}

		close(finish)
		<-firstDone
		select {
		case created := <-secondDone:
			assert.False(t, created)
		case <-time.After(5 * time.Second):
			t.Fatal("the second create did not return after the first published")
		}
		assert.Equal(t, 1, builds)
		assert.Equal(t, 1, v.Len())
	})
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

// tornDown is a teardown that records what it was handed.
func tornDown(got *VectorIndex) func(VectorIndex, *VectorIndexQueue) error {
	return func(index VectorIndex, _ *VectorIndexQueue) error {
		*got = index
		return nil
	}
}

func TestVectorIndexSlots_RemoveWithoutLeasesReturnsAtOnce(t *testing.T) {
	var v vectorIndexSlots
	index := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", index, nil))
	logger, _ := test.NewNullLogger()

	var got VectorIndex
	require.NoError(t, v.Remove(context.Background(), "title", logger, tornDown(&got)))
	assert.Same(t, index, got)
	_, ok := v.get("title")
	assert.False(t, ok)

	calls := 0
	require.NoError(t, v.Remove(context.Background(), "title", logger, func(VectorIndex, *VectorIndexQueue) error {
		calls++
		return nil
	}))
	assert.Equal(t, 0, calls, "a slot that is already gone is not torn down again")
}

func TestVectorIndexSlots_RemoveKeepsTheSlotWhenTeardownFails(t *testing.T) {
	var v vectorIndexSlots
	index := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", index, nil))
	logger, _ := test.NewNullLogger()

	err := v.Remove(context.Background(), "title", logger, func(VectorIndex, *VectorIndexQueue) error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	// the shard still owns the pair: a retried drop and shutdown can reach it
	slot, ok := v.get("title")
	require.True(t, ok)
	assert.Same(t, index, slot.index)
	var seen []string
	require.NoError(t, v.ForEach(func(name string, _ VectorIndex, _ *VectorIndexQueue) error {
		seen = append(seen, name)
		return nil
	}))
	assert.Equal(t, []string{"title"}, seen)
	lease, ok := v.Acquire("title")
	require.True(t, ok, "leases resume once the failed removal is abandoned")
	lease.release()

	var got VectorIndex
	require.NoError(t, v.Remove(context.Background(), "title", logger, tornDown(&got)))
	assert.Same(t, index, got)
	assert.Equal(t, 0, v.Len())
}

func TestVectorIndexSlots_ForEachSkipsASlotBeingRemoved(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("a", &MockVectorIndex{}, nil))
	require.NoError(t, v.Publish("b", &MockVectorIndex{}, nil))
	logger, _ := test.NewNullLogger()

	inTeardown := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- v.Remove(context.Background(), "a", logger, func(VectorIndex, *VectorIndexQueue) error {
			close(inTeardown)
			<-finish
			return nil
		})
	}()
	<-inTeardown

	var seen []string
	require.NoError(t, v.ForEach(func(name string, _ VectorIndex, _ *VectorIndexQueue) error {
		seen = append(seen, name)
		return nil
	}))
	assert.Equal(t, []string{"b"}, seen, "a slot mid-teardown is not handed to a walk")

	close(finish)
	require.NoError(t, <-done)
	assert.Equal(t, 1, v.Len())
}

func TestVectorIndexSlots_RemoveWaitsForLeases(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("title", &MockVectorIndex{}, nil))
	logger, _ := test.NewNullLogger()

	slot, ok := v.Acquire("title")
	require.True(t, ok)

	removed := make(chan error, 1)
	go func() {
		removed <- v.Remove(context.Background(), "title", logger, func(VectorIndex, *VectorIndexQueue) error { return nil })
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
	case err := <-removed:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("remove did not complete after the lease was released")
	}
	assert.Equal(t, 0, v.Len())
}

func TestVectorIndexSlots_RemoveProceedsPastTheDeadline(t *testing.T) {
	v := vectorIndexSlots{drainTimeout: 50 * time.Millisecond}
	require.NoError(t, v.Publish("title", &MockVectorIndex{}, nil))
	logger, hook := test.NewNullLogger()

	slot, ok := v.Acquire("title")
	require.True(t, ok)
	t.Cleanup(slot.release)

	start := time.Now()
	err := v.Remove(context.Background(), "title", logger, func(VectorIndex, *VectorIndexQueue) error { return nil })
	require.NoError(t, err, "past the deadline the slot is removed anyway, as the shard-level drop does")
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
		v.Remove(context.Background(), "b", logger, func(VectorIndex, *VectorIndexQueue) error { return nil })
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

// swapTo is a swap that installs index.
func swapTo(index VectorIndex) func(VectorIndex, *VectorIndexQueue) (VectorIndex, error) {
	return func(VectorIndex, *VectorIndexQueue) (VectorIndex, error) { return index, nil }
}

// The switch waits for the leases in flight, holds new ones back meanwhile,
// and hands them the new index once it is done.
func TestVectorIndexSlots_SwitchWaitsForLeases(t *testing.T) {
	var v vectorIndexSlots
	old := &MockVectorIndex{}
	replacement := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", old, nil))

	held, ok := v.Acquire("title")
	require.True(t, ok)

	var swappedFrom VectorIndex
	switched := make(chan error, 1)
	go func() {
		switched <- v.Switch(context.Background(), "title", func(current VectorIndex, _ *VectorIndexQueue) (VectorIndex, error) {
			swappedFrom = current
			return replacement, nil
		})
	}()
	require.Eventually(t, func() bool {
		slot, _ := v.get("title")
		v.mu.RLock()
		defer v.mu.RUnlock()
		return slot.switching != nil
	}, 5*time.Second, 5*time.Millisecond)

	acquired := make(chan *vectorIndexSlot, 1)
	go func() {
		slot, ok := v.Acquire("title")
		assert.True(t, ok)
		acquired <- slot
	}()
	select {
	case <-switched:
		t.Fatal("switch completed while a lease was held")
	case <-acquired:
		t.Fatal("a new lease was handed out during the switch")
	case <-time.After(100 * time.Millisecond):
	}

	held.release()
	require.NoError(t, <-switched)
	assert.Same(t, old, swappedFrom)
	lease := <-acquired
	assert.Same(t, replacement, lease.index, "the waiting lease gets the new index")
	lease.release()
}

// A failed swap keeps the index, and leases resume.
func TestVectorIndexSlots_SwitchKeepsTheIndexWhenSwapFails(t *testing.T) {
	var v vectorIndexSlots
	old := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", old, nil))

	err := v.Switch(context.Background(), "title", func(VectorIndex, *VectorIndexQueue) (VectorIndex, error) {
		return nil, assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	lease, ok := v.Acquire("title")
	require.True(t, ok)
	assert.Same(t, old, lease.index)
	lease.release()
}

// A lease held past the drain timeout fails the switch without swapping,
// and leases resume.
func TestVectorIndexSlots_SwitchGivesUpPastTheDeadline(t *testing.T) {
	v := vectorIndexSlots{drainTimeout: 50 * time.Millisecond}
	old := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", old, nil))
	held, ok := v.Acquire("title")
	require.True(t, ok)
	t.Cleanup(held.release)

	swapped := false
	err := v.Switch(context.Background(), "title", func(VectorIndex, *VectorIndexQueue) (VectorIndex, error) {
		swapped = true
		return &MockVectorIndex{}, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, swapped)

	lease, ok := v.Acquire("title")
	require.True(t, ok)
	assert.Same(t, old, lease.index)
	lease.release()
}

// Switching a missing slot, one being removed, or one already switching fails.
func TestVectorIndexSlots_SwitchRefusals(t *testing.T) {
	var v vectorIndexSlots
	require.ErrorContains(t, v.Switch(context.Background(), "missing", swapTo(&MockVectorIndex{})), "has no index")

	require.NoError(t, v.Publish("title", &MockVectorIndex{}, nil))
	inSwap := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- v.Switch(context.Background(), "title", func(current VectorIndex, _ *VectorIndexQueue) (VectorIndex, error) {
			close(inSwap)
			<-finish
			return current, nil
		})
	}()
	<-inSwap
	require.ErrorContains(t, v.Switch(context.Background(), "title", swapTo(&MockVectorIndex{})), "already switching")
	close(finish)
	require.NoError(t, <-done)

	logger, _ := test.NewNullLogger()
	held, ok := v.Acquire("title")
	require.True(t, ok)
	removed := make(chan error, 1)
	go func() {
		removed <- v.Remove(context.Background(), "title", logger, func(VectorIndex, *VectorIndexQueue) error { return nil })
	}()
	require.Eventually(t, func() bool {
		v.mu.RLock()
		defer v.mu.RUnlock()
		slot, ok := v.slots["title"]
		return ok && slot.closing
	}, 5*time.Second, 5*time.Millisecond)
	require.ErrorContains(t, v.Switch(context.Background(), "title", swapTo(&MockVectorIndex{})), "has no index")
	held.release()
	require.NoError(t, <-removed)
}

// A removal that starts during a switch waits for it, then tears down the
// index the switch installed.
func TestVectorIndexSlots_RemoveWaitsForASwitch(t *testing.T) {
	var v vectorIndexSlots
	replacement := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", &MockVectorIndex{}, nil))
	logger, _ := test.NewNullLogger()

	inSwap := make(chan struct{})
	finish := make(chan struct{})
	switched := make(chan error, 1)
	go func() {
		switched <- v.Switch(context.Background(), "title", func(VectorIndex, *VectorIndexQueue) (VectorIndex, error) {
			close(inSwap)
			<-finish
			return replacement, nil
		})
	}()
	<-inSwap

	var got VectorIndex
	removed := make(chan error, 1)
	go func() {
		removed <- v.Remove(context.Background(), "title", logger, tornDown(&got))
	}()
	select {
	case <-removed:
		t.Fatal("remove completed during the switch")
	case <-time.After(100 * time.Millisecond):
	}

	close(finish)
	require.NoError(t, <-switched)
	require.NoError(t, <-removed)
	assert.Same(t, replacement, got)
	assert.Equal(t, 0, v.Len())
}

// Switching one slot holds back no lease on another.
func TestVectorIndexSlots_SwitchOfOneSlotDoesNotBlockAnother(t *testing.T) {
	var v vectorIndexSlots
	require.NoError(t, v.Publish("a", &MockVectorIndex{}, nil))
	require.NoError(t, v.Publish("b", &MockVectorIndex{}, nil))

	inSwap := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- v.Switch(context.Background(), "a", func(current VectorIndex, _ *VectorIndexQueue) (VectorIndex, error) {
			close(inSwap)
			<-finish
			return current, nil
		})
	}()
	<-inSwap

	lease, ok := v.Acquire("b")
	require.True(t, ok)
	lease.release()

	close(finish)
	require.NoError(t, <-done)
}

// A walk that starts during a switch waits for it and sees the new index:
// write paths walk the queues, and the switch repoints the queue it shares.
func TestVectorIndexSlots_ForEachWaitsForASwitch(t *testing.T) {
	var v vectorIndexSlots
	replacement := &MockVectorIndex{}
	require.NoError(t, v.Publish("title", &MockVectorIndex{}, nil))
	require.NoError(t, v.Publish("body", &MockVectorIndex{}, nil))

	inSwap := make(chan struct{})
	finish := make(chan struct{})
	switched := make(chan error, 1)
	go func() {
		switched <- v.Switch(context.Background(), "title", func(VectorIndex, *VectorIndexQueue) (VectorIndex, error) {
			close(inSwap)
			<-finish
			return replacement, nil
		})
	}()
	<-inSwap

	seen := make(chan map[string]VectorIndex, 1)
	go func() {
		got := map[string]VectorIndex{}
		assert.NoError(t, v.ForEach(func(name string, index VectorIndex, _ *VectorIndexQueue) error {
			got[name] = index
			return nil
		}))
		seen <- got
	}()
	select {
	case <-seen:
		t.Fatal("a walk ran during the switch")
	case <-time.After(100 * time.Millisecond):
	}

	close(finish)
	require.NoError(t, <-switched)
	got := <-seen
	assert.Same(t, replacement, got["title"])
	assert.Len(t, got, 2)
}

