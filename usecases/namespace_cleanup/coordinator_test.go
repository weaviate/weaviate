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

package namespacecleanup

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// stubNamespaces returns a fixed deleting list.
type stubNamespaces struct{ deleting []string }

func (s stubNamespaces) ListDeleting() []string { return s.deleting }

// stubSchema returns the configured per-namespace residuals.
type stubSchema struct {
	classes    map[string][]string
	aliases    map[string][]string
	classesErr error
}

func (s stubSchema) ClassesInNamespace(ns string) ([]string, error) {
	if s.classesErr != nil {
		return nil, s.classesErr
	}
	return s.classes[ns], nil
}
func (s stubSchema) AliasesInNamespace(ns string) []string { return s.aliases[ns] }

// recordedCall captures a single RAFT call for ordering assertions.
type recordedCall struct {
	op   string
	arg  string
	from string // namespace context, when applicable
}

// stubRaft records calls and lets tests inject errors per op.
type stubRaft struct {
	calls []recordedCall

	deleteUsersErr   map[string]error
	deleteAliasErr   map[string]error
	deleteClassErr   map[string]error
	removeEntityErr  map[string]error
	removeEntityCall map[string]int
}

func newStubRaft() *stubRaft {
	return &stubRaft{
		deleteUsersErr:   map[string]error{},
		deleteAliasErr:   map[string]error{},
		deleteClassErr:   map[string]error{},
		removeEntityErr:  map[string]error{},
		removeEntityCall: map[string]int{},
	}
}

func (s *stubRaft) DeleteUsersInNamespace(_ context.Context, name string) error {
	s.calls = append(s.calls, recordedCall{op: "users", arg: name, from: name})
	return s.deleteUsersErr[name]
}

func (s *stubRaft) DeleteAlias(_ context.Context, alias string) (uint64, error) {
	s.calls = append(s.calls, recordedCall{op: "alias", arg: alias})
	return 0, s.deleteAliasErr[alias]
}

func (s *stubRaft) DeleteClass(_ context.Context, name string) (uint64, error) {
	s.calls = append(s.calls, recordedCall{op: "class", arg: name})
	return 0, s.deleteClassErr[name]
}

func (s *stubRaft) RemoveNamespaceEntity(_ context.Context, name string) error {
	s.calls = append(s.calls, recordedCall{op: "entity", arg: name, from: name})
	s.removeEntityCall[name]++
	return s.removeEntityErr[name]
}

func newTestCoordinator(t *testing.T,
	nsLister namespaceLister,
	schema schemaLister,
	raft raftExecutor,
	isLeader func() bool,
) *Coordinator {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewCoordinator(nsLister, schema, raft, isLeader, logger)
}

func alwaysLeader() bool { return true }

func TestCoordinator_NewCoordinator_PanicsOnNilArgs(t *testing.T) {
	logger, _ := test.NewNullLogger()
	nsLister := stubNamespaces{}
	schema := stubSchema{}
	raft := newStubRaft()

	tests := []struct {
		name     string
		ns       namespaceLister
		schema   schemaLister
		raft     raftExecutor
		isLeader func() bool
	}{
		{name: "nil namespace lister", ns: nil, schema: schema, raft: raft, isLeader: alwaysLeader},
		{name: "nil schema lister", ns: nsLister, schema: nil, raft: raft, isLeader: alwaysLeader},
		{name: "nil raft executor", ns: nsLister, schema: schema, raft: nil, isLeader: alwaysLeader},
		{name: "nil isLeader", ns: nsLister, schema: schema, raft: raft, isLeader: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Panics(t, func() {
				NewCoordinator(tc.ns, tc.schema, tc.raft, tc.isLeader, logger)
			})
		})
	}
}

func TestCoordinator_Tick_EmptyDeletingSetIsNoop(t *testing.T) {
	raft := newStubRaft()
	c := newTestCoordinator(t, stubNamespaces{}, stubSchema{}, raft, alwaysLeader)
	c.Tick(context.Background())
	assert.Empty(t, raft.calls)
}

func TestCoordinator_Tick_NotLeaderReturnsBeforeAnyCall(t *testing.T) {
	raft := newStubRaft()
	ns := stubNamespaces{deleting: []string{"alpha"}}
	c := newTestCoordinator(t, ns, stubSchema{}, raft, func() bool { return false })
	c.Tick(context.Background())
	assert.Empty(t, raft.calls)
}

func TestCoordinator_Tick_OrderingAcrossPhases(t *testing.T) {
	raft := newStubRaft()
	ns := stubNamespaces{deleting: []string{"alpha"}}
	schema := stubSchema{
		classes: map[string][]string{"alpha": {"alpha:Foo", "alpha:Bar"}},
		aliases: map[string][]string{"alpha": {"alpha:A1", "alpha:A2"}},
	}
	c := newTestCoordinator(t, ns, schema, raft, alwaysLeader)
	c.Tick(context.Background())

	// Expected order: users first; then both aliases; then both classes; then RemoveNamespaceEntity.
	require.Len(t, raft.calls, 6)
	assert.Equal(t, recordedCall{op: "users", arg: "alpha", from: "alpha"}, raft.calls[0])
	assert.Equal(t, "alias", raft.calls[1].op)
	assert.Equal(t, "alias", raft.calls[2].op)
	assert.Equal(t, "class", raft.calls[3].op)
	assert.Equal(t, "class", raft.calls[4].op)
	assert.Equal(t, recordedCall{op: "entity", arg: "alpha", from: "alpha"}, raft.calls[5])
}

func TestCoordinator_Tick_ClassesInNamespaceErrorAbortsNamespace(t *testing.T) {
	raft := newStubRaft()
	wantErr := errors.New("read schema failed")
	ns := stubNamespaces{deleting: []string{"alpha"}}
	schema := stubSchema{classesErr: wantErr}
	c := newTestCoordinator(t, ns, schema, raft, alwaysLeader)

	// Tick logs the per-namespace error and returns nil; RemoveNamespaceEntity
	// must not be issued because we cannot prove the namespace is empty.
	require.NoError(t, c.Tick(context.Background()))
	for _, call := range raft.calls {
		assert.NotEqual(t, "entity", call.op, "RemoveNamespaceEntity must not run after a ClassesInNamespace error")
	}
}

func TestCoordinator_Tick_RemoveEntityNotEmptyIsSwallowed(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "wrapped sentinel", err: fmt.Errorf("apply: %w", namespaces.ErrNamespaceNotEmpty)},
		{name: "string fallback", err: errors.New("apply: " + namespaces.ErrNamespaceNotEmpty.Error())},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raft := newStubRaft()
			raft.removeEntityErr["alpha"] = tc.err
			ns := stubNamespaces{deleting: []string{"alpha"}}
			c := newTestCoordinator(t, ns, stubSchema{}, raft, alwaysLeader)
			// Tick must not panic, must not return; the retry will happen
			// at the next tick.
			require.NoError(t, c.Tick(context.Background()))
			assert.Equal(t, 1, raft.removeEntityCall["alpha"])
		})
	}
}

// TestCoordinator_Tick_LeaderFlipBetweenPhasesAborts walks an isLeader
// sequence: every call returns true except the configured one. The
// coordinator must abort the tick once it sees false.
//
// isLeader call sequence per Tick over a single namespace with N aliases
// and M classes:
//
//	1: Tick top-of-loop guard
//	2: cleanupOne entry guard
//	3..2+N: before each DeleteAlias
//	3+N..2+N+M: before each DeleteClass
//	3+N+M: before RemoveNamespaceEntity
func TestCoordinator_Tick_LeaderFlipBetweenPhasesAborts(t *testing.T) {
	tests := []struct {
		name           string
		falseAtCallNum int // 1-based index of the isLeader call that returns false
		schema         stubSchema
		wantOps        []string // RAFT ops the coordinator must have issued before aborting
	}{
		{
			name:           "flip on Tick guard issues nothing",
			falseAtCallNum: 1,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{},
		},
		{
			name:           "flip on cleanupOne entry issues nothing",
			falseAtCallNum: 2,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{},
		},
		{
			name:           "flip before first alias stops after users",
			falseAtCallNum: 3,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1", "alpha:A2"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{"users"},
		},
		{
			name:           "flip before second alias stops after first alias",
			falseAtCallNum: 4,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1", "alpha:A2"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{"users", "alias"},
		},
		{
			name:           "flip before first class stops after aliases",
			falseAtCallNum: 4,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1"}},
				classes: map[string][]string{"alpha": {"alpha:Foo", "alpha:Bar"}},
			},
			wantOps: []string{"users", "alias"},
		},
		{
			name:           "flip before remove entity stops after classes",
			falseAtCallNum: 5,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{"users", "alias", "class"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raft := newStubRaft()
			ns := stubNamespaces{deleting: []string{"alpha"}}
			calls := 0
			isLeader := func() bool {
				calls++
				return calls != tc.falseAtCallNum
			}
			c := newTestCoordinator(t, ns, tc.schema, raft, isLeader)
			require.NoError(t, c.Tick(context.Background()))

			assert.Equal(t, tc.wantOps, opsOf(raft.calls))
			assert.NotContains(t, opsOf(raft.calls), "entity",
				"RemoveNamespaceEntity must not run on a flipped tick")
		})
	}
}

func TestCoordinator_Tick_PerNamespaceErrorContinuesToNext(t *testing.T) {
	raft := newStubRaft()
	raft.deleteUsersErr["alpha"] = errors.New("boom")
	ns := stubNamespaces{deleting: []string{"alpha", "beta"}}
	schema := stubSchema{}
	c := newTestCoordinator(t, ns, schema, raft, alwaysLeader)
	c.Tick(context.Background())

	// alpha aborts after the failed users call (no aliases, no classes,
	// no entity removal). beta proceeds to entity removal.
	assert.Contains(t, opsOf(raft.calls), "entity")
	assert.Equal(t, 1, raft.removeEntityCall["beta"])
	assert.Equal(t, 0, raft.removeEntityCall["alpha"])
}

func TestCoordinator_Tick_NotLeaderInsidePhaseAbortsWholeTick(t *testing.T) {
	raft := newStubRaft()
	raft.deleteUsersErr["alpha"] = types.ErrNotLeader
	ns := stubNamespaces{deleting: []string{"alpha", "beta"}}
	c := newTestCoordinator(t, ns, stubSchema{}, raft, alwaysLeader)
	c.Tick(context.Background())

	// beta must not be processed.
	for _, call := range raft.calls {
		assert.NotEqual(t, "beta", call.from, "tick should have aborted before reaching beta")
	}
}

// TestCoordinator_Tick_RejectsConcurrentRun forces a Tick to overlap with
// itself by holding the ongoing flag set. The second call must short-
// circuit with an error and must not enter cleanup.
func TestCoordinator_Tick_RejectsConcurrentRun(t *testing.T) {
	raft := newStubRaft()
	ns := stubNamespaces{deleting: []string{"alpha"}}
	c := newTestCoordinator(t, ns, stubSchema{}, raft, alwaysLeader)

	c.ongoing.Store(true)
	defer c.ongoing.Store(false)

	err := c.Tick(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already ongoing")
	assert.Empty(t, raft.calls, "Tick must not enter cleanup while another run is ongoing")
}

func opsOf(calls []recordedCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.op)
	}
	return out
}
