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

package distributedtask

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Exhaustive table tests for pure Task / TaskStatus predicates
// -----------------------------------------------------------------------------
//
// Several pure-function predicates on Task and TaskStatus are load-
// bearing for the ack-barrier and conflict-detection logic but lack
// direct table tests. They appear only transitively through
// manager_test.go and scheduler_*_test.go, so a refactor of the
// predicate alone can flip behavior without any single test failing
// in a way that localizes the cause. Background:
// weaviate/0-weaviate-issues#243.
//
// This file pins each predicate against an enumerated fixture, so a
// future refactor that changes (say) the LocalUnitIDs semantics breaks
// here and not in some distant FSM apply test.

// fixtureTask builds a Task with a controlled unit assignment for the
// table tests below. Two groups (g1, g2), three nodes (n-1, n-2, n-3),
// one unit on n-3 that is "unassigned" (empty NodeID) to exercise the
// edge in NodesWithLocalUnits / LocalUnitIDs.
func fixtureTask() *Task {
	return &Task{
		Status: TaskStatusStarted,
		Units: map[string]*Unit{
			"u-1-g1-n1-done":    {ID: "u-1-g1-n1-done", NodeID: "n-1", GroupID: "g1", Status: UnitStatusCompleted},
			"u-2-g1-n2-failed":  {ID: "u-2-g1-n2-failed", NodeID: "n-2", GroupID: "g1", Status: UnitStatusFailed},
			"u-3-g2-n1-pending": {ID: "u-3-g2-n1-pending", NodeID: "n-1", GroupID: "g2", Status: UnitStatusPending},
			"u-4-g2-n2-prog":    {ID: "u-4-g2-n2-prog", NodeID: "n-2", GroupID: "g2", Status: UnitStatusInProgress},
			"u-5-noGroup-n3":    {ID: "u-5-noGroup-n3", NodeID: "n-3", GroupID: "", Status: UnitStatusCompleted},
			"u-6-g1-unassigned": {ID: "u-6-g1-unassigned", NodeID: "", GroupID: "g1", Status: UnitStatusPending},
		},
	}
}

// TestTaskStatus_IsActive pins the active-vs-terminal classification used
// by the schema MutationGuard (see Manager wiring in [Manager] +
// `store.go:334`) and by every conflict detector registered via
// SetConflictDetectors. A status that's accidentally classified as
// inactive lets concurrent mutations slip past the guard; one that's
// accidentally active blocks legitimate schema writes.
func TestTaskStatus_IsActive(t *testing.T) {
	cases := []struct {
		status TaskStatus
		active bool
	}{
		{TaskStatusStarted, true},
		{TaskStatusPreparing, true},
		{TaskStatusSwapping, true},
		{TaskStatusFinished, false},
		{TaskStatusFailed, false},
		{TaskStatusCancelled, false},
		{TaskStatus("UNKNOWN_FUTURE_STATE"), false},
		{TaskStatus(""), false},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			assert.Equal(t, tc.active, tc.status.IsActive(),
				"%q.IsActive() should be %v", tc.status, tc.active)
		})
	}
}

// TestTaskStatus_IsCoordinationPhase pins the PREPARING/SWAPPING
// classification used by the scheduler's bootstrap pre-mark logic
// (`preMarkTerminalCallbacksLocked`) — every task in a coordination
// phase on restart must NOT have its terminal callbacks replayed
// (because they may not have run yet). Adding a new coordination
// phase (e.g. a future "VALIDATING") without updating this method
// would silently regress that protection.
func TestTaskStatus_IsCoordinationPhase(t *testing.T) {
	cases := []struct {
		status         TaskStatus
		coordination   bool
		shouldBeActive bool // sanity cross-check with IsActive
	}{
		{TaskStatusStarted, false, true},
		{TaskStatusPreparing, true, true},
		{TaskStatusSwapping, true, true},
		{TaskStatusFinished, false, false},
		{TaskStatusFailed, false, false},
		{TaskStatusCancelled, false, false},
		{TaskStatus("UNKNOWN_FUTURE_STATE"), false, false},
		{TaskStatus(""), false, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			assert.Equal(t, tc.coordination, tc.status.IsCoordinationPhase(),
				"%q.IsCoordinationPhase() should be %v", tc.status, tc.coordination)
			// Cross-invariant: a coordination phase is always active.
			// IsCoordinationPhase ⊂ IsActive, by design.
			if tc.coordination {
				assert.True(t, tc.status.IsActive(),
					"%q is a coordination phase but not active; predicates have drifted", tc.status)
			}
		})
	}
}

// TestTask_LocalUnitIDs pins per-node ownership. Production callers that
// rely on the empty-NodeID skip include `NodesWithLocalUnits` (and
// therefore `MissingPostCompletionAckNodes`). A regression that started
// returning unassigned units under any specific node would corrupt the
// ack-barrier predicate.
func TestTask_LocalUnitIDs(t *testing.T) {
	task := fixtureTask()
	cases := []struct {
		node string
		want []string // sorted
	}{
		{"n-1", []string{"u-1-g1-n1-done", "u-3-g2-n1-pending"}},
		{"n-2", []string{"u-2-g1-n2-failed", "u-4-g2-n2-prog"}},
		{"n-3", []string{"u-5-noGroup-n3"}},
		{"n-doesnotexist", nil},
		// Empty NodeID literally matches units with empty NodeID — the
		// load-bearing difference from NodesWithLocalUnits (which skips
		// the unassigned unit, since it can't have a node-side ack).
		// Production callers of LocalUnitIDs always pass a real node ID,
		// but the literal-equality semantics are pinned here so a future
		// "skip empty" refactor doesn't silently change the contract.
		{"", []string{"u-6-g1-unassigned"}},
	}
	for _, tc := range cases {
		t.Run("node="+tc.node, func(t *testing.T) {
			got := task.LocalUnitIDs(tc.node)
			sort.Strings(got)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestTask_LocalGroupUnitIDs pins per-(group,node) ownership — the
// predicate used by [ReindexProvider.OnGroupCompleted] to pick which
// shards' swap to fire on a given node. Empty-NodeID skip matters here
// too because unassigned units must not be triggered on any node.
func TestTask_LocalGroupUnitIDs(t *testing.T) {
	task := fixtureTask()
	cases := []struct {
		group, node string
		want        []string
	}{
		{"g1", "n-1", []string{"u-1-g1-n1-done"}},
		{"g1", "n-2", []string{"u-2-g1-n2-failed"}},
		{"g1", "n-3", nil},
		{"g2", "n-1", []string{"u-3-g2-n1-pending"}},
		{"g2", "n-2", []string{"u-4-g2-n2-prog"}},
		{"", "n-3", []string{"u-5-noGroup-n3"}}, // ungrouped on n-3
		// Empty NodeID matches the unassigned unit when the group also
		// matches — literal-equality semantics, same as LocalUnitIDs.
		{"g1", "", []string{"u-6-g1-unassigned"}},
		{"unknownGroup", "n-1", nil},
	}
	for _, tc := range cases {
		t.Run(tc.group+"/"+tc.node, func(t *testing.T) {
			got := task.LocalGroupUnitIDs(tc.group, tc.node)
			sort.Strings(got)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestTask_NodesWithLocalUnits pins the ack-barrier expected-set: every
// node that owns at least one unit must record a PostCompletionAck.
// Unassigned (empty NodeID) units must NOT contribute to the expected
// set, otherwise the barrier would never satisfy.
func TestTask_NodesWithLocalUnits(t *testing.T) {
	task := fixtureTask()
	got := task.NodesWithLocalUnits()
	sort.Strings(got)
	assert.Equal(t, []string{"n-1", "n-2", "n-3"}, got,
		"unassigned (empty NodeID) unit must NOT contribute to the expected set")

	// Edge: a task with ONLY unassigned units returns an empty slice.
	emptyNodeTask := &Task{Units: map[string]*Unit{
		"u-1": {NodeID: "", Status: UnitStatusPending},
	}}
	assert.Empty(t, emptyNodeTask.NodesWithLocalUnits())

	// Edge: a task with no units returns an empty slice (not nil per
	// constructor preallocation, but `len==0` is the load-bearing
	// invariant for callers checking `len(nodes) == 0`).
	zeroUnitTask := &Task{Units: map[string]*Unit{}}
	assert.Empty(t, zeroUnitTask.NodesWithLocalUnits())
}

// TestTask_MissingPostCompletionAckNodes pins the SWAPPING → FINISHED
// gating predicate. The scheduler's terminal-transition path
// (TaskFinalizer.MarkDistributedTaskFinalized) waits until this returns
// empty; a node whose RunSwapOnShard silently never acked must keep
// this list non-empty so the schema flip cannot commit.
func TestTask_MissingPostCompletionAckNodes(t *testing.T) {
	task := fixtureTask()
	// No acks recorded yet — every node-with-local-units is missing.
	got := task.MissingPostCompletionAckNodes()
	sort.Strings(got)
	assert.Equal(t, []string{"n-1", "n-2", "n-3"}, got)

	// Add one ack. The remaining 2 are missing.
	task.PostCompletionAcks = map[string]PostCompletionAck{
		"n-2": {Success: true},
	}
	got = task.MissingPostCompletionAckNodes()
	sort.Strings(got)
	assert.Equal(t, []string{"n-1", "n-3"}, got)

	// All acked — empty.
	task.PostCompletionAcks["n-1"] = PostCompletionAck{Success: true}
	task.PostCompletionAcks["n-3"] = PostCompletionAck{Success: false, Error: "swap failed"}
	assert.Empty(t, task.MissingPostCompletionAckNodes(),
		"once every node-with-local-units has acked (success or failure), missing must be empty")

	// Sanity: an unexpected node ack (not in NodesWithLocalUnits) does
	// NOT remove anyone from the missing list — but it also doesn't
	// cause a false-missing. Reset to just the n-2 ack and add a
	// nonsense extra.
	task.PostCompletionAcks = map[string]PostCompletionAck{
		"n-2":           {Success: true},
		"never-existed": {Success: true},
	}
	got = task.MissingPostCompletionAckNodes()
	sort.Strings(got)
	assert.Equal(t, []string{"n-1", "n-3"}, got,
		"extraneous acks (from nodes not in NodesWithLocalUnits) must be ignored")
}

// TestTask_AnyPostCompletionAckFailed pins the failure-detector: once
// ANY node records Success=false, the FSM must transition to FAILED.
func TestTask_AnyPostCompletionAckFailed(t *testing.T) {
	task := &Task{
		PostCompletionAcks: map[string]PostCompletionAck{
			"n-1": {Success: true},
			"n-2": {Success: true},
		},
	}
	assert.False(t, task.AnyPostCompletionAckFailed())

	task.PostCompletionAcks["n-3"] = PostCompletionAck{Success: false, Error: "swap failed"}
	assert.True(t, task.AnyPostCompletionAckFailed())

	// Edge: nil map.
	assert.False(t, (&Task{}).AnyPostCompletionAckFailed())
}

// TestTask_AllUnitsTerminal and TestTask_AnyUnitFailed pin the two
// invariants the manager FSM uses to decide post-units state.
func TestTask_AllUnitsTerminal(t *testing.T) {
	cases := []struct {
		name     string
		statuses []UnitStatus
		want     bool
	}{
		{"empty units (vacuously true)", nil, true},
		{"all completed", []UnitStatus{UnitStatusCompleted, UnitStatusCompleted}, true},
		{"all failed", []UnitStatus{UnitStatusFailed, UnitStatusFailed}, true},
		{"mixed completed/failed", []UnitStatus{UnitStatusCompleted, UnitStatusFailed}, true},
		{"one pending", []UnitStatus{UnitStatusCompleted, UnitStatusPending}, false},
		{"one in-progress", []UnitStatus{UnitStatusCompleted, UnitStatusInProgress}, false},
		{"all pending", []UnitStatus{UnitStatusPending, UnitStatusPending}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &Task{Units: map[string]*Unit{}}
			for i, s := range tc.statuses {
				task.Units[unitKey(i)] = &Unit{Status: s}
			}
			assert.Equal(t, tc.want, task.AllUnitsTerminal())
		})
	}
}

func TestTask_AnyUnitFailed(t *testing.T) {
	cases := []struct {
		name     string
		statuses []UnitStatus
		want     bool
	}{
		{"empty units", nil, false},
		{"all completed", []UnitStatus{UnitStatusCompleted, UnitStatusCompleted}, false},
		{"one failed amid completed", []UnitStatus{UnitStatusCompleted, UnitStatusFailed}, true},
		{"all failed", []UnitStatus{UnitStatusFailed, UnitStatusFailed}, true},
		{"one failed amid pending", []UnitStatus{UnitStatusPending, UnitStatusFailed}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &Task{Units: map[string]*Unit{}}
			for i, s := range tc.statuses {
				task.Units[unitKey(i)] = &Unit{Status: s}
			}
			assert.Equal(t, tc.want, task.AnyUnitFailed())
		})
	}
}

// TestTask_AllGroupUnitsTerminal pins the group-scoped variant — the
// predicate that gates [ReindexProvider.OnGroupCompleted] dispatch per
// tenant. A regression that returned true when one unit was still
// in-progress would fire OnGroupCompleted prematurely; one that
// returned false on a fully terminal group would never fire it.
func TestTask_AllGroupUnitsTerminal(t *testing.T) {
	task := fixtureTask()
	// g1: one completed (u-1), one failed (u-2), one PENDING unassigned (u-6).
	// PENDING is not terminal → AllGroupUnitsTerminal must be false.
	assert.False(t, task.AllGroupUnitsTerminal("g1"),
		"g1 has u-6 still PENDING; AllGroupUnitsTerminal must be false")

	// g2: one pending (u-3), one in-progress (u-4). Both non-terminal.
	assert.False(t, task.AllGroupUnitsTerminal("g2"))

	// Mark g2 units terminal — should flip to true.
	task.Units["u-3-g2-n1-pending"].Status = UnitStatusCompleted
	task.Units["u-4-g2-n2-prog"].Status = UnitStatusFailed
	assert.True(t, task.AllGroupUnitsTerminal("g2"))

	// Ungrouped (empty groupID) — only u-5 (COMPLETED) qualifies.
	assert.True(t, task.AllGroupUnitsTerminal(""))

	// Unknown group is vacuously true (no units to check); the
	// production callers always call this AFTER confirming the group
	// exists via Groups(), so this is the right default.
	assert.True(t, task.AllGroupUnitsTerminal("unknownGroup"))
}

// TestTask_Groups pins the distinct-group enumeration. Empty groupID
// MUST be included as a real group ("ungrouped" is a group).
func TestTask_Groups(t *testing.T) {
	task := fixtureTask()
	got := task.Groups()
	sort.Strings(got)
	assert.Equal(t, []string{"", "g1", "g2"}, got,
		"empty groupID is an explicit value, not absence; must be in Groups output")

	// Edge: empty task.
	emptyTask := &Task{Units: map[string]*Unit{}}
	assert.Empty(t, emptyTask.Groups())
}

// TestTask_NodeHasNonTerminalUnits pins the per-node "is there work
// left" check. Production caller: scheduler tick uses this to decide
// whether to attempt restart/resume. Empty-NodeID units are treated as
// "could be on any node" — a deliberate choice documented in the
// method's godoc.
func TestTask_NodeHasNonTerminalUnits(t *testing.T) {
	task := fixtureTask()
	// n-1: u-1 (completed) + u-3 (pending) → has non-terminal.
	assert.True(t, task.NodeHasNonTerminalUnits("n-1"))
	// n-2: u-2 (failed) + u-4 (in-progress) → has non-terminal (in-progress).
	assert.True(t, task.NodeHasNonTerminalUnits("n-2"))
	// n-3: only u-5 (completed). But u-6 is unassigned (empty NodeID),
	// which production treats as "could be on n-3" → has non-terminal.
	assert.True(t, task.NodeHasNonTerminalUnits("n-3"))

	// Mark u-6 terminal — n-3 should now have only terminal work.
	task.Units["u-6-g1-unassigned"].Status = UnitStatusCompleted
	assert.False(t, task.NodeHasNonTerminalUnits("n-3"))

	// Unknown node with unassigned units present: still true because
	// unassigned counts toward any node.
	task.Units["u-6-g1-unassigned"].Status = UnitStatusPending
	assert.True(t, task.NodeHasNonTerminalUnits("never-existed"))

	// All terminal AND no unassigned: false.
	for _, u := range task.Units {
		u.Status = UnitStatusCompleted
	}
	assert.False(t, task.NodeHasNonTerminalUnits("never-existed"))
}

func unitKey(i int) string {
	return "u-" + string(rune('a'+i))
}

// TestTaskStatus_TerminalActiveCoordinationDisjoint is a meta-test
// proving the three predicates form the right partition: every status
// is either terminal, active, or both-zero (the empty/unknown
// catch-all), and IsCoordinationPhase ⊂ IsActive. If a future status
// breaks the partition (e.g. an active terminal status), this test
// fires.
func TestTaskStatus_TerminalActiveCoordinationDisjoint(t *testing.T) {
	all := []TaskStatus{
		TaskStatusStarted, TaskStatusPreparing, TaskStatusSwapping,
		TaskStatusFinished, TaskStatusFailed, TaskStatusCancelled,
	}
	for _, s := range all {
		t.Run(string(s), func(t *testing.T) {
			term := s.IsTerminal()
			act := s.IsActive()
			coord := s.IsCoordinationPhase()
			// terminal XOR active for the defined statuses.
			require.NotEqualf(t, term, act,
				"status %q claims both terminal and active simultaneously — invalid", s)
			// coordination implies active.
			if coord {
				require.Truef(t, act, "coordination %q must be active", s)
			}
		})
	}
}
