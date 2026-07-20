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

package namespaces

import (
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

func newTestController(t *testing.T) *Controller {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewController(logger)
}

// createIndex and seedIndex are the RAFT indexes the seed helper records at
// create and at the flip; flipIndex is the one the test under it passes. All
// distinct so an assertion cannot confuse them.
const (
	createIndex uint64 = 1
	seedIndex   uint64 = 2
	flipIndex   uint64 = 42
)

// seededIndex is the index seedNamespace leaves on a namespace: an active
// namespace still carries its create index, every other seed state is
// reached by a flip recorded at seedIndex.
func seededIndex(seedState cmd.NamespaceState) uint64 {
	if seedState == "" {
		return 0
	}
	if seedState == cmd.NamespaceStateActive {
		return createIndex
	}
	return seedIndex
}

// nsExists reports whether the namespace is present in any state.
func nsExists(c *Controller, name string) bool {
	_, ok := c.GetNamespace(name)
	return ok
}

// nsState returns the namespace's state, failing the test if it is absent.
func nsState(t *testing.T, c *Controller, name string) cmd.NamespaceState {
	t.Helper()
	ns, ok := c.GetNamespace(name)
	require.True(t, ok, "namespace %q must exist", name)
	return ns.State
}

// seedNamespace creates name and transitions it to seedState. An empty
// seedState seeds nothing.
func seedNamespace(t *testing.T, c *Controller, name string, seedState cmd.NamespaceState) {
	t.Helper()
	if seedState == "" {
		return
	}
	require.NoError(t, c.Create(cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}}, createIndex))
	if seedState == cmd.NamespaceStateActive {
		return
	}
	if seedState == cmd.NamespaceStateResuming {
		// resuming is only reachable from suspended.
		require.NoError(t, c.ChangeState(name, cmd.NamespaceStateSuspended, seedIndex))
	}
	require.NoError(t, c.ChangeState(name, seedState, seedIndex))
}

func TestValidateName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// valid
		{name: "short valid", input: "foo", wantErr: false},
		{name: "letters and digits", input: "customer1", wantErr: false},
		{name: "max length", input: strings.Repeat("a", entschema.NamespaceMaxLength), wantErr: false},
		{name: "digit leading", input: "1foo", wantErr: false},
		{name: "hyphen middle", input: "foo-bar", wantErr: false},
		{name: "uuid", input: "550e8400-e29b-41d4-a716-446655440000", wantErr: false},
		// length
		{name: "too short", input: "ab", wantErr: true},
		{name: "empty", input: "", wantErr: true},
		{name: "too long", input: strings.Repeat("a", entschema.NamespaceMaxLength+1), wantErr: true},
		// format
		{name: "uppercase leading", input: "Foo", wantErr: true},
		{name: "uppercase middle", input: "fooBar", wantErr: true},
		{name: "trailing hyphen", input: "foo-", wantErr: true},
		{name: "leading hyphen", input: "-foo", wantErr: true},
		{name: "underscore", input: "foo_bar", wantErr: true},
		{name: "contains colon", input: "foo:bar", wantErr: true},
		// Explicit NamespaceSeparator coverage: the reserved separator must
		// never appear in a namespace name regardless of position, so that
		// "<ns>:<entity>" qualified identifiers stay unambiguously parseable.
		{name: "namespace separator middle", input: "foo" + entschema.NamespaceSeparator + "bar", wantErr: true},
		{name: "namespace separator leading", input: entschema.NamespaceSeparator + "foo", wantErr: true},
		{name: "namespace separator trailing", input: "foo" + entschema.NamespaceSeparator, wantErr: true},
		{name: "only namespace separator", input: entschema.NamespaceSeparator, wantErr: true},
		{name: "whitespace", input: "foo bar", wantErr: true},
		// reserved
		{name: "reserved admin", input: "admin", wantErr: true},
		{name: "reserved system", input: "system", wantErr: true},
		{name: "reserved default", input: "default", wantErr: true},
		{name: "reserved internal", input: "internal", wantErr: true},
		{name: "reserved weaviate", input: "weaviate", wantErr: true},
		{name: "reserved global", input: "global", wantErr: true},
		{name: "reserved public", input: "public", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateName(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestController_Create(t *testing.T) {
	tests := []struct {
		name    string
		seed    []string
		input   cmd.Namespace
		index   uint64
		wantErr error
	}{
		{
			name:  "happy path",
			input: cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}},
			index: createIndex,
		},
		{
			name:    "duplicate is rejected with ErrAlreadyExists",
			seed:    []string{"customer1"},
			input:   cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}},
			index:   createIndex,
			wantErr: ErrAlreadyExists,
		},
		{
			name:    "invalid name is rejected",
			input:   cmd.Namespace{Name: "BadName", HomeNodes: []string{"node-1"}},
			index:   createIndex,
			wantErr: ErrBadRequest,
		},
		{
			name:    "reserved name is rejected",
			input:   cmd.Namespace{Name: "admin", HomeNodes: []string{"node-1"}},
			index:   createIndex,
			wantErr: ErrBadRequest,
		},
		{
			name:    "missing home_node is rejected",
			input:   cmd.Namespace{Name: "customer1"},
			index:   createIndex,
			wantErr: ErrBadRequest,
		},
		{
			name:    "zero index is rejected",
			input:   cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}},
			index:   0,
			wantErr: ErrBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			for _, name := range tc.seed {
				require.NoError(t, c.Create(cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}}, createIndex))
			}
			err := c.Create(tc.input, tc.index)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				assert.Equal(t, len(tc.seed), c.Count(), "a rejected create must store nothing")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, len(tc.seed)+1, c.Count())
		})
	}
}

func TestController_Create_StoresActiveState(t *testing.T) {
	c := newTestController(t)
	// Caller-provided State and StateChangeIndex are ignored: a caller
	// cannot store a non-active namespace, nor claim a state change that
	// never happened. The stored index is the one Create was called with.
	require.NoError(t, c.Create(cmd.Namespace{
		Name:             "customer1",
		HomeNodes:        []string{"node-1"},
		State:            cmd.NamespaceStateDeleting,
		StateChangeIndex: flipIndex,
	}, createIndex))
	got := c.Get("customer1")
	require.Len(t, got, 1)
	assert.Equal(t, cmd.NamespaceStateActive, got[0].State)
	assert.Equal(t, createIndex, got[0].StateChangeIndex)
}

func TestController_Create_RejectsDeletingWithDistinctSentinel(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, createIndex))
	require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting, seedIndex))

	err := c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, createIndex)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNamespaceDeleting)
	assert.NotErrorIs(t, err, ErrAlreadyExists,
		"deleting must surface as a distinct conflict so REST can render a different message")
}

func TestController_ChangeState(t *testing.T) {
	tests := []struct {
		name      string
		seedState cmd.NamespaceState // empty = no namespace exists
		target    cmd.NamespaceState
		wantErr   error
	}{
		// from active
		{name: "active -> active is idempotent", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateActive},
		{name: "active -> suspended flips state", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateSuspended},
		{name: "active -> resuming is forbidden", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateResuming, wantErr: ErrInvalidStateTransition},
		{name: "active -> deleting flips state", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateDeleting},
		// from suspended
		{name: "suspended -> active flips state", seedState: cmd.NamespaceStateSuspended, target: cmd.NamespaceStateActive},
		{name: "suspended -> suspended is idempotent", seedState: cmd.NamespaceStateSuspended, target: cmd.NamespaceStateSuspended},
		{name: "suspended -> resuming flips state", seedState: cmd.NamespaceStateSuspended, target: cmd.NamespaceStateResuming},
		{name: "suspended -> deleting flips state", seedState: cmd.NamespaceStateSuspended, target: cmd.NamespaceStateDeleting},
		// from resuming
		{name: "resuming -> active flips state", seedState: cmd.NamespaceStateResuming, target: cmd.NamespaceStateActive},
		{name: "resuming -> suspended flips state", seedState: cmd.NamespaceStateResuming, target: cmd.NamespaceStateSuspended},
		{name: "resuming -> resuming is idempotent", seedState: cmd.NamespaceStateResuming, target: cmd.NamespaceStateResuming},
		{name: "resuming -> deleting flips state", seedState: cmd.NamespaceStateResuming, target: cmd.NamespaceStateDeleting},
		// from deleting: terminal
		{name: "deleting -> active is forbidden", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateActive, wantErr: ErrInvalidStateTransition},
		{name: "deleting -> suspended is forbidden", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateSuspended, wantErr: ErrInvalidStateTransition},
		{name: "deleting -> resuming is forbidden", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateResuming, wantErr: ErrInvalidStateTransition},
		{name: "deleting -> deleting is idempotent", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateDeleting},

		{name: "unknown target state is rejected", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceState("not-a-state"), wantErr: ErrBadRequest},
		// What a payload with no TargetState unmarshals to.
		{name: "empty target state is rejected", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceState(""), wantErr: ErrBadRequest},
		{name: "missing namespace returns ErrNotFound", target: cmd.NamespaceStateDeleting, wantErr: ErrNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			seedNamespace(t, c, "customer1", tc.seedState)

			err := c.ChangeState("customer1", tc.target, flipIndex)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				if tc.seedState != "" {
					got, ok := c.GetNamespace("customer1")
					require.True(t, ok)
					assert.Equal(t, tc.seedState, got.State, "rejected ChangeState must not mutate the stored state")
					assert.Equal(t, seededIndex(tc.seedState), got.StateChangeIndex,
						"rejected ChangeState must not record an index")
				}
				return
			}
			require.NoError(t, err)
			got, ok := c.GetNamespace("customer1")
			require.True(t, ok)
			assert.Equal(t, tc.target, got.State)

			wantIndex := flipIndex
			if tc.target == tc.seedState {
				wantIndex = seededIndex(tc.seedState)
			}
			assert.Equal(t, wantIndex, got.StateChangeIndex,
				"only an accepted flip records the index; a same-state re-apply leaves it alone")
		})
	}
}

func TestController_RemoveEntity(t *testing.T) {
	tests := []struct {
		name      string
		seedState cmd.NamespaceState // empty = no namespace exists
		wantErr   error
	}{
		{name: "deleting namespace is removed", seedState: cmd.NamespaceStateDeleting},
		{name: "active namespace returns ErrInvalidState", seedState: cmd.NamespaceStateActive, wantErr: ErrInvalidState},
		{name: "missing namespace returns ErrNotFound", wantErr: ErrNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			seedNamespace(t, c, "customer1", tc.seedState)

			err := c.RemoveEntity("customer1")
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				assert.Equal(t, tc.seedState != "", nsExists(c, "customer1"))
				return
			}
			require.NoError(t, err)
			assert.False(t, nsExists(c, "customer1"))
		})
	}
}

func TestController_Update(t *testing.T) {
	tests := []struct {
		name      string
		seedState cmd.NamespaceState // "" means do not seed
		input     cmd.Namespace
		wantErr   error
	}{
		{name: "happy path rewrites home_node", seedState: cmd.NamespaceStateActive, input: cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-2"}}},
		{name: "missing namespace returns ErrNotFound", input: cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-2"}}, wantErr: ErrNotFound},
		{name: "empty home_node is rejected", seedState: cmd.NamespaceStateActive, input: cmd.Namespace{Name: "customer1"}, wantErr: ErrBadRequest},
		{name: "deleting namespace returns ErrNamespaceDeleting", seedState: cmd.NamespaceStateDeleting, input: cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-2"}}, wantErr: ErrNamespaceDeleting},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			if tc.seedState != "" {
				require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, createIndex))
				if tc.seedState == cmd.NamespaceStateDeleting {
					require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting, seedIndex))
				}
			}
			err := c.Update(tc.input)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				// On rejection, HomeNodes must not have changed.
				if tc.seedState != "" {
					got := c.Get("customer1")
					require.Len(t, got, 1)
					assert.Equal(t, "node-1", got[0].Primary(), "rejected Update must not mutate the stored HomeNodes")
				}
				return
			}
			require.NoError(t, err)
			got := c.Get("customer1")
			require.Len(t, got, 1)
			assert.Equal(t, tc.input.Primary(), got[0].Primary())
		})
	}
}

func TestController_RecreateAfterRemoval(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, createIndex))
	require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting, seedIndex))
	require.NoError(t, c.RemoveEntity("customer1"))

	// The re-created namespace takes the index of the create that made it, so
	// nothing carries over from the name's previous life.
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, flipIndex))
	assert.Equal(t, cmd.NamespaceStateActive, nsState(t, c, "customer1"))
	got := c.Get("customer1")
	require.Len(t, got, 1)
	assert.Equal(t, flipIndex, got[0].StateChangeIndex)
}

func TestController_ListDeleting(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, createIndex))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2", HomeNodes: []string{"node-1"}}, createIndex))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer3", HomeNodes: []string{"node-1"}}, createIndex))
	require.NoError(t, c.ChangeState("customer3", cmd.NamespaceStateDeleting, seedIndex))
	require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting, seedIndex))

	assert.Equal(t, []string{"customer1", "customer3"}, c.ListDeleting())
}

func TestController_RestoreNormalizesEmptyState(t *testing.T) {
	// Snapshots without a State field must restore as active. HomeNodes
	// is required (see TestController_Restore/"missing HomeNodes...") so
	// the snapshot still carries one.
	c := newTestController(t)
	snap := []byte(`{"customer1":{"Name":"customer1","HomeNodes":["node-1"]}}`)
	require.NoError(t, c.Restore(snap))

	got := c.Get("customer1")
	require.Len(t, got, 1)
	assert.Equal(t, cmd.NamespaceStateActive, got[0].State)
}

func TestController_RestoreRestoresKnownStates(t *testing.T) {
	c := newTestController(t)
	snap := []byte(`{
		"customer1":{"Name":"customer1","HomeNodes":["node-1"],"State":"active"},
		"customer2":{"Name":"customer2","HomeNodes":["node-1"],"State":"deleting"},
		"customer3":{"Name":"customer3","HomeNodes":["node-1"],"State":"suspended"},
		"customer4":{"Name":"customer4","HomeNodes":["node-1"],"State":"resuming"}
	}`)
	require.NoError(t, c.Restore(snap))

	want := map[string]cmd.NamespaceState{
		"customer1": cmd.NamespaceStateActive,
		"customer2": cmd.NamespaceStateDeleting,
		"customer3": cmd.NamespaceStateSuspended,
		"customer4": cmd.NamespaceStateResuming,
	}
	for name, state := range want {
		got, ok := c.GetNamespace(name)
		require.True(t, ok, name)
		assert.Equal(t, state, got.State, name)
	}
	assert.Equal(t, []string{"customer2"}, c.ListDeleting())
}

// TestController_RestoreRejectsUnknownState locks in fail-loud behaviour
// for snapshots that carry a State value the current binary does not
// know. Silently coercing would mis-classify the namespace; the
// startup-time error forces the operator to investigate. Rejection is
// all-or-nothing: a valid entry alongside a bad one is not restored
// either.
func TestController_RestoreRejectsUnknownState(t *testing.T) {
	// HomeNodes is required by Restore; every fixture carries one so these
	// cases exercise the unknown-state rejection rather than the
	// missing-HomeNodes one.
	tests := []struct {
		name string
		snap string
	}{
		{
			name: "single unknown entry",
			snap: `{"customer1":{"Name":"customer1","HomeNodes":["node-1"],"State":"not-a-state"}}`,
		},
		{
			name: "known entry alongside an unknown one",
			snap: `{
				"customer1":{"Name":"customer1","HomeNodes":["node-1"],"State":"suspended"},
				"customer2":{"Name":"customer2","HomeNodes":["node-1"],"State":"not-a-state"}
			}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			err := c.Restore([]byte(tc.snap))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown state")
			// The controller must remain unchanged on rejection so that the
			// caller (RAFT FSM) can fail startup cleanly.
			assert.Equal(t, 0, c.Count())
		})
	}
}

func TestController_Get(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, createIndex))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2", HomeNodes: []string{"node-1"}}, createIndex))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer3", HomeNodes: []string{"node-1"}}, createIndex))

	tests := []struct {
		name  string
		query []string
		want  []string
	}{
		{
			name: "empty names returns all",
			want: []string{"customer1", "customer2", "customer3"},
		},
		{
			name:  "specific names returns subset",
			query: []string{"customer1", "customer3"},
			want:  []string{"customer1", "customer3"},
		},
		{
			name:  "missing names are omitted",
			query: []string{"customer1", "never-existed"},
			want:  []string{"customer1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, namesOf(c.Get(tc.query...)))
		})
	}
}

func TestController_GetNamespace(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, createIndex))

	assert.True(t, nsExists(c, "customer1"))
	assert.False(t, nsExists(c, "never-existed"))

	require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting, seedIndex))
	require.NoError(t, c.RemoveEntity("customer1"))
	assert.False(t, nsExists(c, "customer1"))
}

func TestController_List(t *testing.T) {
	c := newTestController(t)
	assert.Empty(t, c.List())

	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, createIndex))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2", HomeNodes: []string{"node-1"}}, createIndex))

	assert.ElementsMatch(t,
		[]string{"customer1", "customer2"},
		namesOf(c.List()))
}

func TestController_SnapshotRestoreRoundtrip(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-a"}}, createIndex))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2", HomeNodes: []string{"node-b"}}, createIndex))
	require.NoError(t, c.ChangeState("customer2", cmd.NamespaceStateSuspended, flipIndex))

	snap, err := c.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	restored := newTestController(t)
	require.NoError(t, restored.Restore(snap))
	assert.Equal(t, 2, restored.Count())
	assert.ElementsMatch(t,
		[]string{"customer1", "customer2"},
		namesOf(restored.List()))

	// HomeNodes must survive a snapshot/restore round-trip.
	got := restored.Get("customer1", "customer2")
	require.Len(t, got, 2)
	byName := map[string]cmd.Namespace{got[0].Name: got[0], got[1].Name: got[1]}
	assert.Equal(t, "node-a", byName["customer1"].Primary())
	assert.Equal(t, "node-b", byName["customer2"].Primary())

	// So must the state and the index of the flip that set it.
	assert.Equal(t, cmd.NamespaceStateSuspended, byName["customer2"].State)
	assert.Equal(t, flipIndex, byName["customer2"].StateChangeIndex)
	assert.Equal(t, createIndex, byName["customer1"].StateChangeIndex,
		"a never-flipped namespace round-trips carrying its create index")
}

func TestController_Restore(t *testing.T) {
	tests := []struct {
		name      string
		seed      []string
		snap      []byte
		wantErr   bool
		wantNames []string
	}{
		{
			// Cold start / fresh bootstrap: no snapshot bytes yet.
			name: "nil snapshot resets state",
			seed: []string{"stale"},
			snap: nil,
		},
		{
			name: "empty snapshot resets state",
			seed: []string{"stale"},
			snap: []byte{},
		},
		{
			// Snapshots that carry unknown fields (e.g. a future `state`
			// field on Namespace) must still restore cleanly. HomeNodes is
			// required — see "missing HomeNodes" below.
			name:      "unknown fields are tolerated",
			snap:      []byte(`{"customer1":{"Name":"customer1","HomeNodes":["node-1"],"FutureField":"ignored"}}`),
			wantNames: []string{"customer1"},
		},
		{
			name:    "malformed JSON returns an error",
			snap:    []byte("not-json"),
			wantErr: true,
		},
		{
			// Pre-home_node snapshots have no migration path; rather than
			// loading a broken entry that would fail at first placement
			// attempt, Restore rejects up front.
			name:    "missing HomeNodes is rejected",
			snap:    []byte(`{"customer1":{"Name":"customer1","State":"active"}}`),
			wantErr: true,
		},
		{
			name:    "empty HomeNodes entry is rejected",
			snap:    []byte(`{"customer1":{"Name":"customer1","HomeNodes":[""],"State":"active"}}`),
			wantErr: true,
		},
		{
			// A truncated or hand-edited snapshot unmarshals a null entry to
			// a nil pointer; it must be rejected rather than dereferenced.
			name:    "null entry is rejected",
			snap:    []byte(`{"customer1":null}`),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			for _, name := range tc.seed {
				require.NoError(t, c.Create(cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}}, createIndex))
			}
			err := c.Restore(tc.snap)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantNames, namesOf(c.List()))
		})
	}
}

func namesOf(nss []cmd.Namespace) []string {
	out := make([]string, len(nss))
	for i, ns := range nss {
		out[i] = ns.Name
	}
	return out
}
