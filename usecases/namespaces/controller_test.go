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

// seedNamespace creates name and transitions it to seedState. An empty
// seedState seeds nothing.
func seedNamespace(t *testing.T, c *Controller, name string, seedState cmd.NamespaceState) {
	t.Helper()
	if seedState == "" {
		return
	}
	require.NoError(t, c.Create(cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}}))
	if seedState == cmd.NamespaceStateDeleting {
		require.NoError(t, c.ChangeState(name, cmd.NamespaceStateDeleting))
	}
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
		wantErr error
	}{
		{
			name:  "happy path",
			input: cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}},
		},
		{
			name:    "duplicate is rejected with ErrAlreadyExists",
			seed:    []string{"customer1"},
			input:   cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}},
			wantErr: ErrAlreadyExists,
		},
		{
			name:    "invalid name is rejected",
			input:   cmd.Namespace{Name: "BadName", HomeNodes: []string{"node-1"}},
			wantErr: ErrBadRequest,
		},
		{
			name:    "reserved name is rejected",
			input:   cmd.Namespace{Name: "admin", HomeNodes: []string{"node-1"}},
			wantErr: ErrBadRequest,
		},
		{
			name:    "missing home_node is rejected",
			input:   cmd.Namespace{Name: "customer1"},
			wantErr: ErrBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			for _, name := range tc.seed {
				require.NoError(t, c.Create(cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}}))
			}
			err := c.Create(tc.input)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, len(tc.seed)+1, c.Count())
		})
	}
}

func TestController_Create_StoresActiveState(t *testing.T) {
	c := newTestController(t)
	// Caller-provided State is ignored: stored entries are always active.
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}, State: cmd.NamespaceStateDeleting}))
	got := c.Get("customer1")
	require.Len(t, got, 1)
	assert.Equal(t, cmd.NamespaceStateActive, got[0].State)
	assert.True(t, c.IsActive("customer1"))
}

func TestController_Create_RejectsDeletingWithDistinctSentinel(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}))
	require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting))

	err := c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}})
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
		{name: "active -> deleting flips state", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateDeleting},
		{name: "active -> active is idempotent", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateActive},
		{name: "deleting -> deleting is idempotent", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateDeleting},
		{name: "deleting -> active is forbidden", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateActive, wantErr: ErrInvalidStateTransition},
		{name: "unknown target state is rejected", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceState("not-a-state"), wantErr: ErrBadRequest},
		{name: "missing namespace returns ErrNotFound", target: cmd.NamespaceStateDeleting, wantErr: ErrNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			seedNamespace(t, c, "customer1", tc.seedState)

			err := c.ChangeState("customer1", tc.target)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.True(t, c.Exists("customer1"))
			assert.Equal(t, tc.target == cmd.NamespaceStateActive, c.IsActive("customer1"))
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
				assert.Equal(t, tc.seedState != "", c.Exists("customer1"))
				return
			}
			require.NoError(t, err)
			assert.False(t, c.Exists("customer1"))
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
				require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}))
				if tc.seedState == cmd.NamespaceStateDeleting {
					require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting))
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
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}))
	require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting))
	require.NoError(t, c.RemoveEntity("customer1"))

	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}))
	assert.True(t, c.IsActive("customer1"))
}

func TestController_IsActiveAndListDeleting(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2", HomeNodes: []string{"node-1"}}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer3", HomeNodes: []string{"node-1"}}))
	require.NoError(t, c.ChangeState("customer3", cmd.NamespaceStateDeleting))
	require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting))

	assert.True(t, c.IsActive("customer2"))
	assert.False(t, c.IsActive("customer1"))
	assert.False(t, c.IsActive("customer3"))
	assert.False(t, c.IsActive("never-existed"))

	assert.Equal(t, []string{"customer1", "customer3"}, c.ListDeleting())
}

func TestController_RestoreNormalizesEmptyState(t *testing.T) {
	// Snapshots without a State field must restore as active.
	c := newTestController(t)
	snap := []byte(`{"customer1":{"Name":"customer1","Restrictions":{}}}`)
	require.NoError(t, c.Restore(snap))

	assert.True(t, c.IsActive("customer1"))
	got := c.Get("customer1")
	require.Len(t, got, 1)
	assert.Equal(t, cmd.NamespaceStateActive, got[0].State)
}

// TestController_RestoreRejectsUnknownState locks in fail-loud behaviour
// for snapshots that carry a State value the current binary does not
// know. Silently coercing would mis-classify the namespace; the
// startup-time error forces the operator to investigate.
func TestController_RestoreRestoresKnownStates(t *testing.T) {
	c := newTestController(t)
	snap := []byte(`{
		"customer1":{"Name":"customer1","State":"active","Restrictions":{}},
		"customer2":{"Name":"customer2","State":"deleting","Restrictions":{}}
	}`)
	require.NoError(t, c.Restore(snap))

	assert.True(t, c.IsActive("customer1"))
	assert.False(t, c.IsActive("customer2"))
	assert.Equal(t, []string{"customer2"}, c.ListDeleting())
}

func TestController_RestoreRejectsUnknownState(t *testing.T) {
	c := newTestController(t)
	snap := []byte(`{"customer1":{"Name":"customer1","State":"suspended","Restrictions":{}}}`)
	err := c.Restore(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown state")
	// The controller must remain unchanged on rejection so that the
	// caller (RAFT FSM) can fail startup cleanly.
	assert.Equal(t, 0, c.Count())
}

func TestController_Get(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2", HomeNodes: []string{"node-1"}}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer3", HomeNodes: []string{"node-1"}}))

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

func TestController_Exists(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}))

	assert.True(t, c.Exists("customer1"))
	assert.False(t, c.Exists("never-existed"))

	require.NoError(t, c.ChangeState("customer1", cmd.NamespaceStateDeleting))
	require.NoError(t, c.RemoveEntity("customer1"))
	assert.False(t, c.Exists("customer1"))
}

func TestController_List(t *testing.T) {
	c := newTestController(t)
	assert.Empty(t, c.List())

	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2", HomeNodes: []string{"node-1"}}))

	assert.ElementsMatch(t,
		[]string{"customer1", "customer2"},
		namesOf(c.List()))
}

func TestController_SnapshotRestoreRoundtrip(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-a"}}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2", HomeNodes: []string{"node-b"}}))

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
	byName := map[string]string{got[0].Name: got[0].Primary(), got[1].Name: got[1].Primary()}
	assert.Equal(t, "node-a", byName["customer1"])
	assert.Equal(t, "node-b", byName["customer2"])
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
			// field on Namespace) must still restore cleanly.
			name:      "unknown fields are tolerated",
			snap:      []byte(`{"customer1":{"Name":"customer1","Restrictions":{},"FutureField":"ignored"}}`),
			wantNames: []string{"customer1"},
		},
		{
			name:    "malformed JSON returns an error",
			snap:    []byte("not-json"),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			for _, name := range tc.seed {
				require.NoError(t, c.Create(cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}}))
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
