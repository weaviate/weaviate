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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func TestRequireActive(t *testing.T) {
	tests := []struct {
		name      string
		seedState cmd.NamespaceState // empty = no namespace exists
		lookup    string
		wantErr   error
	}{
		{name: "active is allowed", seedState: cmd.NamespaceStateActive, lookup: "customer1"},
		{name: "suspended reports suspension", seedState: cmd.NamespaceStateSuspended, lookup: "customer1", wantErr: ErrNamespaceSuspended},
		{name: "resuming reports resumption", seedState: cmd.NamespaceStateResuming, lookup: "customer1", wantErr: ErrNamespaceResuming},
		{name: "deleting reports deletion", seedState: cmd.NamespaceStateDeleting, lookup: "customer1", wantErr: ErrNamespaceDeleting},
		{name: "missing namespace reports gone", lookup: "never-existed", wantErr: ErrNamespaceGone},
		// An entity belonging to no namespace: nothing to check.
		{name: "empty name is allowed", seedState: cmd.NamespaceStateActive, lookup: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			seedNamespace(t, c, "customer1", tc.seedState)

			err := RequireActive(c, tc.lookup)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAdmitDestructiveApply(t *testing.T) {
	tests := []struct {
		name      string
		seedState cmd.NamespaceState // empty = no namespace exists
		lookup    string
		wantErr   error
	}{
		{name: "active is allowed", seedState: cmd.NamespaceStateActive, lookup: "customer1"},
		{name: "suspended reports suspension", seedState: cmd.NamespaceStateSuspended, lookup: "customer1", wantErr: ErrNamespaceSuspended},
		{name: "resuming reports resumption", seedState: cmd.NamespaceStateResuming, lookup: "customer1", wantErr: ErrNamespaceResuming},
		// Refusing here would stall the cleanup cascade, which deletes a
		// namespace's aliases and classes while it is in this state.
		{name: "deleting is allowed", seedState: cmd.NamespaceStateDeleting, lookup: "customer1"},
		// Reached by a delete naming a namespace whose cascade already finished.
		// Nothing survives under the prefix. Create-side gates refuse a missing
		// namespace, and RemoveEntity at the apply layer refuses a non-empty one.
		{name: "missing namespace is allowed", lookup: "never-existed"},
		// The verdict comes from the namespace the name resolves to, so
		// suspending one must not gate a delete in another.
		{name: "a prefix naming no live namespace is allowed", seedState: cmd.NamespaceStateSuspended, lookup: "ghost"},
		// An entity belonging to no namespace: nothing to check.
		{name: "empty name is allowed", seedState: cmd.NamespaceStateActive, lookup: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			seedNamespace(t, c, "customer1", tc.seedState)

			err := AdmitDestructiveApply(c, tc.lookup)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// controllerWithUnknownState holds one namespace in a state this binary does not
// know. Restore and ChangeState both reject one, so the field is set directly.
func controllerWithUnknownState(t *testing.T) *Controller {
	t.Helper()
	c := newTestController(t)
	require.NoError(t, c.Restore([]byte(
		`{"customer1":{"Name":"customer1","HomeNodes":["node-1"],"State":"active"}}`)))
	c.namespaces["customer1"].State = cmd.NamespaceState("not-a-state")
	return c
}

func TestRequireActive_UnknownStateIsRejected(t *testing.T) {
	// A state this binary doesn't know must not be treated as usable.
	assert.ErrorIs(t, RequireActive(controllerWithUnknownState(t), "customer1"), ErrInvalidState)
}

func TestAdmitDestructiveApply_UnknownStateIsRejected(t *testing.T) {
	assert.ErrorIs(t, AdmitDestructiveApply(controllerWithUnknownState(t), "customer1"), ErrInvalidState)
}
