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

func TestRequireActive_UnknownStateIsRejected(t *testing.T) {
	// A state this binary doesn't know must not be treated as usable. Only a
	// snapshot from a newer binary can produce one, so set the field directly.
	c := newTestController(t)
	require.NoError(t, c.Restore([]byte(
		`{"customer1":{"Name":"customer1","HomeNodes":["node-1"],"State":"active"}}`)))
	c.namespaces["customer1"].State = cmd.NamespaceState("not-a-state")

	assert.ErrorIs(t, RequireActive(c, "customer1"), ErrInvalidState)
}

// TestRequireActiveAll pins the operator-facing half of the contract: every
// name that is not an active namespace appears in the message with the state
// that made it fail.
func TestRequireActiveAll(t *testing.T) {
	tests := []struct {
		name  string
		seed  map[string]cmd.NamespaceState
		names []string
		want  []string // substrings the message must carry; empty means no error
	}{
		{
			name:  "all active returns nil",
			seed:  map[string]cmd.NamespaceState{"onens": cmd.NamespaceStateActive, "twons": cmd.NamespaceStateActive},
			names: []string{"onens", "twons"},
		},
		{
			name:  "empty input returns nil",
			names: nil,
		},
		{
			name:  "each offender is named with its state",
			seed:  map[string]cmd.NamespaceState{"okay": cmd.NamespaceStateActive, "susp": cmd.NamespaceStateSuspended},
			names: []string{"okay", "susp", "gone"},
			want: []string{
				`"susp"`, ErrNamespaceSuspended.Error(),
				`"gone"`, ErrNamespaceGone.Error(),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			for name, state := range tc.seed {
				seedNamespace(t, c, name, state)
			}

			err := RequireActiveAll(c, tc.names)
			if len(tc.want) == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range tc.want {
				assert.Contains(t, err.Error(), want)
			}
			assert.NotContains(t, err.Error(), `"okay"`, "an active namespace must not be reported")
		})
	}
}
