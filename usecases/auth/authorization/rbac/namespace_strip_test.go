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

package rbac

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// TestReferencedNamespaces pins which namespaces a backup's RBAC blob is
// checked against on a target with namespaces on. db grouping subjects are read
// whatever the list says, because their colon always marks a namespace. Every
// other column is covered by the blob's own list alone, since only the source
// cluster could tell a namespace prefix from a colon inside a global id. A
// blob without a list is checked on its db subjects only.
func TestReferencedNamespaces(t *testing.T) {
	tests := []struct {
		name        string
		in          *snapshot
		staticUsers []string
		want        []string
	}{
		{
			name: "the snapshot's own list wins",
			in: &snapshot{
				Namespaces: []string{"acme"},
				Policy: [][]string{
					{"role:acme:manager", "data/collections/other:Movies/shards/*/objects/*", "R", "data"},
				},
			},
			want: []string{"acme"},
		},
		{
			// beta appears only in a role name and acme:Movies only in a resource
			// path. With no list, neither is read; the db subject alone counts.
			name: "no list reads db subjects only",
			in: &snapshot{
				Policy: [][]string{
					{"role:beta:manager", "data/collections/acme:Movies/shards/*/objects/*", "R", "data"},
				},
				GroupingPolicy: [][]string{
					{"db:acme:alice", "role:beta:reader"},
				},
			},
			want: []string{"acme"},
		},
		{
			// A colon in an OIDC subject is not a namespace separator. Nothing here
			// reads OIDC subjects; the source's list carries their namespaces.
			name: "an OIDC subject with no list yields nothing",
			in: &snapshot{
				GroupingPolicy: [][]string{
					{"oidc:urn:foo", "role:viewer"},
				},
			},
		},
		{
			name: "unqualified names yield nothing",
			in: &snapshot{
				Policy:         [][]string{{"role:manager", "data/collections/Movies", "R", "data"}},
				GroupingPolicy: [][]string{{"db:alice", "role:manager"}},
			},
		},
		{
			name: "an empty blob yields nothing",
		},
		{
			// Whatever writes Namespaces never records db subjects, so the path that
			// reads the list has to read db subjects too. Otherwise this grouping
			// row is restored with no namespace to belong to.
			name: "a db subject is read even when the list omits it",
			in: &snapshot{
				Namespaces: []string{"acme"},
				Policy: [][]string{
					{"role:acme:manager", "data/collections/acme:Movies/shards/*/objects/*", "R", "data"},
				},
				GroupingPolicy: [][]string{
					{"db:ns3:bob", "role:viewer"},
				},
			},
			want: []string{"acme", "ns3"},
		},
		{
			// The blob's only reference to ns3 is the subject: no role name, no
			// resource path and no list carries it.
			name: "a db subject is the sole reference, no list",
			in: &snapshot{
				GroupingPolicy: [][]string{
					{"db:ns3:bob", "role:viewer"},
				},
			},
			want: []string{"ns3"},
		},
		{
			// A configured static API key user is a global identity, and its name may
			// hold a colon of its own. The strip treats it the same way.
			name: "a static API key user is not a namespace reference",
			in: &snapshot{
				GroupingPolicy: [][]string{
					{"db:ns3:bob", "role:viewer"},
				},
			},
			staticUsers: []string{"ns3:bob"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var blob []byte
			if tt.in != nil {
				var err error
				blob, err = json.Marshal(tt.in)
				require.NoError(t, err)
			}

			got, err := ReferencedNamespaces(blob, tt.staticUsers)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("a malformed blob errors", func(t *testing.T) {
		_, err := ReferencedNamespaces([]byte("not json"), nil)
		require.Error(t, err)
	})
}

func TestRequireReferencedNamespacesExist(t *testing.T) {
	blobWith := func(t *testing.T, namespaces ...string) []byte {
		t.Helper()
		s := snapshot{Version: 1, Namespaces: namespaces}
		b, err := json.Marshal(s)
		require.NoError(t, err)
		return b
	}

	tests := []struct {
		name    string
		blob    []byte
		states  map[string]cmd.NamespaceState
		wantErr bool
		wantMsg string
	}{
		{
			name:   "all referenced namespaces active",
			blob:   blobWith(t, "ns1", "ns2"),
			states: map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateActive, "ns2": cmd.NamespaceStateActive},
		},
		{
			name:    "one deleting namespace errors",
			blob:    blobWith(t, "ns1"),
			states:  map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateDeleting},
			wantErr: true,
			wantMsg: "ns1",
		},
		{
			name:    "one missing namespace errors",
			blob:    blobWith(t, "ns1"),
			states:  map[string]cmd.NamespaceState{},
			wantErr: true,
			wantMsg: "ns1",
		},
		{
			name:    "malformed blob errors",
			blob:    []byte("{bad"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := usecasesNamespaces.NewMockExisterInState(t, tt.states)
			err := RequireReferencedNamespacesExist(tt.blob, nil, ns)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tt.wantMsg != "" {
				assert.Contains(t, err.Error(), tt.wantMsg)
			}
		})
	}
}
