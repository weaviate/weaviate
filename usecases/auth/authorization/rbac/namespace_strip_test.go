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
)

// TestReferencedNamespaces pins which namespaces a backup's RBAC blob is
// checked against on a namespace-enabled target. db grouping subjects are read
// whatever the list says, because their colon always marks a namespace. For the
// other columns the blob's own list wins, since only the source cluster could
// tell a namespace prefix from a colon inside a global id. Without a list the
// fallback reads role names alone.
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
			name: "no list falls back to role names",
			in: &snapshot{
				Policy: [][]string{
					{"role:acme:manager", "data/collections/acme:Movies/shards/*/objects/*", "R", "data"},
				},
				GroupingPolicy: [][]string{
					{"db:acme:alice", "role:beta:reader"},
				},
			},
			want: []string{"acme", "beta"},
		},
		{
			// A colon in an OIDC subject is not a namespace separator. With no list
			// to confirm one, the fallback must not invent "urn".
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
			// Whatever writes Namespaces never records db subjects, so the list arm
			// has to read them too or this grouping row installs an orphan.
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
