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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

type fakeKnownTenants struct {
	known     map[string]struct{}
	askedFor  []string
	askedWith string
}

func (f *fakeKnownTenants) KnownTenants(class string, tenants []string) map[string]struct{} {
	f.askedWith = class
	f.askedFor = tenants
	return f.known
}

func tenants(names ...string) []*cmd.Tenant {
	out := make([]*cmd.Tenant, 0, len(names))
	for _, n := range names {
		out = append(out, &cmd.Tenant{Name: n, Status: "HOT"})
	}
	return out
}

func names(ts []*cmd.Tenant) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.Name)
	}
	return out
}

// Re-asserting a tenant that already exists costs a full RAFT round trip to
// deliver an entry that metaClass.AddTenants discards, which is what
// auto-tenant creation does on every object write.
func TestAddTenantsSkipsTenantsTheLocalSchemaAlreadyHas(t *testing.T) {
	tests := []struct {
		name        string
		known       []string
		req         []string
		want        []string
		wantDropped int
	}{
		{
			name:        "every tenant already exists, so nothing is left to commit",
			known:       []string{"a", "b"},
			req:         []string{"a", "b"},
			want:        []string{},
			wantDropped: 2,
		},
		{
			name:        "only the tenants missing locally are sent",
			known:       []string{"a", "c"},
			req:         []string{"a", "b", "c", "d"},
			want:        []string{"b", "d"},
			wantDropped: 2,
		},
		{
			name:        "a tenant the local schema has not seen is still sent",
			known:       []string{},
			req:         []string{"a", "b"},
			want:        []string{"a", "b"},
			wantDropped: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			known := make(map[string]struct{}, len(tt.known))
			for _, k := range tt.known {
				known[k] = struct{}{}
			}
			reader := &fakeKnownTenants{known: known}

			got, dropped := withoutKnownTenants(reader, "Collection",
				&cmd.AddTenantsRequest{ClusterNodes: []string{"node1"}, Tenants: tenants(tt.req...)})

			require.ElementsMatch(t, tt.want, names(got.Tenants))
			require.Equal(t, tt.wantDropped, dropped, "dropped count feeds the filtered metric")
			require.Equal(t, "Collection", reader.askedWith)
			require.ElementsMatch(t, tt.req, reader.askedFor)
			require.Equal(t, []string{"node1"}, got.ClusterNodes, "placement candidates must survive the filter")
		})
	}
}

// Nothing known means nothing to filter, so the request must be passed through
// untouched rather than rebuilt.
func TestAddTenantsLeavesTheRequestAloneWhenTheLocalSchemaKnowsNothing(t *testing.T) {
	req := &cmd.AddTenantsRequest{ClusterNodes: []string{"node1"}, Tenants: tenants("a")}

	got, dropped := withoutKnownTenants(&fakeKnownTenants{}, "Collection", req)

	require.Same(t, req, got)
	require.Zero(t, dropped)
}

// removeNilTenants runs on apply, so a nil entry must not survive the filter or
// be counted as a tenant worth committing for.
func TestAddTenantsDropsNilTenants(t *testing.T) {
	req := &cmd.AddTenantsRequest{
		ClusterNodes: []string{"node1"},
		Tenants:      []*cmd.Tenant{nil, {Name: "a", Status: "HOT"}, nil},
	}

	got, dropped := withoutKnownTenants(&fakeKnownTenants{known: map[string]struct{}{"a": {}}}, "Collection", req)

	require.Empty(t, got.Tenants)
	require.Equal(t, 1, dropped, "the nil entries are not tenants and must not be counted as filtered")
}
