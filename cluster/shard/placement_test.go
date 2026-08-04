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

package shard_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/cluster/shard"
)

func TestPreferredBirthLeader(t *testing.T) {
	tests := []struct {
		name        string
		members     []string
		multiTenant bool
		tenantCount int
		want        string
	}{
		{
			name:    "single-tenant returns first replica",
			members: []string{"n1", "n2", "n3"},
			want:    "n1",
		},
		{
			name:        "single-tenant ignores tenant count",
			members:     []string{"n2", "n3", "n1"},
			tenantCount: 7,
			want:        "n2",
		},
		{
			name:    "single-tenant single member",
			members: []string{"n1"},
			want:    "n1",
		},
		{
			name:        "multi-tenant count zero returns first replica",
			members:     []string{"n1", "n2", "n3"},
			multiTenant: true,
			tenantCount: 0,
			want:        "n1",
		},
		{
			name:        "multi-tenant count strides the member list",
			members:     []string{"n1", "n2", "n3"},
			multiTenant: true,
			tenantCount: 1,
			want:        "n2",
		},
		{
			name:        "multi-tenant count two",
			members:     []string{"n1", "n2", "n3"},
			multiTenant: true,
			tenantCount: 2,
			want:        "n3",
		},
		{
			name:        "multi-tenant wraps around",
			members:     []string{"n1", "n2", "n3"},
			multiTenant: true,
			tenantCount: 3,
			want:        "n1",
		},
		{
			name:        "multi-tenant large count",
			members:     []string{"n1", "n2", "n3"},
			multiTenant: true,
			tenantCount: 1000,
			want:        "n2", // 1000 % 3 == 1
		},
		{
			name:        "multi-tenant composes with rotated member heads",
			members:     []string{"n3", "n1", "n2"},
			multiTenant: true,
			tenantCount: 4,
			want:        "n1", // 4 % 3 == 1 into the rotated list
		},
		{
			name:        "multi-tenant single member always wins",
			members:     []string{"n1"},
			multiTenant: true,
			tenantCount: 41,
			want:        "n1",
		},
		{
			name:        "multi-tenant negative count clamps to zero",
			members:     []string{"n1", "n2", "n3"},
			multiTenant: true,
			tenantCount: -5,
			want:        "n1",
		},
		{
			name:    "empty members returns empty",
			members: nil,
			want:    "",
		},
		{
			name:        "empty members multi-tenant returns empty",
			members:     nil,
			multiTenant: true,
			tenantCount: 3,
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shard.PreferredBirthLeader(tt.members, tt.multiTenant, tt.tenantCount)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPreferredBirthLeader_TenantSequenceRoundRobin pins the orbital-filling
// property end to end over a simulated creation sequence: tenants created one
// at a time (count striding, heads fixed — the equal-disk singleton-request
// regime) and tenants created in one batch (count fixed, heads rotating — the
// sharding-state generator's regime) must both spread designations exactly
// evenly across the voter ring.
func TestPreferredBirthLeader_TenantSequenceRoundRobin(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}

	t.Run("singleton creations stride via the count", func(t *testing.T) {
		got := make(map[string]int)
		for count := 0; count < 9; count++ {
			got[shard.PreferredBirthLeader(nodes, true, count)]++
		}
		assert.Equal(t, map[string]int{"n1": 3, "n2": 3, "n3": 3}, got)
	})

	t.Run("batch creation rotates via the member heads", func(t *testing.T) {
		// initPhysical/GetPartitions assign successive shards of one batch
		// member lists that are successive rotations of the node ring.
		rotations := [][]string{
			{"n1", "n2", "n3"},
			{"n2", "n3", "n1"},
			{"n3", "n1", "n2"},
		}
		const count = 5 // constant within one batch
		got := make(map[string]int)
		for _, members := range rotations {
			got[shard.PreferredBirthLeader(members, true, count)]++
		}
		assert.Equal(t, map[string]int{"n1": 1, "n2": 1, "n3": 1}, got)
	})
}
