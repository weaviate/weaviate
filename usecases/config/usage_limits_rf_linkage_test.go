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

package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestValidateUsageLimitsReplicationLinkage(t *testing.T) {
	unset := runtime.NewDynamicValue(-1)
	set := runtime.NewDynamicValue(10)

	tests := []struct {
		name      string
		objects   *runtime.DynamicValue[int]
		tenants   *runtime.DynamicValue[int]
		shards    *runtime.DynamicValue[int]
		maxFactor int
		wantErr   bool
	}{
		{name: "no limits, no cap", objects: unset, tenants: unset, shards: unset, maxFactor: 0, wantErr: false},
		{name: "no limits, MaximumFactor=1", objects: unset, tenants: unset, shards: unset, maxFactor: 1, wantErr: false},
		{name: "no limits, MaximumFactor=3", objects: unset, tenants: unset, shards: unset, maxFactor: 3, wantErr: false},
		{name: "objects set, MaximumFactor=1", objects: set, tenants: unset, shards: unset, maxFactor: 1, wantErr: false},
		{name: "tenants set, MaximumFactor=1", objects: unset, tenants: set, shards: unset, maxFactor: 1, wantErr: false},
		{name: "shards set, MaximumFactor=1", objects: unset, tenants: unset, shards: set, maxFactor: 1, wantErr: false},
		{name: "objects set, MaximumFactor unset", objects: set, tenants: unset, shards: unset, maxFactor: 0, wantErr: true},
		{name: "objects set, MaximumFactor=3", objects: set, tenants: unset, shards: unset, maxFactor: 3, wantErr: true},
		{name: "tenants set, MaximumFactor unset", objects: unset, tenants: set, shards: unset, maxFactor: 0, wantErr: true},
		{name: "shards set, MaximumFactor=2", objects: unset, tenants: unset, shards: set, maxFactor: 2, wantErr: true},
		{name: "nil DynamicValue treated as unset", objects: nil, tenants: nil, shards: nil, maxFactor: 0, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				UsageLimits: UsageLimitsConfig{
					MaxObjectsCount:         tt.objects,
					MaxTenantsPerCollection: tt.tenants,
					MaxShardsPerCollection:  tt.shards,
				},
				Replication: replication.GlobalConfig{MaximumFactor: tt.maxFactor},
			}
			err := c.validateUsageLimitsReplicationLinkage()
			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), "REPLICATION_MAXIMUM_FACTOR"),
					"error should mention the env var, got: %v", err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

// Collection-count cap is intentionally NOT tied to MaximumFactor because it
// predates this work and re-tying it would break existing operators.
func TestValidateUsageLimitsReplicationLinkage_CollectionsCountExcluded(t *testing.T) {
	c := &Config{
		UsageLimits: UsageLimitsConfig{
			MaxObjectsCount:         runtime.NewDynamicValue(-1),
			MaxTenantsPerCollection: runtime.NewDynamicValue(-1),
			MaxShardsPerCollection:  runtime.NewDynamicValue(-1),
		},
		SchemaHandlerConfig: SchemaHandlerConfig{
			MaximumAllowedCollectionsCount: runtime.NewDynamicValue(10),
		},
		Replication: replication.GlobalConfig{MaximumFactor: 0},
	}
	assert.NoError(t, c.validateUsageLimitsReplicationLinkage())
}
