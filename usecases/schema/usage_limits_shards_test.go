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

package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// TestAddClass_WithShardLimit covers the per-collection shard-count cap
// added by the Free-Tier guardrails RFC. Config-time check only — there's
// no live count to consult; the limit is compared against the
// ShardingConfig.DesiredCount on the create request.
//
// Multi-tenant collections set DesiredCount=0 (shards are dynamic), so
// the cap is naturally satisfied without explicit handling here.
func TestAddClass_WithShardLimit(t *testing.T) {
	tests := []struct {
		name         string
		shardCap     int
		desiredCount int
		expectExceed bool
	}{
		{name: "cap unset (-1) → no enforcement", shardCap: -1, desiredCount: 100, expectExceed: false},
		{name: "well under cap", shardCap: 4, desiredCount: 1, expectExceed: false},
		{name: "exact at cap", shardCap: 1, desiredCount: 1, expectExceed: false},
		{name: "one over cap", shardCap: 1, desiredCount: 2, expectExceed: true},
		{name: "many over cap", shardCap: 2, desiredCount: 8, expectExceed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, fakeMgr := newTestHandler(t, &fakeDB{})
			handler.config.UsageLimits.MaxShardsPerCollection = runtime.NewDynamicValue(tt.shardCap)
			handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)

			fakeMgr.On("QueryCollectionsCount", "").Return(0, nil).Maybe()
			if !tt.expectExceed {
				fakeMgr.On("AddClass", mock.Anything, mock.Anything).Return(nil)
			}

			class := &models.Class{
				Class:             "ShardLimitTest",
				Vectorizer:        "none",
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				ShardingConfig: map[string]interface{}{
					"desiredCount": tt.desiredCount,
				},
			}

			_, _, err := handler.AddClass(context.Background(), nil, class)
			if tt.expectExceed {
				require.Error(t, err)
				le, ok := usagelimits.AsLimitExceeded(err)
				require.True(t, ok, "expected *LimitExceededError, got %T: %v", err, err)
				assert.Equal(t, usagelimits.LimitShards, le.Limit)
				assert.Equal(t, int64(tt.shardCap), le.Value)
			} else {
				require.NoError(t, err, "did not expect rejection")
			}
		})
	}
}
