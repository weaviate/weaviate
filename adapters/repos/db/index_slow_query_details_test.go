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

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// TestIndexWithSlowQueryDetails: a search that neither logs nor reports a profile
// must not collect details.
func TestIndexWithSlowQueryDetails(t *testing.T) {
	tests := []struct {
		name           string
		slowLogEnabled *configRuntime.DynamicValue[bool]
		queryProfile   bool
		wantCollected  bool
	}{
		{
			name:           "slow log off and no profile requested",
			slowLogEnabled: configRuntime.NewDynamicValue(false),
			wantCollected:  false,
		},
		{
			name:          "slow log unset",
			wantCollected: false,
		},
		{
			name:           "slow log on",
			slowLogEnabled: configRuntime.NewDynamicValue(true),
			wantCollected:  true,
		},
		{
			name:           "profile requested with the slow log off",
			slowLogEnabled: configRuntime.NewDynamicValue(false),
			queryProfile:   true,
			wantCollected:  true,
		},
		{
			name:           "profile requested with the slow log on",
			slowLogEnabled: configRuntime.NewDynamicValue(true),
			queryProfile:   true,
			wantCollected:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx := &Index{Config: IndexConfig{QuerySlowLogEnabled: test.slowLogEnabled}}

			ctx := idx.withSlowQueryDetails(context.Background(), test.queryProfile)
			helpers.AnnotateSlowQueryLog(ctx, "took", "1s")

			var want map[string]any
			if test.wantCollected {
				want = map[string]any{"took": "1s"}
			}
			require.Equal(t, want, helpers.ExtractSlowQueryDetails(ctx))
		})
	}
}
