//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"testing"
	"time"
)

func TestSegmentsCleanup(t *testing.T) {
	ctx := testCtx()
	tests := bucketIntegrationTests{
		{
			name: "cleanupReplaceStrategy",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				cleanupReplaceStrategy(ctx, t, opts)
			},
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSegmentsCleanupInterval(time.Second),
				WithCalcCountNetAdditions(true),
			},
		},
		{
			name: "cleanupReplaceStrategy_WithSecondaryKeys",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				cleanupReplaceStrategy_WithSecondaryKeys(ctx, t, opts)
			},
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(2),
				WithSegmentsCleanupInterval(time.Second),
				WithCalcCountNetAdditions(true),
			},
		},
	}
	tests.run(ctx, t)
}
