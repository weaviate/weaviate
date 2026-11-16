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

package lsmkv

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestSwapBucketPointersIsAllOrNothing(t *testing.T) {
	tests := []struct {
		name    string
		swaps   []BucketSwap
		wantErr string
	}{
		{name: "every pair moves", swaps: []BucketSwap{{"a", "a2"}, {"b", "b2"}}},
		{name: "a missing source", swaps: []BucketSwap{{"a", "a2"}, {"b", "gone"}}, wantErr: `"gone" not found`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			logger, _ := test.NewNullLogger()
			store, err := New(dir, dir, logger, nil, nil, cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
			require.NoError(t, err)
			before := map[string]*Bucket{}
			for _, name := range []string{"a", "a2", "b", "b2"} {
				require.NoError(t, store.CreateOrLoadBucket(ctx, name, WithStrategy(StrategyReplace)))
				before[name] = store.Bucket(name)
			}
			defer store.Shutdown(ctx)

			displaced, err := store.SwapBucketPointers(ctx, tc.swaps)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				for name, bucket := range before {
					require.Same(t, bucket, store.Bucket(name), name)
				}
				return
			}
			require.NoError(t, err)
			for _, bucket := range displaced {
				t.Cleanup(func() { bucket.Shutdown(ctx) })
			}
			require.Equal(t, []*Bucket{before["a"], before["b"]}, displaced)
			require.Same(t, before["a2"], store.Bucket("a"))
			require.Same(t, before["b2"], store.Bucket("b"))
			require.Nil(t, store.Bucket("a2"))
			require.Nil(t, store.Bucket("b2"))
		})
	}
}
