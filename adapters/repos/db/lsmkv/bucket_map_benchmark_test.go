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
	"encoding/binary"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func BenchmarkBucketMapSet(b *testing.B) {
	for _, tc := range []struct {
		name     string
		strategy string
		sameRow  bool
	}{
		{"map/sameRow", StrategyMapCollection, true},
		{"map/newRow", StrategyMapCollection, false},
		{"inverted/sameRow", StrategyInverted, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			logger, _ := test.NewNullLogger()
			bucket, err := NewBucketCreator().NewBucket(ctx, b.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(tc.strategy), WithMemtableThreshold(1<<40))
			require.NoError(b, err)
			defer bucket.Shutdown(ctx)

			rowKeys := make([][]byte, b.N)
			mks := make([][]byte, b.N)
			backing := make([]byte, 24*b.N)
			for i := range rowKeys {
				rk := backing[24*i : 24*i+16 : 24*i+16]
				copy(rk, "row-key-00000000")
				if !tc.sameRow {
					binary.BigEndian.PutUint64(rk[8:], uint64(i))
				}
				rowKeys[i] = rk
				mks[i] = backing[24*i+16 : 24*i+24 : 24*i+24]
				binary.BigEndian.PutUint64(mks[i], uint64(i))
			}
			val := make([]byte, 8)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if tc.strategy == StrategyInverted {
					err = bucket.InvertedSet(rowKeys[i], binary.BigEndian.Uint64(mks[i]), 1, 1)
				} else {
					err = bucket.MapSet(rowKeys[i], MapPair{Key: mks[i], Value: val})
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
