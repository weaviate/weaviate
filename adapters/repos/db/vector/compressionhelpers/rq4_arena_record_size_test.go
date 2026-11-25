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

package compressionhelpers

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TestRQ4ArenaRecordSize pins that the arena record size used by the RQ4
// factories matches the codes the quantizer actually writes — for the wide
// centered header past rq4CenteredCompactMaxDim too, and on the restore
// path — so a restored index under VECTOR_CACHE_IMPL=arena doesn't refuse
// every valid persisted code.
func TestRQ4ArenaRecordSize(t *testing.T) {
	t.Setenv(cache.VectorCacheImplEnv, "arena")
	logger, _ := test.NewNullLogger()
	provider := distancer.NewDotProductProvider()

	for _, tc := range []struct {
		name     string
		dim      int
		centered bool
	}{
		{name: "uncentered", dim: 1536},
		{name: "centered compact", dim: 1536, centered: true},
		{name: "centered wide", dim: 5000, centered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(tc.dim), 1))
			vec := make([]float32, tc.dim)
			for i := range vec {
				vec[i] = rng.Float32() - 0.5
			}

			var built *FourBitRotationalQuantizer
			if tc.centered {
				mean := make([]float32, tc.dim)
				for i := range mean {
					mean[i] = 0.01
				}
				var err error
				built, err = NewCenteredFourBitRotationalQuantizer(tc.dim, DefaultFastRotationSeed, provider, mean)
				require.NoError(t, err)
			} else {
				built = NewFourBitRotationalQuantizer(tc.dim, DefaultFastRotationSeed, provider)
			}
			data := built.Data()
			restored, err := RestoreFourBitRotationalQuantizer(int(data.InputDim),
				int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
				data.Rotation.Swaps, data.Rotation.Signs, data.Mean, provider)
			require.NoError(t, err)

			for name, rq := range map[string]*FourBitRotationalQuantizer{"built": built, "restored": restored} {
				code := rq.Encode(vec)
				require.Len(t, code, rq.rq4RecordSize(), name)

				c := newRQByteCache(nil, rq.rq4RecordSize(), 1e9, logger, nil)
				t.Cleanup(c.Drop)
				c.Grow(1)
				c.Preload(1, code)
				got, err := c.Get(context.Background(), 1)
				require.NoError(t, err, name)
				assert.Equal(t, code, got, "%s: the arena must accept and return the code", name)
			}
		})
	}
}
