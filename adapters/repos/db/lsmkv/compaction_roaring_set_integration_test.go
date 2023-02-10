//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func Test_CompactionRoaringSet(t *testing.T) {
	maxID := uint64(100)
	maxElement := uint64(1e6)
	iterations := uint64(100_000)

	deleteRatio := 0.2   // 20% of all operations will be deletes, 80% additions
	flushChance := 0.001 // on average one flus per 1000 iterations

	rand.Seed(time.Now().UnixNano())

	instr := generateRandomInstructions(maxID, maxElement, iterations, deleteRatio)
	control := controlFromInstructions(instr, maxID)

	b, err := NewBucket(testCtx(), t.TempDir(), "", nullLogger(), nil,
		WithStrategy(StrategyRoaringSet))
	require.Nil(t, err)

	// stop default compaction to run one manually
	b.disk.compactionCycle.StopAndWait(context.Background())
	defer b.Shutdown(testCtx())

	// so big it effectively never triggers as part of this test
	b.SetMemtableThreshold(1e9)

	compactions := 0
	for _, inst := range instr {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, inst.key)
		if inst.addition {
			b.RoaringSetAddOne(key, inst.element)
		} else {
			b.RoaringSetRemoveOne(key, inst.element)
		}

		if rand.Float64() < flushChance {
			require.Nil(t, b.FlushAndSwitch())

			for b.disk.eligibleForCompaction() {
				require.Nil(t, b.disk.compactOnce())
				compactions++
			}
		}

	}

	// this is a sanity check to make sure the test setup actually does what we
	// want. With the current setup, we expect on avarage to have ~100
	// compactions. It would be extremely unexpected to have fewer than 25.
	assert.Greater(t, compactions, 25)

	verifyBucketAgainstControl(t, b, control)
}

func verifyBucketAgainstControl(t *testing.T, b *Bucket, control []*sroar.Bitmap) {
	// This test was built before the bucket had cursors, so we are retrieving
	// each key individually, rather than cursing over the entire bucket.
	// However, this is also good for isolation purposes, this test tests
	// compactions, not cursors.

	for i, controlBM := range control {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))

		actual, err := b.RoaringSetGet(key)
		require.Nil(t, err)

		assert.Equal(t, controlBM.ToArray(), actual.ToArray())

	}
}

type roaringSetInstruction struct {
	// is a []byte in reality, but makes the test setup easier if we pretent
	// its an int
	key     uint64
	element uint64

	// true=addition, false=deletion
	addition bool
}

func generateRandomInstructions(maxID, maxElement, iterations uint64,
	deleteRatio float64,
) []roaringSetInstruction {
	instr := make([]roaringSetInstruction, iterations)

	for i := range instr {
		instr[i].key = uint64(rand.Intn(int(maxID)))
		instr[i].element = uint64(rand.Intn(int(maxElement)))

		if rand.Float64() > deleteRatio {
			instr[i].addition = true
		} else {
			instr[i].addition = false
		}
	}

	return instr
}

func controlFromInstructions(instr []roaringSetInstruction, maxID uint64) []*sroar.Bitmap {
	out := make([]*sroar.Bitmap, maxID)
	for i := range out {
		out[i] = sroar.NewBitmap()
	}

	for _, inst := range instr {
		if inst.addition {
			out[inst.key].Set(inst.element)
		} else {
			out[inst.key].Remove(inst.element)
		}
	}

	return out
}
