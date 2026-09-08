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

package replica

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
)

func TestRepairDigestsBinaryCodec(t *testing.T) {
	digests := []types.RepairDigest{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), UpdateTime: 1},
		{ID: uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"), UpdateTime: math.MaxInt64, Deleted: true},
		{ID: uuid.New(), UpdateTime: -42},
		{ID: uuid.UUID{}, UpdateTime: 0},
	}

	t.Run("round trip", func(t *testing.T) {
		decoded, err := RepairDigestsFromBinary(RepairDigestsToBinary(digests))
		require.NoError(t, err)
		assert.Equal(t, digests, decoded)
	})

	t.Run("record layout", func(t *testing.T) {
		out := RepairDigestsToBinary(digests[:2])
		require.Len(t, out, 2*CompareDigestsRecordLength)
		assert.Equal(t, digests[0].ID[:], out[:16])
		assert.Equal(t, uint64(1), binary.BigEndian.Uint64(out[16:24]))
		assert.Equal(t, byte(0), out[24])
		assert.Equal(t, CompareDigestsFlagDeleted, out[2*CompareDigestsRecordLength-1])
	})

	t.Run("empty", func(t *testing.T) {
		assert.Empty(t, RepairDigestsToBinary(nil))
		decoded, err := RepairDigestsFromBinary(nil)
		require.NoError(t, err)
		assert.Empty(t, decoded)
	})

	t.Run("invalid lengths", func(t *testing.T) {
		for _, n := range []int{1, 24, 26, 49, 51} {
			_, err := RepairDigestsFromBinary(make([]byte, n))
			assert.ErrorContains(t, err, "not a multiple", "length %d", n)
		}
	})
}
