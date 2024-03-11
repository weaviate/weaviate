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

package hashtree

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentedHashTreeSerialization(t *testing.T) {
	totalSegmentsCount := 128

	segmentSize := uint64(math.MaxUint64 / uint64(totalSegmentsCount))

	segments := make([]uint64, 30)

	for i, s := range rand.Perm(totalSegmentsCount)[:len(segments)] {
		segments[i] = uint64(s) * segmentSize
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })

	for h := 1; h < 10; h++ {
		ht, err := NewSegmentedHashTree(segmentSize, segments, h)
		require.NoError(t, err)

		actualNumberOfElementsPerSegment := 1_000

		valuePrefix := "somevalue"

		for _, s := range segments {
			for i := 0; i < actualNumberOfElementsPerSegment; i++ {
				l := s + uint64(rand.Int()%int(segmentSize))
				err = ht.AggregateLeafWith(l, []byte(fmt.Sprintf("%s%d", valuePrefix, l)))
				require.NoError(t, err)
			}
		}

		var buf bytes.Buffer

		_, err = ht.Serialize(&buf)
		require.NoError(t, err)

		readBuf := bytes.NewBuffer(buf.Bytes())

		ht1, err := DeserializeSegmentedHashTree(readBuf)
		require.NoError(t, err)
		require.Equal(t, ht.SegmentSize(), ht1.SegmentSize())
		require.Equal(t, ht.Segments(), ht1.Segments())
		require.Equal(t, ht.Height(), ht1.Height())
		require.Equal(t, ht.Root(), ht1.Root())
	}
}
