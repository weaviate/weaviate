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

func TestMultiSegmentHashTreeSerialization(t *testing.T) {
	totalSegmentsCount := 128

	segmentSize := uint64(math.MaxUint64 / uint64(totalSegmentsCount))

	segments := make([]Segment, 30)

	for i, s := range rand.Perm(totalSegmentsCount)[:len(segments)] {
		segments[i] = NewSegment(uint64(s)*segmentSize, segmentSize)
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].Start() < segments[j].Start() })

	for h := 1; h < 2; h++ {
		ht, err := NewMultiSegmentHashTree(segments, h)
		require.NoError(t, err)

		actualNumberOfElementsPerSegment := 1_000

		valuePrefix := "somevalue"

		for _, s := range segments {
			for i := 0; i < actualNumberOfElementsPerSegment; i++ {
				l := s.Start() + uint64(rand.Int()%int(s.Size()))
				err = ht.AggregateLeafWith(l, []byte(fmt.Sprintf("%s%d", valuePrefix, l)))
				require.NoError(t, err)
			}
		}

		var buf bytes.Buffer

		_, err = ht.Serialize(&buf)
		require.NoError(t, err)

		readBuf := bytes.NewBuffer(buf.Bytes())

		ht1, err := DeserializeMultiSegmentHashTree(readBuf)
		require.NoError(t, err)
		require.Equal(t, ht.Segments(), ht1.Segments())
		require.Equal(t, ht.Height(), ht1.Height())
		require.Equal(t, ht.Root(), ht1.Root())
	}
}
