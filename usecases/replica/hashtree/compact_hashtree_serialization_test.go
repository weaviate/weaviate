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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactHashTreeSerialization(t *testing.T) {
	capacity := uint64(math.MaxUint64)

	for h := 0; h < 10; h++ {
		ht, err := NewCompactHashTree(capacity, h)
		require.NoError(t, err)

		require.Equal(t, h, ht.Height())

		leavesCount := LeavesCount(ht.Height())
		valuePrefix := "somevalue"

		for i := 0; i < leavesCount; i++ {
			err = ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
			require.NoError(t, err)
		}

		var buf bytes.Buffer

		_, err = ht.Serialize(&buf)
		require.NoError(t, err)

		readBuf := bytes.NewBuffer(buf.Bytes())

		ht1, err := DeserializeCompactHashTree(readBuf)
		require.NoError(t, err)
		require.Equal(t, ht.Capacity(), ht1.Capacity())
		require.Equal(t, ht.Height(), ht1.Height())
		require.Equal(t, ht.Root(), ht1.Root())
	}
}
