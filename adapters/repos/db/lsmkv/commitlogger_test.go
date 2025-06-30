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

package lsmkv

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkCommitlogWriter(b *testing.B) {
	for _, val := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", val), func(b *testing.B) {
			cl, err := newCommitLogger(b.TempDir(), "n/a", 0)
			require.NoError(b, err)

			data := make([]byte, val)
			for i := 0; i < len(data); i++ {
				data[i] = byte(rand.Intn(100))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := cl.writeEntry(CommitTypeReplace, data)
				require.NoError(b, err)
			}
		})
	}
}
