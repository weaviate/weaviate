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

package tachiomfresh

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkPackedPostingDocIDs is the cost of stage 1's decode per posting
// at each doc-id width, on postings of 100 entries.
func BenchmarkPackedPostingDocIDs(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	for _, docBytes := range []int{1, 2, 3, 4} {
		refs := make([]tokenRef, 100)
		for i := range refs {
			refs[i] = tokenRef{DocID: uint32(rng.Intn(1 << (8 * docBytes))), Pos: uint16(rng.Intn(180))}
		}
		p := encodePosting(refs, docBytes, 1, false)
		buf := make([]uint32, 0, len(refs))
		b.Run(fmt.Sprintf("docBytes=%d", docBytes), func(b *testing.B) {
			var sum uint32
			for range b.N {
				for _, id := range p.docIDs(buf) {
					sum += id
				}
			}
			_ = sum
		})
	}
}

// BenchmarkPackedPostingAppendRemove is the cost of the copy an append and
// a removal make (the arrays are sized exactly).
func BenchmarkPackedPostingAppendRemove(b *testing.B) {
	refs := make([]tokenRef, 100)
	for i := range refs {
		refs[i] = tokenRef{DocID: uint32(i * 7), Pos: uint16(i)}
	}
	p := encodePosting(refs, 2, 1, false)
	b.Run("append", func(b *testing.B) {
		for range b.N {
			_ = p.append(tokenRef{DocID: 1000, Pos: 3})
		}
	})
	b.Run("remove last", func(b *testing.B) {
		for range b.N {
			_, _ = p.remove(refs[99])
		}
	})
	b.Run("decode", func(b *testing.B) {
		for range b.N {
			_ = p.decode()
		}
	})
}
