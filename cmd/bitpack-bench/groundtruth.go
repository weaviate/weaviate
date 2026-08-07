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

package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// computeGroundTruth writes exact top-gtK neighbors by inner product (rows
// are expected L2-normalized, so this is cosine order) for every query over
// the base set, row-major int32, to outPath. Needed whenever a dataset
// subset is used: shipped ground truth for the full set is invalid for any
// subset.
func computeGroundTruth(base []float32, dims, n int, queries []float32, nq, gtK int, outPath string) error {
	if gtK > n {
		return fmt.Errorf("gtK %d > base size %d", gtK, n)
	}
	dot := distancer.NewDotProductProvider()
	out := make([]int32, nq*gtK)
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	start := time.Now()
	compressionhelpers.Concurrently(logger, uint64(nq), func(qi uint64) {
		q := queries[int(qi)*dims : (int(qi)+1)*dims]
		type cand struct {
			id   int32
			dist float32 // SingleDist of dot provider: -<q,x>, ascending = best first
		}
		top := make([]cand, 0, gtK+1)
		worst := float32(0)
		for id := 0; id < n; id++ {
			d, _ := dot.SingleDist(q, base[id*dims:(id+1)*dims])
			if len(top) < gtK {
				top = append(top, cand{int32(id), d})
				if len(top) == gtK {
					sort.Slice(top, func(a, b int) bool { return top[a].dist < top[b].dist })
					worst = top[gtK-1].dist
				}
				continue
			}
			if d >= worst {
				continue
			}
			// Insert in sorted position, drop the last.
			pos := sort.Search(gtK, func(i int) bool { return top[i].dist > d })
			copy(top[pos+1:], top[pos:gtK-1])
			top[pos] = cand{int32(id), d}
			worst = top[gtK-1].dist
		}
		if len(top) < gtK {
			sort.Slice(top, func(a, b int) bool { return top[a].dist < top[b].dist })
		}
		for i, c := range top {
			out[int(qi)*gtK+i] = c.id
		}
	})
	fmt.Fprintf(os.Stderr, "ground truth: %d queries x top-%d over %d rows in %.1fs\n",
		nq, gtK, n, time.Since(start).Seconds())

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 4*len(out))
	for i, v := range out {
		binary.LittleEndian.PutUint32(buf[i*4:], uint32(v))
	}
	if _, err := f.Write(buf); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d bytes)\n", outPath, len(buf))
	return nil
}
