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
	"fmt"
	"math/bits"
	"os"
	"sort"
	"strings"
	"time"
)

// rankCurve measures, for each prefix depth d (64, 128, ..., retained bits),
// the rank of each query's k true neighbours among all base vectors by
// prefix-Hamming distance. The per-query statistic is the MAXIMUM of those k
// ranks: the smallest candidate budget at depth d that retains all k true
// neighbours for that query. Ranks are worst-case under ties (count of
// strictly closer vectors plus all equal-distance vectors), because a budget
// cut breaks ties arbitrarily.
//
// Prefixes grow one block at a time, so the whole curve for one query is one
// accumulation pass plus one histogram pass per block. Percentile
// aggregation happens after the measurement loop.
func rankCurve(store *bitStore, queries []float32, dims, nq int, gt []int32, gtCols, k, sampleN int) [][]int {
	n := store.n
	blocks := store.blocks

	if sampleN > nq {
		sampleN = nq
	}
	stride := nq / sampleN

	sc := struct {
		center []float32
		rot    []float32
		words  []uint64
		acc    []uint16
		hist   []int32 // histogram of accumulated distances
		cum    []int64 // cum[d] = #vectors with distance < d
	}{
		center: make([]float32, dims),
		words:  make([]uint64, blocks),
		acc:    make([]uint16, n),
		hist:   make([]int32, store.retained+1),
		cum:    make([]int64, store.retained+2),
	}
	if store.rotation != nil {
		sc.rot = make([]float32, store.rotation.OutputDim)
	}

	// maxRanks[b] holds the per-query max rank at depth (b+1)*64.
	maxRanks := make([][]int, blocks)
	for b := range maxRanks {
		maxRanks[b] = make([]int, 0, sampleN)
	}

	start := time.Now()
	for s := 0; s < sampleN; s++ {
		qi := s * stride
		q := queries[qi*dims : (qi+1)*dims]
		store.encodeInto(q, sc.center, sc.rot, sc.words)

		for b := 0; b < blocks; b++ {
			col := store.codes[b*n : (b+1)*n]
			qb := sc.words[b]
			acc := sc.acc
			if b == 0 {
				for id := 0; id < n; id++ {
					acc[id] = uint16(bits.OnesCount64(col[id] ^ qb))
				}
			} else {
				for id := 0; id < n; id++ {
					acc[id] += uint16(bits.OnesCount64(col[id] ^ qb))
				}
			}

			maxD := (b + 1) * 64
			hist := sc.hist[:maxD+1]
			for i := range hist {
				hist[i] = 0
			}
			for id := 0; id < n; id++ {
				hist[acc[id]]++
			}
			cum := sc.cum[:maxD+2]
			cum[0] = 0
			for d := 0; d <= maxD; d++ {
				cum[d+1] = cum[d] + int64(hist[d])
			}

			maxRank := 0
			for i := 0; i < k; i++ {
				gtID := int(gt[qi*gtCols+i])
				d := int(acc[gtID])
				// Worst case under ties: strictly closer + all ties.
				rank := int(cum[d+1])
				if rank > maxRank {
					maxRank = rank
				}
			}
			maxRanks[b] = append(maxRanks[b], maxRank)
		}
		if (s+1)%50 == 0 {
			fmt.Fprintf(os.Stderr, "  rank curve: %d/%d queries (%.0fs)\n", s+1, sampleN, time.Since(start).Seconds())
		}
	}
	return maxRanks
}

type rankCurveRow struct {
	depth              int
	p50, p95, p99, max int
}

func rankCurveStats(maxRanks [][]int) []rankCurveRow {
	rows := make([]rankCurveRow, 0, len(maxRanks))
	for b, ranks := range maxRanks {
		s := make([]int, len(ranks))
		copy(s, ranks)
		sort.Ints(s)
		pick := func(q float64) int {
			return s[int(q*float64(len(s)-1)+0.5)]
		}
		rows = append(rows, rankCurveRow{
			depth: (b + 1) * 64,
			p50:   pick(0.50),
			p95:   pick(0.95),
			p99:   pick(0.99),
			max:   s[len(s)-1],
		})
	}
	return rows
}

func printRankCurve(w *os.File, config string, sampleN, k int, rows []rankCurveRow) {
	fmt.Fprintf(w, "\n=== prefix rank curve: %s (%d queries, max rank over %d true neighbours, worst-case ties) ===\n", config, sampleN, k)
	fmt.Fprintf(w, "depth       p50       p95       p99       max\n")
	for _, r := range rows {
		fmt.Fprintf(w, "%5d %9d %9d %9d %9d\n", r.depth, r.p50, r.p95, r.p99, r.max)
	}
}

var rankCSVHeader = strings.Join([]string{
	"timestamp", "dataset", "config", "rotate", "center", "retained", "queries", "k", "depth", "p50", "p95", "p99", "max",
}, ",")

func appendRankCSV(path, dataset, config string, rotate, center bool, retained, sampleN, k int, rows []rankCurveRow) error {
	newFile := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		newFile = true
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if newFile {
		fmt.Fprintln(f, rankCSVHeader)
	}
	ts := time.Now().Format(time.RFC3339)
	for _, r := range rows {
		if _, err := fmt.Fprintf(f, "%s,%s,%s,%v,%v,%d,%d,%d,%d,%d,%d,%d,%d\n",
			ts, dataset, config, rotate, center, retained, sampleN, k,
			r.depth, r.p50, r.p95, r.p99, r.max); err != nil {
			return err
		}
	}
	return nil
}
