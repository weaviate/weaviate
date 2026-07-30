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

// rankCurveResult holds, per block (depth = (b+1)*64), the per-query
// maximum rank of the k true neighbours under two tie conventions:
//   - worst: strictly closer vectors plus ALL equal-distance vectors — the
//     budget that guarantees retention under any tie-breaking.
//   - expected: strictly closer vectors plus half the tie bucket — the
//     budget that retains the neighbour in expectation under arbitrary
//     tie-breaking. At shallow depths ties are enormous (65 distance values
//     for ~1M vectors at depth 64), so the conventions diverge exactly
//     where the budget matters most.
type rankCurveResult struct {
	worst    [][]int
	expected [][]int
}

// rankCurve measures, for each prefix depth d (64, 128, ..., retained bits),
// the rank of each query's k true neighbours among all base vectors by
// prefix-Hamming distance. The per-query statistic is the MAXIMUM of those k
// ranks: the smallest candidate budget at depth d that retains all k true
// neighbours for that query.
//
// Prefixes grow one block at a time, so the whole curve for one query is one
// accumulation pass plus one histogram pass per block. Percentile
// aggregation happens after the measurement loop.
func rankCurve(store *bitStore, queries []float32, dims, nq int, gt []int32, gtCols, k, sampleN int) rankCurveResult {
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

	// res.worst[b] / res.expected[b] hold the per-query max rank at depth
	// (b+1)*64 under each tie convention.
	res := rankCurveResult{
		worst:    make([][]int, blocks),
		expected: make([][]int, blocks),
	}
	for b := 0; b < blocks; b++ {
		res.worst[b] = make([]int, 0, sampleN)
		res.expected[b] = make([]int, 0, sampleN)
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

			maxWorst, maxExp := 0, 0
			for i := 0; i < k; i++ {
				gtID := int(gt[qi*gtCols+i])
				d := int(acc[gtID])
				// Worst case under ties: strictly closer + all ties.
				worst := int(cum[d+1])
				// Expected case: strictly closer + half the tie bucket
				// (the bucket includes the neighbour itself).
				exp := int(cum[d]) + (int(hist[d])+1)/2
				if worst > maxWorst {
					maxWorst = worst
				}
				if exp > maxExp {
					maxExp = exp
				}
			}
			res.worst[b] = append(res.worst[b], maxWorst)
			res.expected[b] = append(res.expected[b], maxExp)
		}
		if (s+1)%100 == 0 {
			fmt.Fprintf(os.Stderr, "  rank curve: %d/%d queries (%.0fs)\n", s+1, sampleN, time.Since(start).Seconds())
		}
	}
	return res
}

type rankCurveRow struct {
	depth                  int
	p50, p95, p99, max     int // worst-case ties
	p50e, p95e, p99e, maxe int // expected-case ties
}

// quantileInt returns the q-th percentile of vals (vals is sorted in place).
func quantileInt(vals []int, q float64) int {
	sort.Ints(vals)
	return vals[int(q*float64(len(vals)-1)+0.5)]
}

func rankCurveStats(res rankCurveResult) []rankCurveRow {
	rows := make([]rankCurveRow, 0, len(res.worst))
	for b := range res.worst {
		w := make([]int, len(res.worst[b]))
		copy(w, res.worst[b])
		sort.Ints(w)
		e := make([]int, len(res.expected[b]))
		copy(e, res.expected[b])
		sort.Ints(e)
		pick := func(s []int, q float64) int {
			return s[int(q*float64(len(s)-1)+0.5)]
		}
		rows = append(rows, rankCurveRow{
			depth: (b + 1) * 64,
			p50:   pick(w, 0.50),
			p95:   pick(w, 0.95),
			p99:   pick(w, 0.99),
			max:   w[len(w)-1],
			p50e:  pick(e, 0.50),
			p95e:  pick(e, 0.95),
			p99e:  pick(e, 0.99),
			maxe:  e[len(e)-1],
		})
	}
	return rows
}

// generateBudgets derives a fixed budget schedule from the rank curve: the
// q-th percentile of the expected-case per-query max rank at each depth,
// floored at the final rescore count, capped at n, and made nonincreasing.
func generateBudgets(res rankCurveResult, q float64, floor, n int) []int {
	budgets := make([]int, len(res.expected))
	prev := n
	for b := range res.expected {
		vals := make([]int, len(res.expected[b]))
		copy(vals, res.expected[b])
		v := quantileInt(vals, q)
		if v < floor {
			v = floor
		}
		if v > prev {
			v = prev
		}
		budgets[b] = v
		prev = v
	}
	return budgets
}

func printRankCurve(w *os.File, config string, sampleN, k int, rows []rankCurveRow) {
	fmt.Fprintf(w, "\n=== prefix rank curve: %s (%d queries, max rank over %d true neighbours) ===\n", config, sampleN, k)
	fmt.Fprintf(w, "                 worst-case ties                    expected-case ties\n")
	fmt.Fprintf(w, "depth       p50       p95       p99       max       p50       p95       p99       max\n")
	for _, r := range rows {
		fmt.Fprintf(w, "%5d %9d %9d %9d %9d %9d %9d %9d %9d\n",
			r.depth, r.p50, r.p95, r.p99, r.max, r.p50e, r.p95e, r.p99e, r.maxe)
	}
}

var rankCSVHeader = strings.Join([]string{
	"timestamp", "dataset", "config", "rotate", "center", "retained", "queries", "k", "depth",
	"p50_worst", "p95_worst", "p99_worst", "max_worst",
	"p50_exp", "p95_exp", "p99_exp", "max_exp",
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
		if _, err := fmt.Fprintf(f, "%s,%s,%s,%v,%v,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
			ts, dataset, config, rotate, center, retained, sampleN, k,
			r.depth, r.p50, r.p95, r.p99, r.max, r.p50e, r.p95e, r.p99e, r.maxe); err != nil {
			return err
		}
	}
	return nil
}
