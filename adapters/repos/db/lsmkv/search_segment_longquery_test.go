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

//go:build integrationTest

package lsmkv

import (
	"context"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// Regression: a long (many-term) BM25 query must not hang the WAND loop.
// Watchdog (not a ctx deadline) so non-termination fails instead of masking as a
// timeout. (Repro authored by QA Claude.)
func TestBlockMaxWandLongQueryTerminates(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })
	bucket.SetMemtableThreshold(1 << 30)

	const nDocs, vocab, termsPerDoc = 50000, 30000, 40
	rng := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rng, 1.07, 1.0, uint64(vocab-1))
	key := func(id uint64) string { return "t" + strconv.FormatUint(id, 36) }
	var plSum float64
	for d := 0; d < nDocs; d++ {
		pl := float32(20 + rng.Intn(500))
		plSum += float64(pl)
		seen := make(map[uint64]uint32, termsPerDoc)
		for k := 0; k < termsPerDoc; k++ {
			seen[zipf.Uint64()]++
		}
		for tid, tf := range seen {
			require.NoError(t, bucket.MapSet([]byte(key(tid)),
				NewMapPairFromDocIdAndTf(uint64(d), float32(tf), pl, false)))
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())
	avgPropLen := plSum / float64(nDocs)

	// 200 unique terms, Zipf-drawn with count boosts (mirrors a real paste:
	// AnalyzeAndCountDuplicates dedups to unique terms + duplicateBoosts).
	qz := rand.NewZipf(rand.New(rand.NewSource(99)), 1.07, 1.0, uint64(vocab-1))
	counts := map[string]int{}
	for len(counts) < 200 {
		counts[key(qz.Uint64())]++
	}
	terms, boosts := make([]string, 0, 200), make([]int, 0, 200)
	for tk, c := range counts {
		terms, boosts = append(terms, tk), append(boosts, c)
	}

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(nDocs), nil, terms, "", 1, boosts, schema.BM25Config{K1: 1.2, B: 0.75})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		for _, segTerms := range diskTerms {
			if len(segTerms) == 0 {
				continue
			}
			h, _ := DoBlockMaxWand(ctx, 10, segTerms, avgPropLen, false, len(terms), 1, logger)
			for h != nil && h.Len() > 0 {
				h.Pop()
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("DoBlockMaxWand did not terminate within 20s on a 200-term query (pivot not advancing)")
	}
}
