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

// filteredscan measures the three-stage filtered prefix scan
// (hnsw.FilteredPrefixScan) against the frozen wiki-dpr filtered benchmark:
// 10M passages, 27 global filters (topical/random/conjunction) × 1000 NQ
// queries plus 300 per-query anti-correlated filters, exact per-pair ground
// truth. Corpus floats are mmapped straight out of the HDF5 (the train
// dataset is contiguous at a fixed offset); no server import.
//
// Single-threaded by design this round; concurrency is the next round.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const (
	corpusN     = 10_000_000
	dims        = 768
	rowBytes    = dims * 4
	trainOffset = 2048 // train dataset offset inside the HDF5 (contiguous layout, verified)
)

type filterMeta struct {
	Name     string `json:"name"`
	Family   string `json:"family"`
	Size     int    `json:"size"`
	Scope    string `json:"scope"`
	QueryRow int    `json:"query_row"`
}

type noopBucketView struct{}

func (noopBucketView) ReleaseView() {}

func main() {
	hdf5Path := flag.String("hdf5", "/Users/abdel/Documents/Projects/hnsw+pq/datasets/wiki-dpr-10m-e5-filtered/wiki-dpr-10m-e5b-filtered.hdf5", "frozen dataset")
	sidecar := flag.String("sidecar", filepath.Join(os.Getenv("HOME"), "Documents/datasets/wikidpr-10m-scan"), "exported filters/GT dir")
	limit := flag.Int("limit", corpusN, "corpus rows (smoke tests; GT invalid below full size)")
	b1 := flag.Int("b1", 4096, "stage-1 budget")
	b2 := flag.Int("b2", 700, "stage-2 budget")
	trainLimit := flag.Int("trainlimit", 10000, "centering training limit")
	csvPath := flag.String("csv", "filteredscan-results.csv", "per-filter CSV output")
	flag.Parse()
	if err := run(*hdf5Path, *sidecar, *limit, *b1, *b2, *trainLimit, *csvPath); err != nil {
		fmt.Fprintf(os.Stderr, "filteredscan: %v\n", err)
		os.Exit(1)
	}
}

func run(hdf5Path, sidecar string, limit, b1, b2, trainLimit int, csvPath string) error {
	ctx := context.Background()

	// mmap the corpus floats straight out of the HDF5.
	f, err := os.Open(hdf5Path)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	floatsFor := func(id uint64) []float32 {
		off := trainOffset + int64(id)*rowBytes
		return unsafe.Slice((*float32)(unsafe.Pointer(&data[off])), dims)
	}

	queries, err := loadF32(filepath.Join(sidecar, "test.f32"), dims)
	if err != nil {
		return err
	}
	var metas []filterMeta
	mb, err := os.ReadFile(filepath.Join(sidecar, "meta.json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(mb, &metas); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "corpus %d rows (limit %d), %d queries, %d filters\n",
		corpusN, limit, len(queries)/dims, len(metas))

	// The arena cache is the point of the exercise: set the toggle before
	// the index exists, loudly.
	os.Setenv("VECTOR_CACHE_IMPL", "arena")
	fmt.Fprintln(os.Stderr, "VECTOR_CACHE_IMPL=arena")

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	uc := enthnsw.NewDefaultUserConfig()
	uc.VectorCacheMaxObjects = 1e13
	// The graph is not used by the scan; build it as cheaply as the index
	// allows while keeping the real activation path.
	uc.MaxConnections = 8
	uc.EFConstruction = 32
	uc.RQ = enthnsw.RQConfig{
		Enabled: true, Bits: 1, Centering: true,
		TrainingLimit: trainLimit, RescoreLimit: 100,
	}

	scratchDir, err := os.MkdirTemp("", "filteredscan-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratchDir)
	store, err := lsmkv.New(scratchDir, scratchDir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return err
	}

	index, err := hnsw.New(hnsw.Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              scratchDir,
		ID:                    "filteredscan",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= limit {
				return nil, storobj.NewErrNotFoundf(id, "oor")
			}
			return floatsFor(id), nil
		},
		GetViewThunk: func() common.BucketView { return noopBucketView{} },
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, c *common.VectorSlice, v common.BucketView) ([]float32, error) {
			copy(c.Slice, floatsFor(id))
			return c.Slice, nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	if err != nil {
		return err
	}
	defer index.Shutdown(ctx)
	index.PostStartup(ctx)

	// Real activation sequencing: train at trainLimit, then the rest.
	start := time.Now()
	trainN := uint64(trainLimit + 1)
	if err := compressionhelpers.ConcurrentlyWithError(logger, trainN, func(id uint64) error {
		return index.Add(ctx, id, floatsFor(id))
	}); err != nil {
		return err
	}
	var uwg sync.WaitGroup
	uwg.Add(1)
	if err := index.Upgrade(uwg.Done); err != nil {
		return err
	}
	uwg.Wait()
	if !index.Compressed() {
		return fmt.Errorf("activation failed")
	}
	fmt.Fprintf(os.Stderr, "trained at %d (%.0fs); building remaining %d ...\n", trainN, time.Since(start).Seconds(), limit-int(trainN))
	if err := compressionhelpers.ConcurrentlyWithError(logger, uint64(limit)-trainN, func(i uint64) error {
		id := trainN + i
		return index.Add(ctx, id, floatsFor(id))
	}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "index built in %.0fs\n", time.Since(start).Seconds())

	cfg := hnsw.FilteredScanConfig{Budget1: b1, Budget2: b2, FloatsForID: func(id uint64) []float32 {
		return floatsFor(id)
	}}
	scratch := hnsw.NewFilteredScanScratch(cfg)

	csv, err := os.Create(csvPath)
	if err != nil {
		return err
	}
	defer csv.Close()
	fmt.Fprintln(csv, "filter,family,size,queries,recall_at_10,perfect_frac,p50_ms,p95_ms,p99_ms,"+
		"allow_iter_p50_ms,stage1_p50_ms,stage2_p50_ms,stage3_p50_ms,stage1_mb_per_q,stage2_kb_per_q,stage3_kb_per_q")

	k := 10
	for _, m := range metas {
		bits, err := os.ReadFile(filepath.Join(sidecar, "filters", m.Name+".bits"))
		if err != nil {
			return err
		}
		gtRaw, err := os.ReadFile(filepath.Join(sidecar, "filters", m.Name+".gt.i64"))
		if err != nil {
			return err
		}
		allow := allowListFromPackbits(bits, limit)

		var qRows []int
		if m.Scope == "per_query" {
			qRows = []int{m.QueryRow}
		} else {
			for i := 0; i < len(queries)/dims; i++ {
				qRows = append(qRows, i)
			}
		}

		var totals, iters, s1s, s2s, s3s []float64
		var b1MB, b2KB, b3KB float64
		var hits, wanted, perfect int
		for qi, qRow := range qRows {
			q := queries[qRow*dims : (qRow+1)*dims]
			ids, _, stats, err := index.FilteredPrefixScan(ctx, q, k, allow, cfg, scratch)
			if err != nil {
				return fmt.Errorf("filter %s query %d: %w", m.Name, qRow, err)
			}
			total := stats.AllowIter + stats.Stage1 + stats.Stage2 + stats.Stage3
			totals = append(totals, total.Seconds()*1000)
			iters = append(iters, stats.AllowIter.Seconds()*1000)
			s1s = append(s1s, stats.Stage1.Seconds()*1000)
			s2s = append(s2s, stats.Stage2.Seconds()*1000)
			s3s = append(s3s, stats.Stage3.Seconds()*1000)
			b1MB += float64(stats.Stage1Bytes) / (1 << 20)
			b2KB += float64(stats.Stage2Bytes) / 1024
			b3KB += float64(stats.Stage3Bytes) / 1024

			// GT row: global filters index by query row, per-query by 0
			gtStart := qi * 100
			if m.Scope == "per_query" {
				gtStart = 0
			} else {
				gtStart = qRow * 100
			}
			truth := map[uint64]bool{}
			for i := 0; i < k; i++ {
				truth[binary.LittleEndian.Uint64(gtRaw[(gtStart+i)*8:])] = true
			}
			qHits := 0
			for _, id := range ids {
				if truth[id] {
					qHits++
				}
			}
			hits += qHits
			wanted += k
			if qHits == k {
				perfect++
			}
		}
		nQ := float64(len(qRows))
		recall := float64(hits) / float64(wanted)
		fmt.Fprintf(csv, "%s,%s,%d,%d,%.4f,%.4f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.2f,%.1f,%.1f\n",
			m.Name, m.Family, m.Size, len(qRows), recall, float64(perfect)/nQ,
			pct(totals, 0.50), pct(totals, 0.95), pct(totals, 0.99),
			pct(iters, 0.50), pct(s1s, 0.50), pct(s2s, 0.50), pct(s3s, 0.50),
			b1MB/nQ, b2KB/nQ, b3KB/nQ)
		fmt.Fprintf(os.Stderr, "%-28s %-11s size=%-8d recall@10=%.4f perfect=%.3f p50=%.2fms (iter %.2f | s1 %.2f | s2 %.2f | s3 %.2f)\n",
			m.Name, m.Family, m.Size, recall, float64(perfect)/nQ,
			pct(totals, 0.50), pct(iters, 0.50), pct(s1s, 0.50), pct(s2s, 0.50), pct(s3s, 0.50))
		allow.Close()
	}
	return nil
}

// allowListFromPackbits converts a numpy packbits (big-endian bit order)
// bitmap to a sroar-backed allowlist.
func allowListFromPackbits(bits []byte, limit int) helpers.AllowList {
	bm := sroar.NewBitmap()
	ids := make([]uint64, 0, 1<<16)
	flush := func() {
		if len(ids) > 0 {
			bm.SetMany(ids)
			ids = ids[:0]
		}
	}
	for byteIdx, b := range bits {
		if b == 0 {
			continue
		}
		base := uint64(byteIdx) * 8
		for bit := 0; bit < 8; bit++ {
			if b&(0x80>>bit) != 0 {
				id := base + uint64(bit)
				if int(id) < limit {
					ids = append(ids, id)
				}
			}
		}
		if len(ids) >= 1<<16 {
			flush()
		}
	}
	flush()
	return helpers.NewAllowListFromBitmap(bm)
}

func loadF32(path string, dims int) ([]float32, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out, nil
}

func pct(vals []float64, q float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	return s[int(q*float64(len(s)-1)+0.5)]
}
