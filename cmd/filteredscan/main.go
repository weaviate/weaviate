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
	"strings"
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
	b1Alt := flag.Int("b1alt", 0, "optional second stage-1 budget: run every filter at both")
	only := flag.String("only", "", "only run filters whose name has this prefix")
	mode := flag.String("mode", "scan", "scan (three-stage prefix scan) or acorn (current filtered graph search)")
	efsArg := flag.String("efs", "64,128,256,512", "acorn mode: ef sweep")
	b2 := flag.Int("b2", 700, "stage-2 budget")
	trainLimit := flag.Int("trainlimit", 10000, "centering training limit")
	csvPath := flag.String("csv", "filteredscan-results.csv", "per-filter CSV output")
	flag.Parse()
	if err := run(*hdf5Path, *sidecar, *limit, *b1, *b1Alt, *b2, *trainLimit, *csvPath, *only, *mode, *efsArg); err != nil {
		fmt.Fprintf(os.Stderr, "filteredscan: %v\n", err)
		os.Exit(1)
	}
}

func run(hdf5Path, sidecar string, limit, b1, b1Alt, b2, trainLimit int, csvPath, only, mode, efsArg string) error {
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
	if mode == "scan" {
		// The graph is not used by the scan; build it as cheaply as the
		// index allows while keeping the real activation path.
		uc.MaxConnections = 8
		uc.EFConstruction = 32
	}
	// acorn mode keeps the defaults: MaxConnections/EFConstruction as
	// shipped, FilterStrategy already defaults to acorn.
	rescore := 100
	if mode == "acorn" {
		rescore = enthnsw.DefaultBRQRescoreLimit // the bits=1 default, untuned
	}
	uc.RQ = enthnsw.RQConfig{
		Enabled: true, Bits: 1, Centering: true,
		TrainingLimit: trainLimit, RescoreLimit: rescore,
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

	budgets := []int{b1}
	if b1Alt > 0 {
		budgets = append(budgets, b1Alt)
	}
	var efs []int
	for _, part := range strings.Split(efsArg, ",") {
		var v int
		fmt.Sscanf(strings.TrimSpace(part), "%d", &v)
		if v > 0 {
			efs = append(efs, v)
		}
	}

	csv, err := os.Create(csvPath)
	if err != nil {
		return err
	}
	defer csv.Close()
	fmt.Fprintln(csv, "method,filter,family,size,b1,queries,recall_at_10,perfect_frac,sim_regret,p50_ms,p95_ms,p99_ms,"+
		"allow_iter_p50_ms,stage1_p50_ms,stage2_p50_ms,stage3_p50_ms,stage1_mb_per_q,stage2_kb_per_q,stage3_kb_per_q")

	k := 10
	dot := func(a, b []float32) float64 {
		var s float64
		for i := range a {
			s += float64(a[i]) * float64(b[i])
		}
		return s
	}
	for _, m := range metas {
		if only != "" && !strings.HasPrefix(m.Name, only) {
			continue
		}
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

		type armSpec struct {
			label   string
			budget1 int
			ef      int
		}
		var arms []armSpec
		if mode == "acorn" {
			for _, ef := range efs {
				arms = append(arms, armSpec{label: fmt.Sprintf("acorn-ef%d", ef), ef: ef})
			}
		} else {
			for _, budget1 := range budgets {
				arms = append(arms, armSpec{label: "scan", budget1: budget1})
			}
		}
		for _, arm := range arms {
			budget1 := arm.budget1
			if arm.ef > 0 {
				uc.EF = arm.ef
				var cwg sync.WaitGroup
				cwg.Add(1)
				if err := index.UpdateUserConfig(uc, cwg.Done); err != nil {
					return err
				}
				cwg.Wait()
			}
			cfg := hnsw.FilteredScanConfig{Budget1: budget1, Budget2: b2, FloatsForID: func(id uint64) []float32 {
				return floatsFor(id)
			}}
			scratch := hnsw.NewFilteredScanScratch(cfg)
			var regretSum float64

			var totals, iters, s1s, s2s, s3s []float64
			var b1MB, b2KB, b3KB float64
			var hits, wanted, perfect int
			for qi, qRow := range qRows {
				q := queries[qRow*dims : (qRow+1)*dims]
				var ids []uint64
				var stats hnsw.FilteredScanStats
				if arm.ef > 0 {
					// the current filtered graph path: ACORN strategy (the
					// default) inside SearchByVector, whole query timed as
					// one stage
					qStart := time.Now()
					gotIDs, _, err := index.SearchByVector(ctx, q, k, allow)
					if err != nil {
						return fmt.Errorf("acorn filter %s query %d: %w", m.Name, qRow, err)
					}
					ids = gotIDs
					stats.Stage1 = time.Since(qStart)
					stats.Members = allow.Len()
				} else {
					var err error
					ids, _, stats, err = index.FilteredPrefixScan(ctx, q, k, allow, cfg, scratch)
					if err != nil {
						return fmt.Errorf("filter %s query %d: %w", m.Name, qRow, err)
					}
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
				// similarity regret: mean sim of GT top-k minus mean sim of the
				// scan's returned k — quantifies answer QUALITY independent of
				// set overlap (tie-band filters can have zero overlap and zero
				// regret at once)
				var gtSim, gotSim float64
				for i := 0; i < k; i++ {
					gtSim += dot(q, floatsFor(binary.LittleEndian.Uint64(gtRaw[(gtStart+i)*8:])))
				}
				for _, id := range ids {
					gotSim += dot(q, floatsFor(id))
				}
				if len(ids) == k {
					regretSum += (gtSim - gotSim) / float64(k)
				}
			}
			nQ := float64(len(qRows))
			recall := float64(hits) / float64(wanted)
			fmt.Fprintf(csv, "%s,%s,%s,%d,%d,%d,%.4f,%.4f,%.5f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.2f,%.1f,%.1f\n",
				arm.label, m.Name, m.Family, m.Size, budget1, len(qRows), recall, float64(perfect)/nQ, regretSum/nQ,
				pct(totals, 0.50), pct(totals, 0.95), pct(totals, 0.99),
				pct(iters, 0.50), pct(s1s, 0.50), pct(s2s, 0.50), pct(s3s, 0.50),
				b1MB/nQ, b2KB/nQ, b3KB/nQ)
			fmt.Fprintf(os.Stderr, "%-11s %-28s %-11s size=%-8d recall@10=%.4f perfect=%.3f regret=%.5f p50=%.2fms (iter %.2f | s1 %.2f | s2 %.2f | s3 %.2f)\n",
				arm.label, m.Name, m.Family, m.Size, recall, float64(perfect)/nQ, regretSum/nQ,
				pct(totals, 0.50), pct(iters, 0.50), pct(s1s, 0.50), pct(s2s, 0.50), pct(s3s, 0.50))
		}
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
