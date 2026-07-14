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

package lsmkv

// Wall-clock (on-CPU + off-CPU) fgprof harness for the batched secondary resolver
// (gh#309). It answers WHERE the batch resolution wall time sits per phase, including
// the off-CPU value-read (pread) time that a plain `-cpuprofile` misses. The four
// resolution phases are call-tree siblings (never nested), so every sampled stack that
// touches the batch maps to exactly one phase by the phase-function symbol on the stack:
//
//	phase 0  memtable pass       -> (*Bucket).resolveSecondaryFromMemtables
//	phase 1  index descents      -> (*SegmentGroup).getBySecondaryBatchIndexHits
//	phase 2  value reads         -> (*SegmentGroup).readSecondaryBatchValuesConcurrent
//	phase 3  recheck             -> (*SegmentGroup).recheckSecondaryBatchInSegments
//	                                (*Bucket).secondaryPresentInMemtables
//
// It is gated behind LSMKV_FGPROF_OUT (a writable output dir). When unset the test
// t.Skip()s, so the normal lsmkv suite stays fast and deterministic and no profiling
// runs in CI. Run it explicitly:
//
//	LSMKV_FGPROF_OUT=$PWD/docs/bench/getbysecondary-fgprof \
//	  go test -run TestBucketGetBySecondaryBatchFgprofProfiles -count=1 \
//	  ./adapters/repos/db/lsmkv/
//
// Two shapes x two batch sizes = 4 profiles, each exported as a pprof file (for
// `go tool pprof`) and a folded-stacks file (for FlameGraph/speedscope), plus a
// README.md with the per-phase wall-time attribution table.
//
//   - warm: values page-cache resident. Profiles a timed loop over the REAL entry
//     point GetBySecondaryBatchWithView (nil hook). This is the shape QA's warm A/B
//     found the 1.05-1.15x regression on.
//   - simulated-cold: injected phase-2 read latency. Profiles a timed loop over a
//     faithful in-package replay of the entry point's exact phase order, calling the
//     SAME production phase functions, with the existing secondaryBatchReadHook sleeping
//     inside each phase-2 read goroutine. This surfaces the off-CPU value-read wall time
//     the warm shape hides. LIMITATION: the hook's scope is phase-2 value reads only, so
//     phases 0/1/3 run warm in the cold shape (documented in the README caveat).

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/weaviate/fgprof"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// fgprofProfileDuration is the wall window each profile samples over. At fgprof's 99Hz
// this accrues ~300 samples per profile: enough for a coarse per-phase split (the split
// is reported as coarse in the README, not a precision instrument).
const fgprofProfileDuration = 3 * time.Second

// fgprofInjectedReadLatency is the per-value-read sleep injected in the simulated-cold
// shape. It stands in for a cold pread (off-CPU device time) so fgprof's wall-clock
// sampler attributes the phase-2 wait that a CPU profile cannot see. Sized so phase 2
// clearly dominates the cold profile while each iteration stays short enough to accrue
// many iterations in the sampling window (B=500 / 16-wide ~= 32 read waves x 250us
// ~= 8ms of phase-2 wall per iteration).
const fgprofInjectedReadLatency = 250 * time.Microsecond

// phaseAttribution maps a stack-frame substring to a phase bucket label. Order is
// significant: the four phase functions are siblings, but a driver-rooted sample stack
// (e.g. GetBySecondaryBatchWithView;getBySecondaryBatchIndexHits;...) contains BOTH the
// driver symbol and one phase symbol, so phase symbols are matched BEFORE driver symbols
// and the first match wins.
type phaseAttribution struct {
	label string
	sym   string
}

// phaseSymbols is checked first, in this order, per sampled stack. phase2 is first because
// its work runs in worker goroutines whose stacks are rooted at the phase-2 closure, not at
// the batch driver.
var phaseSymbols = []phaseAttribution{
	{"phase2_value_reads", "readSecondaryBatchValuesConcurrent"},
	{"phase1_index_descents", "getBySecondaryBatchIndexHits"},
	{"phase3_recheck", "recheckSecondaryBatchInSegments"},
	{"phase3_recheck", "secondaryPresentInMemtables"},
	{"phase0_memtables", "resolveSecondaryFromMemtables"},
}

// phaseOrder fixes the row order in the README table.
var phaseOrder = []string{
	"phase0_memtables",
	"phase1_index_descents",
	"phase2_value_reads",
	"phase3_recheck",
	"batch_driver",
}

// fgprofShape is one profiled configuration.
type fgprofShape struct {
	name      string // "warm" | "cold"
	batchSize int
	driverSym string // symbol identifying the batch driver root frame for this shape
	// work resolves one batch under a fresh view; it is looped for the sample window.
	work func()
}

// fgprofResult holds one shape's per-phase sample attribution.
type fgprofResult struct {
	shape      string
	batchSize  int
	totalBatch int            // samples attributed to any batch phase or the driver
	perPhase   map[string]int // label -> sample count
	pprofPath  string
	foldedPath string
	iterations int
}

func TestBucketGetBySecondaryBatchFgprofProfiles(t *testing.T) {
	outDir := os.Getenv("LSMKV_FGPROF_OUT")
	if outDir == "" {
		t.Skip("LSMKV_FGPROF_OUT unset; set it to a writable dir to capture fgprof wall-clock profiles")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", outDir, err)
	}

	ctx := context.Background()
	bucket, _, docIDs := buildReadOpsBucket(t, benchWarmShape())

	// Prime the page cache: resolve every key once so all value pages are resident before
	// any profiled iteration (warm by construction). The cold shape reuses the same warm
	// bucket and injects latency at the read hook, so "cold" here means injected device
	// latency, not an evicted cache (documented in the README caveat).
	{
		view := bucket.GetConsistentView()
		for _, id := range docIDs {
			if _, _, err := bucket.GetBySecondaryWithBufferAndView(ctx, secondaryPos, encodeDocID(id), nil, view); err != nil {
				view.ReleaseView()
				t.Fatalf("warmup resolve: %v", err)
			}
		}
		view.ReleaseView()
	}

	latencyHook := &secondaryBatchReadHook{onReadStart: func() { time.Sleep(fgprofInjectedReadLatency) }}

	shapes := make([]fgprofShape, 0, 4)
	for _, b := range []int{100, 500} {
		keys := warmBenchKeys(docIDs, b, int64(b))

		warmKeys := keys
		shapes = append(shapes, fgprofShape{
			name:      "warm",
			batchSize: b,
			driverSym: "GetBySecondaryBatchWithView",
			work: func() {
				v := bucket.GetConsistentView()
				_, err := bucket.GetBySecondaryBatchWithView(ctx, secondaryPos, warmKeys, v)
				v.ReleaseView()
				if err != nil {
					t.Fatalf("warm batch resolve: %v", err)
				}
			},
		})

		coldKeys := keys
		shapes = append(shapes, fgprofShape{
			name:      "cold",
			batchSize: b,
			driverSym: "replaySecondaryBatchWithHook",
			work: func() {
				v := bucket.GetConsistentView()
				_, err := replaySecondaryBatchWithHook(ctx, bucket, secondaryPos, coldKeys, v, latencyHook)
				v.ReleaseView()
				if err != nil {
					t.Fatalf("cold batch replay: %v", err)
				}
			},
		})
	}

	results := make([]fgprofResult, 0, len(shapes))
	for _, sh := range shapes {
		res := profileShape(t, outDir, sh)
		results = append(results, res)
		t.Logf("fgprof %s B=%d: %d iterations, %d batch-attributed samples -> %s , %s",
			res.shape, res.batchSize, res.iterations, res.totalBatch, res.pprofPath, res.foldedPath)
	}

	readmePath := filepath.Join(outDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(renderReadme(results)), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	t.Logf("fgprof README written to %s", readmePath)

	// Sanity: every profile must have attributed at least one sample to some batch phase,
	// otherwise the run captured nothing (window too short, symbol drift, or a skipped path).
	for _, res := range results {
		if res.totalBatch == 0 {
			t.Fatalf("profile %s B=%d attributed zero batch samples; folded=%s", res.shape, res.batchSize, res.foldedPath)
		}
	}
	// The cold shape must attribute the plurality of its samples to phase 2 (the injected
	// off-CPU read latency), proving fgprof captured off-CPU wall time a CPU profile misses.
	for _, res := range results {
		if res.shape != "cold" {
			continue
		}
		if res.perPhase["phase2_value_reads"]*2 < res.totalBatch {
			t.Fatalf("cold profile B=%d: phase2 got %d/%d batch samples; expected phase-2-dominant under injected read latency",
				res.batchSize, res.perPhase["phase2_value_reads"], res.totalBatch)
		}
	}
}

// profileShape profiles one shape twice (folded + pprof) over identical loops, writes both
// artifacts, and returns the per-phase attribution parsed from the folded run.
func profileShape(t *testing.T, outDir string, sh fgprofShape) fgprofResult {
	t.Helper()
	base := fmt.Sprintf("%s-B%d", sh.name, sh.batchSize)
	foldedPath := filepath.Join(outDir, base+".folded")
	pprofPath := filepath.Join(outDir, base+".pprof")

	// Folded run: capture into a buffer so we can both persist it and parse it for the table.
	var foldedBuf bytes.Buffer
	iters := profileLoop(t, &foldedBuf, fgprof.FormatFolded, sh.work)
	if err := os.WriteFile(foldedPath, foldedBuf.Bytes(), 0o644); err != nil {
		t.Fatalf("write folded %s: %v", foldedPath, err)
	}

	// pprof run: identical loop, separate profiling session (fgprof.Start binds one format
	// per session), written straight to the .pprof file.
	pf, err := os.Create(pprofPath)
	if err != nil {
		t.Fatalf("create pprof %s: %v", pprofPath, err)
	}
	profileLoop(t, pf, fgprof.FormatPprof, sh.work)
	if err := pf.Close(); err != nil {
		t.Fatalf("close pprof %s: %v", pprofPath, err)
	}

	perPhase, total := attributeFolded(foldedBuf.Bytes(), sh.driverSym)
	return fgprofResult{
		shape:      sh.name,
		batchSize:  sh.batchSize,
		totalBatch: total,
		perPhase:   perPhase,
		pprofPath:  pprofPath,
		foldedPath: foldedPath,
		iterations: iters,
	}
}

// profileLoop starts fgprof, runs work in a tight loop for fgprofProfileDuration, stops the
// profiler (which writes to w in the given format), and returns the iteration count.
func profileLoop(t *testing.T, w interface{ Write([]byte) (int, error) }, format fgprof.Format, work func()) int {
	t.Helper()
	stop := fgprof.Start(w, format, nil)
	deadline := time.Now().Add(fgprofProfileDuration)
	iters := 0
	for time.Now().Before(deadline) {
		work()
		iters++
	}
	if err := stop(); err != nil {
		t.Fatalf("fgprof stop (%s): %v", format, err)
	}
	return iters
}

// attributeFolded parses fgprof folded output ("frame0;frame1;...;frameN count" per line)
// and buckets each sample by the first matching phase symbol, falling back to the batch
// driver symbol. Samples touching none of these are not batch-related and are excluded from
// the returned total (the per-phase % denominator).
func attributeFolded(folded []byte, driverSym string) (map[string]int, int) {
	perPhase := map[string]int{}
	total := 0
	for _, line := range strings.Split(string(folded), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		stack := line[:sp]
		count, err := strconv.Atoi(line[sp+1:])
		if err != nil || count <= 0 {
			continue
		}

		label := ""
		for _, pa := range phaseSymbols {
			if strings.Contains(stack, pa.sym) {
				label = pa.label
				break
			}
		}
		if label == "" && strings.Contains(stack, driverSym) {
			label = "batch_driver"
		}
		if label == "" {
			continue // not part of the batch call tree
		}
		perPhase[label] += count
		total += count
	}
	return perPhase, total
}

// replaySecondaryBatchWithHook is a faithful in-package replay of
// (*Bucket).GetBySecondaryBatchWithView's exact phase order, calling the SAME production
// phase functions, with a caller-supplied read hook injected into phase 2. It exists so the
// simulated-cold shape can inject per-read latency at the phase-2 value reads (the hook's
// only scope) without any production diff. It mirrors the n>=2 batch branch; single-key
// delegation is not exercised here (batch sizes are 100/500). Kept in lockstep with
// GetBySecondaryBatchWithView: if that phase order changes, this must change with it.
func replaySecondaryBatchWithHook(ctx context.Context, b *Bucket, pos int, keys [][]byte,
	view BucketConsistentView, hook *secondaryBatchReadHook,
) ([][]byte, error) {
	n := len(keys)
	out := make([][]byte, n)
	memtables, count := viewMemtables(view)
	segments := view.Disk

	// phase 0 - memtable pass in key-sorted order
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, c int) bool {
		return bytes.Compare(keys[order[a]], keys[order[c]]) < 0
	})
	unresolved := make([]secondaryBatchKey, 0, n)
	for _, oi := range order {
		v, resolved, err := b.resolveSecondaryFromMemtables(pos, keys[oi], memtables, count, nil)
		if resolved {
			if err == nil {
				out[oi] = bytes.Clone(v)
			} else if !entlsmkv.IsDeletedOrNotFound(err) {
				return nil, err
			}
			continue
		}
		unresolved = append(unresolved, secondaryBatchKey{origIdx: oi, key: keys[oi]})
	}
	if len(unresolved) == 0 {
		return out, nil
	}

	// phase 1 - index descents
	hits, err := b.disk.getBySecondaryBatchIndexHits(ctx, pos, unresolved, segments)
	if err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return out, nil
	}

	// phase 2 - concurrent value reads WITH the injected hook
	lives, _, err := b.disk.readSecondaryBatchValuesConcurrent(
		ctx, hits, segments, b.secondaryBatchReadConcurrencyValue(), hook)
	if err != nil {
		return nil, err
	}
	if len(lives) == 0 {
		return out, nil
	}

	// phase 3 - recheck: memtable half then segment half
	kept := lives[:0]
	for _, lh := range lives {
		present, err := b.secondaryPresentInMemtables(lh.priKey, memtables, count)
		if err != nil {
			return nil, err
		}
		if !present {
			kept = append(kept, lh)
		}
	}
	lives = kept
	if err := b.disk.recheckSecondaryBatchInSegments(ctx, lives, segments); err != nil {
		return nil, err
	}
	for i := range lives {
		if !lives[i].superseded {
			out[lives[i].origIdx] = lives[i].value
		}
	}
	return out, nil
}

// renderReadme builds the committed README.md: the per-phase wall-time attribution table
// plus methodology and the cold-shape hook-scope caveat.
func renderReadme(results []fgprofResult) string {
	var sb strings.Builder
	sb.WriteString("# GetBySecondaryBatch fgprof wall-clock profiles\n\n")
	sb.WriteString("Wall-clock (on-CPU + off-CPU) per-phase attribution for the batched secondary\n")
	sb.WriteString("resolver (gh#309), captured with `github.com/weaviate/fgprof`. fgprof samples\n")
	sb.WriteString("every goroutine at 99Hz, so it sees the off-CPU value-read (pread) wait that a\n")
	sb.WriteString("plain `-cpuprofile` misses. Each row's percentage is over the batch-attributed\n")
	sb.WriteString("samples only (samples whose stack touches a batch phase function or the batch\n")
	sb.WriteString("driver); unrelated process goroutines are excluded.\n\n")

	sb.WriteString("Artifacts per (shape, B): `<shape>-B<size>.pprof` (open with `go tool pprof`)\n")
	sb.WriteString("and `<shape>-B<size>.folded` (open with FlameGraph / speedscope). The `.pprof`\n")
	sb.WriteString("and `.folded` files come from two separate but identical profiling runs of the\n")
	sb.WriteString("same shape (fgprof binds one output format per session).\n\n")

	for _, res := range results {
		fmt.Fprintf(&sb, "## %s, B=%d\n\n", res.shape, res.batchSize)
		fmt.Fprintf(&sb, "Iterations sampled: %d. Batch-attributed samples: %d.\n\n", res.iterations, res.totalBatch)
		sb.WriteString("| Phase | Samples | % of batch-attributed |\n")
		sb.WriteString("|---|---:|---:|\n")
		for _, label := range phaseOrder {
			cnt := res.perPhase[label]
			pct := 0.0
			if res.totalBatch > 0 {
				pct = 100 * float64(cnt) / float64(res.totalBatch)
			}
			fmt.Fprintf(&sb, "| %s | %d | %.1f%% |\n", label, cnt, pct)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("## Methodology\n\n")
	fmt.Fprintf(&sb, "- Fixture: the warm bench shape (`benchWarmShape`, %d-segment LTK hot-shard shape), "+
		"page-cache primed before sampling.\n", scaledShape().segments)
	fmt.Fprintf(&sb, "- Sample window: %s per profile at fgprof's 99Hz (~300 samples).\n", fgprofProfileDuration)
	sb.WriteString("- warm: profiles a timed loop over the real `GetBySecondaryBatchWithView` (nil hook).\n")
	fmt.Fprintf(&sb, "- cold: profiles a timed loop over `replaySecondaryBatchWithHook`, a faithful "+
		"in-package replay of the same phase order calling the SAME production phase functions, with a "+
		"`secondaryBatchReadHook` sleeping %s inside each phase-2 read goroutine.\n", fgprofInjectedReadLatency)
	sb.WriteString("- Phases are attributed by the phase-function symbol on each sampled stack; the four " +
		"phase functions are call-tree siblings, so each batch sample maps to exactly one phase (the " +
		"`batch_driver` row is batch time outside any single phase function).\n\n")

	sb.WriteString("## Caveat: cold-shape hook scope\n\n")
	sb.WriteString("The injected latency hook fires only at phase-2 value reads (its only seam), so in the\n")
	sb.WriteString("cold shape phases 0/1/3 still run warm. The cold profile therefore models the shape\n")
	sb.WriteString("where the device value-read cost dominates (the real cold-load concern); it does NOT\n")
	sb.WriteString("model cold index-page or cold recheck-descent latency. The per-phase split is coarse\n")
	sb.WriteString("(~300 samples): read it as \"which phase owns the wall time\", not as a precise ratio.\n")
	return sb.String()
}
