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
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func copyDir(src string, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := fmt.Sprintf("%s/%s", src, entry.Name())
		dstPath := fmt.Sprintf("%s/%s", dst, entry.Name())
		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile2(srcPath, dstPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFile2(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = srcFile.WriteTo(dstFile)
	return err
}

var allStrategiesTest = []string{
	StrategyReplace,
	StrategySetCollection,
	StrategyMapCollection,
	StrategyRoaringSet,
	StrategyRoaringSetRange,
	StrategyInverted,
}

func findStrategyFromPath(path string) (string, error) {
	// find a .db file in the folder and extract the strategy from its sixth byte from start
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".db") && !strings.HasSuffix(entry.Name(), ".db.tmp") {
			segPath := filepath.Join(path, entry.Name())
			f, err := os.Open(segPath)
			if err != nil {
				return "", err
			}

			buf := make([]byte, 8)
			_, err = f.Read(buf)
			f.Close()
			if err != nil {
				return "", err
			}
			// parse int from sixth and seventh byte
			strategyInt := int(buf[5])<<8 + int(buf[6])
			return allStrategiesTest[strategyInt], nil
		}
	}
	return "", os.ErrNotExist
}

func compactBucket(t *testing.T, b *Bucket, tempDir string, done chan struct{}) {
	startTime := time.Now()
	compactionCount := 0
	for {
		res, err := b.disk.compactOnce()
		if err != nil {
			t.Fatalf("failed to compact disk: %v", err)
			return
		}

		if !res {
			break
		}
		compactionCount++
		// Log memory after each compaction round
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("After compaction %d - HeapAlloc: %s, HeapInuse: %s, HeapSys: %s",
			compactionCount, formatBytes(m.HeapAlloc), formatBytes(m.HeapInuse), formatBytes(m.HeapSys))
	}

	elapsed := time.Since(startTime)
	close(done)

	// Compute SHA256 of the compacted segment file
	entries, _ := os.ReadDir(tempDir)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".db") && !strings.HasSuffix(entry.Name(), ".db.tmp") {
			segPath := filepath.Join(tempDir, entry.Name())
			sf, err := os.Open(segPath)
			if err != nil {
				t.Fatalf("open segment for hash: %v", err)
			}
			h := sha256.New()
			io.Copy(h, sf)
			sf.Close()
			t.Logf("Segment %s SHA256: %x", entry.Name(), h.Sum(nil))
		}
	}
	t.Logf("=== Compaction Summary ===")
	t.Logf("Compaction rounds: %d", compactionCount)
	t.Logf("Total time: %s", elapsed)
	t.Logf("")
}

// TestCompactSpecificIdBuckets discovers bucket directories and spawns a
// separate go test process for each one via TestCompactSingleBucket.
// Each subprocess gets a fresh Go runtime so allocation stats (TotalAlloc,
// pprof allocs, HeapSys) are fully isolated per bucket.
func TestCompactSpecificIdBuckets(t *testing.T) {
	topLevelSegmentPaths := []string{
		//"/Users/amourao/code/weaviate/weaviate/data-weaviate-0-vector/msmarco_test/default/lsm",
		"/Users/amourao/Downloads/weaviate/data/vector/sRcIYy7dKGNw/lsm/",
	}
	// list all subdirectories of the above path and add them to the list of paths to test
	entries, err := os.ReadDir(topLevelSegmentPaths[0])
	if err != nil {
		t.Fatalf("failed to read top level segment directory: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			topLevelSegmentPaths = append(topLevelSegmentPaths, filepath.Join(topLevelSegmentPaths[0], entry.Name()))
		}
	}

	topLevelSegmentPaths = topLevelSegmentPaths[1:] // skip the first one since it's the parent directory of the others

	// Set to a strategy name (e.g. "setcollection", "replace", "mapcollection") to
	// only compact buckets of that strategy, or leave empty to compact all.
	targetStrategy := ""

	t.Logf("Found %d segment directories to test compaction on", len(topLevelSegmentPaths))
	for _, topLevelSegmentPath := range topLevelSegmentPaths {
		strategy, err := findStrategyFromPath(topLevelSegmentPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			t.Fatalf("failed to determine strategy from path %s: %v", topLevelSegmentPath, err)
		}
		if targetStrategy != "" && strategy != targetStrategy {
			continue
		}

		bucketName := filepath.Base(topLevelSegmentPath)
		t.Run(fmt.Sprintf("%s_%s", strategy, bucketName), func(t *testing.T) {
			t.Logf("Spawning subprocess for %s (%s)", topLevelSegmentPath, strategy)

			cmd := exec.Command("go", "test", "-v", "-run", "^TestCompactSingleBucket$", "-count=1", "-timeout=10m", "./adapters/repos/db/lsmkv/")
			cmd.Dir = "/Users/amourao/code/weaviate/weaviate"
			cmd.Env = append(os.Environ(), "COMPACT_TEST_BUCKET_PATH="+topLevelSegmentPath)
			output, err := cmd.CombinedOutput()
			t.Logf("\n%s", string(output))
			if err != nil {
				t.Fatalf("subprocess failed: %v", err)
			}
		})
	}
}

// TestCompactSingleBucket compacts a single bucket directory passed via
// the COMPACT_TEST_BUCKET_PATH env var. It runs in its own go test process
// so all allocation stats are fully isolated.
func TestCompactSingleBucket(t *testing.T) {
	bucketPath := os.Getenv("COMPACT_TEST_BUCKET_PATH")
	if bucketPath == "" {
		t.Skip("COMPACT_TEST_BUCKET_PATH not set; run via TestCompactSpecificIdBuckets")
	}

	tempDir := os.TempDir() + "lsmkv_compact_test/" + filepath.Base(bucketPath)
	err := copyDir(bucketPath, tempDir)
	if err != nil {
		t.Fatalf("failed to copy data to temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	profileDir := "/tmp/compact_profiles"
	os.MkdirAll(profileDir, 0o755)
	profileSuffix := filepath.Base(bucketPath)

	// --- Enable mutex and block profiling ---
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	defer func() {
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	}()

	// --- CPU profile ---
	cpuPath := fmt.Sprintf("%s/cpu_%s.prof", profileDir, profileSuffix)
	cpuFile, err := os.Create(cpuPath)
	if err != nil {
		t.Fatalf("could not create CPU profile: %v", err)
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Logf("WARNING: could not start CPU profile: %v", err)
	}

	// --- Memory stats tracking goroutine ---
	var peakHeapAlloc, peakHeapInuse, peakHeapSys, peakTotalAlloc uint64
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		var m runtime.MemStats
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peakHeapAlloc {
					peakHeapAlloc = m.HeapAlloc
				}
				if m.HeapInuse > peakHeapInuse {
					peakHeapInuse = m.HeapInuse
				}
				if m.HeapSys > peakHeapSys {
					peakHeapSys = m.HeapSys
				}
				if m.TotalAlloc > peakTotalAlloc {
					peakTotalAlloc = m.TotalAlloc
				}
			}
		}
	}()

	// --- Record baseline memory ---
	var baselineStats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&baselineStats)
	t.Logf("Baseline - HeapAlloc: %s, HeapInuse: %s, HeapSys: %s",
		formatBytes(baselineStats.HeapAlloc), formatBytes(baselineStats.HeapInuse), formatBytes(baselineStats.HeapSys))

	strategy, err := findStrategyFromPath(tempDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Skip("no .db files found in bucket directory")
		}
		t.Fatalf("failed to determine strategy from path: %v", err)
	}
	// size on disk
	var totalSize int64
	entries, _ := os.ReadDir(tempDir)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".db") && !strings.HasSuffix(entry.Name(), ".db.tmp") {
			info, err := entry.Info()
			if err != nil {
				t.Logf("WARNING: could not get file info for %s: %v", entry.Name(), err)
				continue
			}
			totalSize += info.Size()
		}
	}

	b, err := NewBucketCreator().NewBucket(t.Context(), tempDir, tempDir, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(strategy), WithForceCompaction(true))
	if err != nil {
		t.Fatalf("failed to find searchable segments: %v", err)
		return
	}

	// --- Run compaction ---
	compactBucket(t, b, tempDir, done)

	// --- Final memory stats ---
	var finalStats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&finalStats)

	t.Logf("=== Bucket information ===")
	t.Logf("Bucket path: %s", bucketPath)
	t.Logf("Strategy: %s", strategy)
	t.Logf("Total size on disk after compaction: %s", formatBytes(uint64(totalSize)))
	t.Logf("")
	t.Logf("=== Peak Memory (sampled every 50ms) ===")
	t.Logf("Peak HeapAlloc:   %s", formatBytes(peakHeapAlloc))
	t.Logf("Peak HeapInuse:   %s", formatBytes(peakHeapInuse))
	t.Logf("Peak HeapSys:     %s", formatBytes(peakHeapSys))
	t.Logf("Total Allocated:  %s", formatBytes(peakTotalAlloc))
	t.Logf("")
	t.Logf("=== Final Memory (after GC) ===")
	t.Logf("HeapAlloc:   %s", formatBytes(finalStats.HeapAlloc))
	t.Logf("HeapInuse:   %s", formatBytes(finalStats.HeapInuse))
	t.Logf("HeapSys:     %s", formatBytes(finalStats.HeapSys))
	t.Logf("HeapObjects: %d", finalStats.HeapObjects)
	t.Logf("NumGC:       %d", finalStats.NumGC)
	t.Logf("")
	t.Logf("=== Memory Delta (final - baseline) ===")
	t.Logf("HeapAlloc delta:  %s", formatBytesSigned(int64(finalStats.HeapAlloc)-int64(baselineStats.HeapAlloc)))
	t.Logf("HeapInuse delta:  %s", formatBytesSigned(int64(finalStats.HeapInuse)-int64(baselineStats.HeapInuse)))

	// --- Stop CPU profile (must happen before parsing the file) ---
	pprof.StopCPUProfile()
	cpuFile.Close()

	// --- Write heap profile ---
	heapFile, err := os.Create(fmt.Sprintf("%s/heap_%s.prof", profileDir, profileSuffix))
	if err != nil {
		t.Fatalf("could not create heap profile: %v", err)
	}
	defer heapFile.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(heapFile); err != nil {
		t.Fatalf("could not write heap profile: %v", err)
	}

	// --- Write allocs profile ---
	allocsPath := fmt.Sprintf("%s/allocs_%s.prof", profileDir, profileSuffix)
	writePprofProfile(t, "allocs", allocsPath)

	// --- Write mutex profile ---
	mutexPath := fmt.Sprintf("%s/mutex_%s.prof", profileDir, profileSuffix)
	writePprofProfile(t, "mutex", mutexPath)

	// --- Write block profile ---
	blockPath := fmt.Sprintf("%s/block_%s.prof", profileDir, profileSuffix)
	writePprofProfile(t, "block", blockPath)

	// --- Write goroutine profile ---
	goroutineFile, err := os.Create(fmt.Sprintf("%s/goroutine_%s.prof", profileDir, profileSuffix))
	if err != nil {
		t.Fatalf("could not create goroutine profile: %v", err)
	}
	defer goroutineFile.Close()
	goroutineProfile := pprof.Lookup("goroutine")
	if err := goroutineProfile.WriteTo(goroutineFile, 1); err != nil {
		t.Fatalf("could not write goroutine profile: %v", err)
	}

	// --- Print top hotspots inline ---
	printTopHotspots(t, allocsPath, "Allocation", "alloc_space", "alloc_objects", formatBytes, 15)
	printTopHotspots(t, cpuPath, "CPU", "cpu", "samples", formatNanoseconds, 15)
	printTopHotspots(t, mutexPath, "Mutex Contention", "delay", "contentions", formatNanoseconds, 15)
	printTopHotspots(t, blockPath, "Block/IO", "delay", "contentions", formatNanoseconds, 15)

	t.Logf("")
	t.Logf("=== Profile files written to: %s ===", profileDir)
	for _, name := range []string{"cpu", "heap", "allocs", "mutex", "block", "goroutine"} {
		t.Logf("  %s_%s.prof - go tool pprof %s/%s_%s.prof", name, profileSuffix, profileDir, name, profileSuffix)
	}
}

func formatBytes(b uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func formatBytesSigned(b int64) string {
	sign := ""
	if b < 0 {
		sign = "-"
		b = -b
	} else {
		sign = "+"
	}
	return sign + formatBytes(uint64(b))
}

func formatNanoseconds(v uint64) string {
	switch {
	case v >= 1_000_000_000:
		return fmt.Sprintf("%.2fs", float64(v)/1e9)
	case v >= 1_000_000:
		return fmt.Sprintf("%.2fms", float64(v)/1e6)
	case v >= 1_000:
		return fmt.Sprintf("%.2fus", float64(v)/1e3)
	default:
		return fmt.Sprintf("%dns", v)
	}
}

func writePprofProfile(t *testing.T, name, path string) {
	t.Helper()
	p := pprof.Lookup(name)
	if p == nil {
		t.Logf("WARNING: pprof profile %q not found", name)
		return
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("could not create %s profile: %v", name, err)
	}
	defer f.Close()
	if err := p.WriteTo(f, 0); err != nil {
		t.Fatalf("could not write %s profile: %v", name, err)
	}
}

type hotspot struct {
	name      string
	primary   int64
	secondary int64
}

// printTopHotspots is a generic profile hotspot printer.
// primaryType/secondaryType are pprof sample type names (e.g. "alloc_space"/"alloc_objects",
// "cpu"/"samples", "delay"/"contentions").
// fmtPrimary formats the primary value for display.
func printTopHotspots(t *testing.T, profilePath, label, primaryType, secondaryType string, fmtPrimary func(uint64) string, topN int) {
	t.Helper()

	f, err := os.Open(profilePath)
	if err != nil {
		t.Logf("WARNING: could not open %s profile: %v", label, err)
		return
	}
	defer f.Close()

	p, err := profile.Parse(f)
	if err != nil {
		t.Logf("WARNING: could not parse %s profile: %v", label, err)
		return
	}

	primaryIdx, secondaryIdx := -1, -1
	for i, st := range p.SampleType {
		if st.Type == primaryType {
			primaryIdx = i
		}
		if st.Type == secondaryType {
			secondaryIdx = i
		}
	}
	if primaryIdx < 0 {
		t.Logf("WARNING: sample type %q not found in %s profile", primaryType, label)
		return
	}

	byFunc := map[string]*hotspot{}
	for _, s := range p.Sample {
		if len(s.Location) == 0 {
			continue
		}
		var key string
		for _, loc := range s.Location {
			for _, line := range loc.Line {
				fn := line.Function
				if fn == nil {
					continue
				}
				if strings.HasPrefix(fn.Name, "runtime.") {
					continue
				}
				key = fmt.Sprintf("%s (%s:%d)", fn.Name, filepath.Base(fn.Filename), line.Line)
				break
			}
			if key != "" {
				break
			}
		}
		if key == "" {
			continue
		}
		site, ok := byFunc[key]
		if !ok {
			site = &hotspot{name: key}
			byFunc[key] = site
		}
		site.primary += s.Value[primaryIdx]
		if secondaryIdx >= 0 {
			site.secondary += s.Value[secondaryIdx]
		}
	}

	sites := make([]hotspot, 0, len(byFunc))
	for _, s := range byFunc {
		if s.primary > 0 {
			sites = append(sites, *s)
		}
	}
	sort.Slice(sites, func(i, j int) bool {
		return sites[i].primary > sites[j].primary
	})

	if topN > len(sites) {
		topN = len(sites)
	}
	if topN == 0 {
		return
	}

	t.Logf("")
	t.Logf("=== Top %d %s Hotspots (%s) ===", topN, label, primaryType)
	t.Logf("%-4s  %14s  %12s  %s", "Rank", primaryType, secondaryType, "Function")
	t.Logf("%-4s  %14s  %12s  %s", "----", "----------", "-----------", "--------")
	for i := 0; i < topN; i++ {
		s := sites[i]
		t.Logf("%-4d  %14s  %12d  %s", i+1, fmtPrimary(uint64(s.primary)), s.secondary, s.name)
	}
}
