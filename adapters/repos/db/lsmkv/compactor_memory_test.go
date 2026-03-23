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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"math"
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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
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

func hashSegments(t *testing.T, dir string, label string) {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".db") && !strings.HasSuffix(entry.Name(), ".db.tmp") {
			segPath := filepath.Join(dir, entry.Name())
			sf, err := os.Open(segPath)
			if err != nil {
				t.Fatalf("open segment for hash: %v", err)
			}
			h := sha256.New()
			io.Copy(h, sf)
			sf.Close()
			info, _ := entry.Info()
			size := int64(0)
			if info != nil {
				size = info.Size()
			}
			t.Logf("[%s] %s  SHA256: %x  Size: %s", label, entry.Name(), h.Sum(nil), formatBytes(uint64(size)))
		}
	}
}

// dumpSegmentTrees reads each .db segment in dir, extracts the index tree,
// walks all nodes collecting (key, start, end), sorts by key, and writes
// a text dump to outDir/label/segmentName.txt. It also logs a SHA256 of
// the sorted dump for quick comparison.
func dumpSegmentTrees(t *testing.T, dir, outDir, label string) {
	t.Helper()
	dumpDir := filepath.Join(outDir, label)
	os.MkdirAll(dumpDir, 0o755)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir for tree dump: %v", err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".db") || strings.HasSuffix(entry.Name(), ".db.tmp") {
			continue
		}
		segPath := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(segPath)
		if err != nil {
			t.Fatalf("read segment %s: %v", entry.Name(), err)
		}
		if len(data) < segmentindex.HeaderSize {
			continue
		}

		header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
		if err != nil {
			t.Logf("[%s] %s: skip (header parse: %v)", label, entry.Name(), err)
			continue
		}

		primaryIdx, err := header.PrimaryIndex(data)
		if err != nil {
			t.Logf("[%s] %s: skip (primary index: %v)", label, entry.Name(), err)
			continue
		}

		tree := segmentindex.NewDiskTree(primaryIdx)

		type nodeInfo struct {
			key   []byte
			start uint64
			end   uint64
		}

		// Walk all nodes in BFS order (level-order) by parsing serialized data
		var nodes []nodeInfo
		bufferPos := 0
		for bufferPos+36 <= len(primaryIdx) {
			keyLen := int(binary.LittleEndian.Uint32(primaryIdx[bufferPos:]))
			nodeSize := keyLen + 36
			if bufferPos+nodeSize > len(primaryIdx) {
				break
			}
			k := make([]byte, keyLen)
			copy(k, primaryIdx[bufferPos+4:bufferPos+4+keyLen])
			start := binary.LittleEndian.Uint64(primaryIdx[bufferPos+4+keyLen:])
			end := binary.LittleEndian.Uint64(primaryIdx[bufferPos+4+keyLen+8:])
			nodes = append(nodes, nodeInfo{key: k, start: start, end: end})
			bufferPos += nodeSize
		}

		// Sort by key for order-independent comparison
		sort.Slice(nodes, func(i, j int) bool {
			return string(nodes[i].key) < string(nodes[j].key)
		})

		// Write sorted dump and compute hash
		var sb strings.Builder
		for _, n := range nodes {
			fmt.Fprintf(&sb, "%x %d %d\n", n.key, n.start, n.end)
		}
		dump := sb.String()

		dumpPath := filepath.Join(dumpDir, entry.Name()+".tree.txt")
		os.WriteFile(dumpPath, []byte(dump), 0o644)

		h := sha256.Sum256([]byte(dump))
		t.Logf("[%s] %s  tree: %d nodes  SHA256: %x", label, entry.Name(), len(nodes), h)

		_ = tree // ensure import is used
	}
}

// segmentSection identifies a named byte range in a segment file.
type segmentSection struct {
	name  string
	start uint64
	end   uint64
	hash  [32]byte
}

// segmentParts holds parsed parts of a segment file for comparison.
type segmentParts struct {
	name     string
	data     []byte // full file content
	header   segmentindex.Header
	sections []segmentSection
	treeData []byte // raw tree bytes (IndexStart to EOF)
}

// parseSegmentParts reads a segment file and splits it into named sections
// based on the segment header (and inverted header if applicable), computing
// SHA256 checksums for each section.
func parseSegmentParts(t *testing.T, path string) segmentParts {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment %s: %v", path, err)
	}
	if len(data) < segmentindex.HeaderSize {
		t.Fatalf("segment %s too small (%d bytes)", path, len(data))
	}

	header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
	if err != nil {
		t.Fatalf("parse header %s: %v", path, err)
	}

	if header.IndexStart > uint64(len(data)) {
		t.Fatalf("segment %s: IndexStart %d exceeds file size %d", path, header.IndexStart, len(data))
	}

	sp := segmentParts{
		name:     filepath.Base(path),
		data:     data,
		header:   *header,
		treeData: data[header.IndexStart:],
	}

	// For inverted strategy, break into sub-sections using the inverted header
	if header.Strategy == segmentindex.StrategyInverted && len(data) >= segmentindex.HeaderSize+segmentindex.HeaderInvertedSize {
		invHdr, err := segmentindex.LoadHeaderInverted(data[segmentindex.HeaderSize : segmentindex.HeaderSize+segmentindex.HeaderInvertedSize])
		if err == nil {
			headerEnd := uint64(segmentindex.HeaderSize + segmentindex.SegmentInvertedDefaultHeaderSize + len(invHdr.DataFields))
			sp.sections = []segmentSection{
				{name: "headers", start: 0, end: headerEnd},
				{name: "key-data", start: headerEnd, end: invHdr.TombstoneOffset},
				{name: "tombstones", start: invHdr.TombstoneOffset, end: invHdr.PropertyLengthsOffset},
				{name: "prop-lengths", start: invHdr.PropertyLengthsOffset, end: header.IndexStart},
				{name: "tree", start: header.IndexStart, end: uint64(len(data))},
			}
		}
	}

	// Fallback: just data + tree
	if len(sp.sections) == 0 {
		sp.sections = []segmentSection{
			{name: "data", start: 0, end: header.IndexStart},
			{name: "tree", start: header.IndexStart, end: uint64(len(data))},
		}
	}

	// Compute hashes
	for i := range sp.sections {
		s := &sp.sections[i]
		if s.end > uint64(len(data)) {
			s.end = uint64(len(data))
		}
		if s.start < s.end {
			s.hash = sha256.Sum256(data[s.start:s.end])
		}
	}

	return sp
}

// treeEntry is one node from the serialized balanced BST index.
type treeEntry struct {
	key        string
	start      uint64
	end        uint64
	leftChild  int64
	rightChild int64
}

// parseTreeEntries walks the serialized tree sequentially (node order = file
// order) and returns all entries.
func parseTreeEntries(data []byte) []treeEntry {
	var entries []treeEntry
	pos := 0
	for pos+36 <= len(data) {
		keyLen := int(binary.LittleEndian.Uint32(data[pos:]))
		nodeSize := keyLen + 36
		if pos+nodeSize > len(data) {
			break
		}
		entries = append(entries, treeEntry{
			key:        string(data[pos+4 : pos+4+keyLen]),
			start:      binary.LittleEndian.Uint64(data[pos+4+keyLen:]),
			end:        binary.LittleEndian.Uint64(data[pos+4+keyLen+8:]),
			leftChild:  int64(binary.LittleEndian.Uint64(data[pos+4+keyLen+16:])),
			rightChild: int64(binary.LittleEndian.Uint64(data[pos+4+keyLen+24:])),
		})
		pos += nodeSize
	}
	return entries
}

// compareSegmentFiles finds all .db files in both directories, parses their
// segment headers, checksums each section (headers, key-data, tombstones,
// prop-lengths, tree), and if trees differ does a full key+offset diff.
// Returns true if everything matches.
func compareSegmentFiles(t *testing.T, mainDir, baselineDir string) bool {
	t.Helper()

	listDBFiles := func(dir string) []string {
		entries, _ := os.ReadDir(dir)
		var out []string
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".db") && !strings.HasSuffix(e.Name(), ".db.tmp") {
				out = append(out, filepath.Join(dir, e.Name()))
			}
		}
		sort.Strings(out)
		return out
	}

	mainFiles := listDBFiles(mainDir)
	baselineFiles := listDBFiles(baselineDir)

	if len(mainFiles) != len(baselineFiles) {
		t.Errorf("segment count mismatch: main=%d baseline=%d", len(mainFiles), len(baselineFiles))
		return false
	}

	allMatch := true
	for i := range mainFiles {
		mp := parseSegmentParts(t, mainFiles[i])
		bp := parseSegmentParts(t, baselineFiles[i])

		t.Logf("Segment %s (strategy=%d indexStart=%d size=%d):",
			mp.name, mp.header.Strategy, mp.header.IndexStart, len(mp.data))

		for si := range mp.sections {
			ms, bs := mp.sections[si], bp.sections[si]
			match := ms.hash == bs.hash
			size := ms.end - ms.start
			t.Logf("  %-14s [%d:%d] (%s) match=%v", ms.name, ms.start, ms.end, formatBytes(size), match)
			if !match {
				t.Logf("    BYTES DIFFER: main=%x baseline=%x", ms.hash, bs.hash)

				// For prop-lengths, gob serializes maps non-deterministically.
				// Compare logically instead; only fail if the decoded maps differ.
				if ms.name == "prop-lengths" {
					if !comparePropLengths(t, mp.data[ms.start:ms.end], bp.data[bs.start:bs.end]) {
						allMatch = false
					}
					continue
				}

				allMatch = false

				// For tree mismatches, do key+offset analysis
				if ms.name == "tree" {
					compareTreeSections(t, mp, bp)
				}

				// For other mismatches, find first differing byte
				if ms.start < uint64(len(mp.data)) && ms.start < uint64(len(bp.data)) {
					mSlice := mp.data[ms.start:ms.end]
					bSlice := bp.data[bs.start:bs.end]
					if len(mSlice) != len(bSlice) {
						t.Logf("    Size differs: main=%d baseline=%d", len(mSlice), len(bSlice))
					} else {
						diffCount := 0
						firstDiff := -1
						for k := range mSlice {
							if mSlice[k] != bSlice[k] {
								if firstDiff == -1 {
									firstDiff = k
								}
								diffCount++
							}
						}
						t.Logf("    %d differing bytes out of %d (first at offset %d = file offset %d)",
							diffCount, len(mSlice), firstDiff, int(ms.start)+firstDiff)
					}
				}
			}
		}
	}
	return allMatch
}

// compareTreeSections does a full key+offset analysis of two trees.
func compareTreeSections(t *testing.T, mp, bp segmentParts) {
	t.Helper()

	mainEntries := parseTreeEntries(mp.treeData)
	baselineEntries := parseTreeEntries(bp.treeData)

	t.Logf("  Tree node count: main=%d baseline=%d", len(mainEntries), len(baselineEntries))

	if len(mainEntries) != len(baselineEntries) {
		t.Errorf("  TREE NODE COUNT MISMATCH")
		return
	}

	// Sort both by key for order-independent comparison
	mainSorted := make([]treeEntry, len(mainEntries))
	copy(mainSorted, mainEntries)
	sort.Slice(mainSorted, func(i, j int) bool { return mainSorted[i].key < mainSorted[j].key })

	baselineSorted := make([]treeEntry, len(baselineEntries))
	copy(baselineSorted, baselineEntries)
	sort.Slice(baselineSorted, func(i, j int) bool { return baselineSorted[i].key < baselineSorted[j].key })

	diffs := 0
	for j := range mainSorted {
		m, b := mainSorted[j], baselineSorted[j]
		if m.key != b.key || m.start != b.start || m.end != b.end {
			if diffs < 20 {
				t.Logf("  DIFF[%d] key=%x main(start=%d end=%d) baseline(start=%d end=%d)",
					j, []byte(m.key), m.start, m.end, b.start, b.end)
			}
			diffs++
		}
	}
	if diffs > 0 {
		t.Errorf("  TREE KEY+OFFSET DIFFS: %d / %d", diffs, len(mainSorted))
	} else {
		t.Logf("  All %d keys match (start/end offsets identical) — tree structure differs only in child pointers", len(mainSorted))
	}
}

// decodePropLengthsSection decodes the prop-lengths section of an inverted
// segment. Layout: [avg float64 (8)][count uint64 (8)][gobLen uint64 (8)][gob bytes].
func decodePropLengthsSection(data []byte) (float64, uint64, map[uint64]uint32, error) {
	if len(data) < 24 {
		return 0, 0, nil, fmt.Errorf("prop-lengths section too small: %d bytes", len(data))
	}
	avg := math.Float64frombits(binary.LittleEndian.Uint64(data[0:8]))
	count := binary.LittleEndian.Uint64(data[8:16])
	gobLen := binary.LittleEndian.Uint64(data[16:24])

	if uint64(len(data)) < 24+gobLen {
		return 0, 0, nil, fmt.Errorf("prop-lengths gob truncated: need %d, have %d", 24+gobLen, len(data))
	}

	m := make(map[uint64]uint32)
	if gobLen > 0 {
		dec := gob.NewDecoder(bytes.NewReader(data[24 : 24+gobLen]))
		if err := dec.Decode(&m); err != nil {
			return 0, 0, nil, fmt.Errorf("gob decode: %w", err)
		}
	}
	return avg, count, m, nil
}

// comparePropLengths decodes two prop-lengths sections and compares the
// fixed fields (avg, count) and the map contents logically. Returns true
// if they are logically identical.
func comparePropLengths(t *testing.T, mainData, baselineData []byte) bool {
	t.Helper()

	mAvg, mCount, mMap, mErr := decodePropLengthsSection(mainData)
	bAvg, bCount, bMap, bErr := decodePropLengthsSection(baselineData)
	if mErr != nil {
		t.Logf("    main prop-lengths decode error: %v", mErr)
		return false
	}
	if bErr != nil {
		t.Logf("    baseline prop-lengths decode error: %v", bErr)
		return false
	}

	t.Logf("    Fixed fields: avg main=%.6f baseline=%.6f match=%v",
		mAvg, bAvg, mAvg == bAvg)
	t.Logf("    Fixed fields: count main=%d baseline=%d match=%v",
		mCount, bCount, mCount == bCount)
	t.Logf("    Map size: main=%d baseline=%d match=%v",
		len(mMap), len(bMap), len(mMap) == len(bMap))

	// Compare map contents
	missing := 0
	extra := 0
	valueDiffs := 0
	for k, mv := range mMap {
		bv, ok := bMap[k]
		if !ok {
			if missing < 5 {
				t.Logf("    key %d in main but not baseline (value=%d)", k, mv)
			}
			missing++
		} else if mv != bv {
			if valueDiffs < 5 {
				t.Logf("    key %d value differs: main=%d baseline=%d", k, mv, bv)
			}
			valueDiffs++
		}
	}
	for k, bv := range bMap {
		if _, ok := mMap[k]; !ok {
			if extra < 5 {
				t.Logf("    key %d in baseline but not main (value=%d)", k, bv)
			}
			extra++
		}
	}

	if missing == 0 && extra == 0 && valueDiffs == 0 && mAvg == bAvg && mCount == bCount {
		t.Logf("    Property lengths maps are LOGICALLY IDENTICAL (%d entries) — byte difference is gob serialization order only", len(mMap))
		return true
	}
	t.Errorf("    Property lengths maps DIFFER: missing=%d extra=%d valueDiffs=%d", missing, extra, valueDiffs)
	return false
}

// keySnapshot stores the set of live doc IDs per outer key.
type keySnapshot struct {
	key    string
	docIDs []uint64
}

// snapshotBucketKeys opens a bucket at dir with the given strategy, iterates
// all keys via MapCursor, and returns the full snapshot of (key → sorted docIDs).
func snapshotBucketKeys(t *testing.T, dir, strategy string) []keySnapshot {
	t.Helper()

	b, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(strategy), WithForceCompaction(true))
	if err != nil {
		t.Fatalf("snapshotBucketKeys: open bucket: %v", err)
	}
	defer b.Shutdown(context.Background())

	cursor, err := b.MapCursor()
	if err != nil {
		t.Fatalf("snapshotBucketKeys: create cursor: %v", err)
	}
	defer cursor.Close()

	var snap []keySnapshot
	ctx := context.Background()

	for key, values := cursor.First(ctx); key != nil; key, values = cursor.Next(ctx) {
		// Filter out tombstoned values
		var docIDs []uint64
		for _, v := range values {
			if !v.Tombstone {
				docIDs = append(docIDs, binary.BigEndian.Uint64(v.Key))
			}
		}
		if len(docIDs) == 0 {
			continue
		}
		sort.Slice(docIDs, func(i, j int) bool { return docIDs[i] < docIDs[j] })
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		snap = append(snap, keySnapshot{key: string(keyCopy), docIDs: docIDs})
	}

	t.Logf("snapshotBucketKeys: %d keys", len(snap))
	return snap
}

// compareSnapshots compares two key snapshots and logs the first N differences.
func compareSnapshots(t *testing.T, before, after []keySnapshot) bool {
	t.Helper()

	// Build map for after
	afterMap := make(map[string][]uint64, len(after))
	for _, s := range after {
		afterMap[s.key] = s.docIDs
	}
	beforeMap := make(map[string][]uint64, len(before))
	for _, s := range before {
		beforeMap[s.key] = s.docIDs
	}

	diffs := 0
	maxDiffs := 10

	// Check keys in before but different/missing in after
	for _, s := range before {
		afterDocIDs, ok := afterMap[s.key]
		if !ok {
			if diffs < maxDiffs {
				t.Logf("DIFF: key %x present before (%d docs) but missing after", s.key, len(s.docIDs))
			}
			diffs++
			continue
		}
		if len(s.docIDs) != len(afterDocIDs) {
			if diffs < maxDiffs {
				t.Logf("DIFF: key %x doc count changed: %d -> %d", s.key, len(s.docIDs), len(afterDocIDs))
				// Show which doc IDs differ
				beforeSet := make(map[uint64]bool, len(s.docIDs))
				for _, d := range s.docIDs {
					beforeSet[d] = true
				}
				afterSet := make(map[uint64]bool, len(afterDocIDs))
				for _, d := range afterDocIDs {
					afterSet[d] = true
				}
				for d := range beforeSet {
					if !afterSet[d] {
						t.Logf("  lost docID %d", d)
					}
				}
				for d := range afterSet {
					if !beforeSet[d] {
						t.Logf("  gained docID %d", d)
					}
				}
			}
			diffs++
			continue
		}
		for i := range s.docIDs {
			if s.docIDs[i] != afterDocIDs[i] {
				if diffs < maxDiffs {
					t.Logf("DIFF: key %x docID[%d] changed: %d -> %d", s.key, i, s.docIDs[i], afterDocIDs[i])
				}
				diffs++
				break
			}
		}
	}

	// Check keys in after but missing in before
	for _, s := range after {
		if _, ok := beforeMap[s.key]; !ok {
			if diffs < maxDiffs {
				t.Logf("DIFF: key %x present after (%d docs) but missing before", s.key, len(s.docIDs))
			}
			diffs++
		}
	}

	t.Logf("Total differences: %d", diffs)
	return diffs == 0
}

// Set to true to enable tree dumps for order-independent comparison.
// Disabled by default because it reads entire segments into memory and
// dominates the allocation profile (~14 GB overhead).
const enableTreeDumps = false

const enableHashSegments = false

func compactBucket(t *testing.T, b *Bucket, tempDir string, treeDumpDir string, done chan struct{}, profileDir, profileSuffix string) (int, time.Duration) {
	startTime := time.Now()
	compactionCount := 0

	if enableHashSegments {
		hashSegments(t, tempDir, "before")
	}
	if enableTreeDumps {
		dumpSegmentTrees(t, tempDir, treeDumpDir, "before")
	}

	for {
		res, err := b.disk.compactOnce()
		if err != nil {
			t.Fatalf("failed to compact disk: %v", err)
			return 0, 0
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
		label := fmt.Sprintf("round_%d", compactionCount)

		if enableHashSegments {
			hashSegments(t, tempDir, label)
		}
		if enableTreeDumps {
			dumpSegmentTrees(t, tempDir, treeDumpDir, label)
		}

		// Write checkpoint profiles after each round so they're available if the
		// next round causes an OOM kill (SIGKILL cannot be caught by defer).
		if profileDir != "" {
			for _, pName := range []string{"allocs", "heap"} {
				p := pprof.Lookup(pName)
				if p == nil {
					continue
				}
				cpPath := fmt.Sprintf("%s/checkpoint_round%02d_%s_%s.prof", profileDir, compactionCount, profileSuffix, pName)
				if pf, perr := os.Create(cpPath); perr == nil {
					p.WriteTo(pf, 0)
					pf.Close()
				}
			}
			// Flush current memory stats to a JSON file (not just t.Logf which is
			// buffered and lost on OOM kill).
			statsJSON := fmt.Sprintf(`{"round":%d,"heap_alloc":%d,"heap_inuse":%d,"heap_sys":%d}`,
				compactionCount, m.HeapAlloc, m.HeapInuse, m.HeapSys)
			statsPath := fmt.Sprintf("%s/checkpoint_round%02d_%s_memstats.json", profileDir, compactionCount, profileSuffix)
			os.WriteFile(statsPath, []byte(statsJSON), 0o644)
		}
	}

	elapsed := time.Since(startTime)
	close(done)

	t.Logf("=== Compaction Summary ===")
	t.Logf("Compaction rounds: %d", compactionCount)
	t.Logf("Total time: %s", elapsed)
	t.Logf("")

	return compactionCount, elapsed
}

// baselineResult stores compaction results for cross-branch comparison.
type baselineResult struct {
	Strategy         string `json:"strategy"`
	CompactionRounds int    `json:"compaction_rounds"`
	CompactionTimeNs int64  `json:"compaction_time_ns"`
	PeakHeapAlloc    uint64 `json:"peak_heap_alloc"`
	PeakHeapInuse    uint64 `json:"peak_heap_inuse"`
	PeakHeapSys      uint64 `json:"peak_heap_sys"`
	PeakTotalAlloc   uint64 `json:"peak_total_alloc"`
	SegmentCount     int    `json:"segment_count"`
	TotalSegmentSize int64  `json:"total_segment_size"`
}

// saveBaseline copies the compacted segments and writes stats to baselineDir.
func saveBaseline(t *testing.T, compactedDir, baselineDir string, result baselineResult) {
	t.Helper()

	// Clean and create baseline dir
	os.RemoveAll(baselineDir)
	if err := os.MkdirAll(baselineDir, 0o755); err != nil {
		t.Fatalf("create baseline dir: %v", err)
	}

	// Copy all .db files
	entries, err := os.ReadDir(compactedDir)
	if err != nil {
		t.Fatalf("read compacted dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".db") && !strings.HasSuffix(e.Name(), ".db.tmp") {
			src := filepath.Join(compactedDir, e.Name())
			dst := filepath.Join(baselineDir, e.Name())
			if err := copyFile2(src, dst); err != nil {
				t.Fatalf("copy segment to baseline: %v", err)
			}
		}
	}

	// Write stats JSON
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatalf("marshal baseline stats: %v", err)
	}
	if err := os.WriteFile(filepath.Join(baselineDir, "stats.json"), data, 0o644); err != nil {
		t.Fatalf("write baseline stats: %v", err)
	}

	t.Logf("=== Baseline saved to %s ===", baselineDir)
	t.Logf("  Segments: %d, Total size: %s", result.SegmentCount, formatBytes(uint64(result.TotalSegmentSize)))
	t.Logf("  Compaction rounds: %d, Time: %s", result.CompactionRounds, time.Duration(result.CompactionTimeNs))
	t.Logf("  PeakTotalAlloc: %s, PeakHeapAlloc: %s", formatBytes(result.PeakTotalAlloc), formatBytes(result.PeakHeapAlloc))
}

// loadBaseline loads a previously saved baseline from baselineDir.
func loadBaseline(t *testing.T, baselineDir string) baselineResult {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(baselineDir, "stats.json"))
	if err != nil {
		t.Fatalf("read baseline stats: %v", err)
	}
	var result baselineResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal baseline stats: %v", err)
	}
	return result
}

// compareBaselineStats logs a side-by-side comparison of baseline vs current stats.
func compareBaselineStats(t *testing.T, baseline, current baselineResult) {
	t.Helper()

	t.Logf("=== Cross-branch Memory Comparison ===")
	t.Logf("%-22s  %14s  %14s  %14s", "", "Baseline", "Current", "Delta")
	t.Logf("%-22s  %14s  %14s  %14s", "----", "--------", "-------", "-----")

	logRow := func(label string, base, cur uint64) {
		delta := int64(cur) - int64(base)
		pct := float64(0)
		if base > 0 {
			pct = float64(delta) / float64(base) * 100
		}
		t.Logf("%-22s  %14s  %14s  %14s (%+.1f%%)", label,
			formatBytes(base), formatBytes(cur), formatBytesSigned(delta), pct)
	}

	logRow("Peak TotalAlloc", baseline.PeakTotalAlloc, current.PeakTotalAlloc)
	logRow("Peak HeapAlloc", baseline.PeakHeapAlloc, current.PeakHeapAlloc)
	logRow("Peak HeapInuse", baseline.PeakHeapInuse, current.PeakHeapInuse)
	logRow("Peak HeapSys", baseline.PeakHeapSys, current.PeakHeapSys)

	t.Logf("%-22s  %14d  %14d  %14d", "Compaction rounds",
		baseline.CompactionRounds, current.CompactionRounds,
		current.CompactionRounds-baseline.CompactionRounds)
	t.Logf("%-22s  %14s  %14s  %14s", "Compaction time",
		time.Duration(baseline.CompactionTimeNs), time.Duration(current.CompactionTimeNs),
		time.Duration(current.CompactionTimeNs-baseline.CompactionTimeNs))
	t.Logf("%-22s  %14d  %14d  %14d", "Segment count",
		baseline.SegmentCount, current.SegmentCount,
		current.SegmentCount-baseline.SegmentCount)
	logRow("Total segment size", uint64(baseline.TotalSegmentSize), uint64(current.TotalSegmentSize))
}

// countSegments returns the count and total size of .db files in dir.
func countSegments(dir string) (int, int64) {
	entries, _ := os.ReadDir(dir)
	count := 0
	var total int64
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".db") && !strings.HasSuffix(e.Name(), ".db.tmp") {
			count++
			if info, err := e.Info(); err == nil {
				total += info.Size()
			}
		}
	}
	return count, total
}

// TestCompactSpecificIdBuckets discovers bucket directories and spawns a
// separate go test process for each one via TestCompactSingleBucket.
// Each subprocess gets a fresh Go runtime so allocation stats (TotalAlloc,
// pprof allocs, HeapSys) are fully isolated per bucket.
func TestCompactSpecificIdBuckets(t *testing.T) {
	topLevelSegmentPaths := []string{
		"/Users/amourao/code/weaviate/weaviate/data-weaviate-0-vector/msmarco_test/default/lsm/",
		//"/Users/amourao/Downloads/weaviate/data/vector/sRcIYy7dKGNw/lsm/",
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
	targetStrategy := StrategyReplace

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
			cmd.Env = append(os.Environ(), "COMPACT_TEST_BUCKET_PATH="+topLevelSegmentPath, "COPY_TO_TEMP_DIR=true")
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
//
// Environment variables:
//
//	COMPACT_TEST_BUCKET_PATH  - path to the source bucket directory (required)
//	COMPACT_BASELINE_DIR      - persistent directory for cross-branch baseline data
//	COMPACT_SAVE_BASELINE     - if "true", save compaction results as baseline and exit
func TestCompactSingleBucket(t *testing.T) {
	bucketPath := os.Getenv("COMPACT_TEST_BUCKET_PATH")
	if bucketPath == "" {
		t.Skip("COMPACT_TEST_BUCKET_PATH not set; run via TestCompactSpecificIdBuckets")
	}
	copyToTempDir := os.Getenv("COPY_TO_TEMP_DIR") == "true"

	baselineDirEnv := os.Getenv("COMPACT_BASELINE_DIR")
	saveBaselineMode := os.Getenv("COMPACT_SAVE_BASELINE") == "true"

	tempDir := bucketPath
	if copyToTempDir {
		tempDir := os.TempDir() + "lsmkv_compact_test/" + filepath.Base(bucketPath)
		err := copyDir(bucketPath, tempDir)
		if err != nil {
			t.Fatalf("failed to copy data to temp directory: %v", err)
		}
		defer os.RemoveAll(tempDir)
	}

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
	liveStatsPath := fmt.Sprintf("%s/live_stats_%s.json", profileDir, profileSuffix)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		flushTicker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		defer flushTicker.Stop()
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
			case <-flushTicker.C:
				// Write peak stats to a file so they survive an OOM kill.
				statsJSON := fmt.Sprintf(
					`{"peak_heap_alloc":%d,"peak_heap_inuse":%d,"peak_heap_sys":%d,"peak_total_alloc":%d}`,
					peakHeapAlloc, peakHeapInuse, peakHeapSys, peakTotalAlloc,
				)
				os.WriteFile(liveStatsPath, []byte(statsJSON), 0o644)
			}
		}
	}()

	// --- Record baseline memory ---
	var memBaseline runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBaseline)
	t.Logf("Baseline - HeapAlloc: %s, HeapInuse: %s, HeapSys: %s",
		formatBytes(memBaseline.HeapAlloc), formatBytes(memBaseline.HeapInuse), formatBytes(memBaseline.HeapSys))

	strategy, err := findStrategyFromPath(tempDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Skip("no .db files found in bucket directory")
		}
		t.Fatalf("failed to determine strategy from path: %v", err)
	}

	bucketOptions := []BucketOption{WithStrategy(strategy), WithForceCompaction(true)}

	if strategy == StrategyReplace {
		// For replace strategy, disable compaction heuristics to force a full compaction pass for testing.
		bucketOptions = append(bucketOptions, WithSecondaryIndices(1))
		bucketOptions = append(bucketOptions, WithKeepTombstones(true))
	}

	b, err := NewBucketCreator().NewBucket(t.Context(), tempDir, tempDir, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), bucketOptions...)
	if err != nil {
		t.Fatalf("failed to open bucket: %v", err)
	}
	t.Logf("Opened bucket at %s with strategy %s", tempDir, strategy)

	// --- Run compaction ---
	treeDumpDir := filepath.Join("/tmp/tree_dumps", filepath.Base(bucketPath))
	os.MkdirAll(treeDumpDir, 0o755)
	compactionRounds, compactionTime := compactBucket(t, b, tempDir, treeDumpDir, done, profileDir, profileSuffix)
	b.Shutdown(context.Background())

	// --- Stop CPU profile ---
	pprof.StopCPUProfile()
	cpuFile.Close()

	// --- Write profiles ---
	heapFile, err := os.Create(fmt.Sprintf("%s/heap_%s.prof", profileDir, profileSuffix))
	if err != nil {
		t.Fatalf("could not create heap profile: %v", err)
	}
	defer heapFile.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(heapFile); err != nil {
		t.Fatalf("could not write heap profile: %v", err)
	}

	allocsPath := fmt.Sprintf("%s/allocs_%s.prof", profileDir, profileSuffix)
	writePprofProfile(t, "allocs", allocsPath)

	mutexPath := fmt.Sprintf("%s/mutex_%s.prof", profileDir, profileSuffix)
	writePprofProfile(t, "mutex", mutexPath)

	blockPath := fmt.Sprintf("%s/block_%s.prof", profileDir, profileSuffix)
	writePprofProfile(t, "block", blockPath)

	goroutineFile, err := os.Create(fmt.Sprintf("%s/goroutine_%s.prof", profileDir, profileSuffix))
	if err != nil {
		t.Fatalf("could not create goroutine profile: %v", err)
	}
	defer goroutineFile.Close()
	goroutineProfile := pprof.Lookup("goroutine")
	if err := goroutineProfile.WriteTo(goroutineFile, 1); err != nil {
		t.Fatalf("could not write goroutine profile: %v", err)
	}

	// --- Collect current run stats ---
	segCount, segSize := countSegments(tempDir)
	currentResult := baselineResult{
		Strategy:         strategy,
		CompactionRounds: compactionRounds,
		CompactionTimeNs: compactionTime.Nanoseconds(),
		PeakHeapAlloc:    peakHeapAlloc,
		PeakHeapInuse:    peakHeapInuse,
		PeakHeapSys:      peakHeapSys,
		PeakTotalAlloc:   peakTotalAlloc,
		SegmentCount:     segCount,
		TotalSegmentSize: segSize,
	}

	// --- Baseline save/compare ---
	if saveBaselineMode && baselineDirEnv != "" {
		saveBaseline(t, tempDir, baselineDirEnv, currentResult)
	} else if baselineDirEnv != "" {
		// Compare against saved baseline
		savedBaseline := loadBaseline(t, baselineDirEnv)

		t.Logf("=== Comparing segment files against baseline ===")
		if !compareSegmentFiles(t, tempDir, baselineDirEnv) {
			t.Errorf("SEGMENT-LEVEL COMPARISON FAILED")
		} else {
			t.Logf("=== Segment-level comparison PASSED ===")
		}

		if strategy == StrategyMapCollection || strategy == StrategyInverted {
			t.Logf("=== Comparing post-compaction keys against baseline ===")
			snapMain := snapshotBucketKeys(t, tempDir, strategy)
			snapBaseline := snapshotBucketKeys(t, baselineDirEnv, strategy)

			if !compareSnapshots(t, snapBaseline, snapMain) {
				t.Errorf("KEY-LEVEL COMPARISON FAILED")
			} else {
				t.Logf("=== Key-level comparison PASSED ===")
			}
		}

		compareBaselineStats(t, savedBaseline, currentResult)
	} else {
		t.Logf("=== No COMPACT_BASELINE_DIR set; skipping cross-branch comparison ===")
	}

	// --- Final memory stats ---
	var finalStats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&finalStats)

	t.Logf("")
	t.Logf("=== Bucket information ===")
	t.Logf("Bucket path: %s", bucketPath)
	t.Logf("Strategy: %s", strategy)
	t.Logf("Segments: %d, Total size: %s", segCount, formatBytes(uint64(segSize)))
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
	t.Logf("HeapAlloc delta:  %s", formatBytesSigned(int64(finalStats.HeapAlloc)-int64(memBaseline.HeapAlloc)))
	t.Logf("HeapInuse delta:  %s", formatBytesSigned(int64(finalStats.HeapInuse)-int64(memBaseline.HeapInuse)))

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
