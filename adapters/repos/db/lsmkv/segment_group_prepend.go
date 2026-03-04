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

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// PrependSegmentsFromBucket copies all segments from srcDir and atomically
// prepends them into this SegmentGroup's segment list. The copied segments
// are given a timestamp shift so they sort before any existing segments on
// disk (and on reload). The shift is computed dynamically so that the highest
// source timestamp is strictly less than the lowest target timestamp, making
// this safe across multiple successive prepends.
//
// Preconditions:
//   - The source bucket must be shut down (or otherwise guaranteed immutable)
//     before calling this method. If compaction or flushing modifies/deletes
//     source files concurrently, the copy will be corrupted. The caller is
//     responsible for ensuring this.
//   - Replace strategy is explicitly not supported because existsOnLower
//     (used to compute countNetAdditions) is not recalculated for prepended
//     segments, which would produce incorrect object counts.
//   - Supported strategies: RoaringSet, RoaringSetRange, SetCollection,
//     MapCollection, Inverted.
func (sg *SegmentGroup) PrependSegmentsFromBucket(ctx context.Context, srcDir string) error {
	// Step 1: Validate strategy — Replace is not supported.
	if sg.strategy == StrategyReplace {
		return fmt.Errorf("prepend segments: strategy %q is not supported because "+
			"countNetAdditions cannot be recalculated for prepended segments", sg.strategy)
	}

	// Step 2: Discover source segments (.db files).
	srcDBFiles, err := discoverDBFiles(srcDir)
	if err != nil {
		return fmt.Errorf("prepend segments: discover source segments: %w", err)
	}
	if len(srcDBFiles) == 0 {
		return nil // no-op
	}

	// Pause compaction for the duration of the operation. Compaction's
	// switchInMemory uses stored segment indices — a prepend that shifts
	// indices while compaction is in flight would cause it to replace the
	// wrong segment. Pausing ensures no compaction cycle starts (and waits
	// for any in-progress cycle to finish) before we proceed.
	if err := sg.pauseCompaction(ctx); err != nil {
		return fmt.Errorf("prepend segments: pause compaction: %w", err)
	}
	defer func() {
		// Best-effort resume — if this fails, the segment group's compaction
		// stays paused, which is degraded but not data-losing.
		_ = sg.resumeCompaction(ctx)
	}()

	// Step 3: Compute timestamp shift and copy files with crash-safe staging.
	tgtDBFiles, err := discoverDBFiles(sg.dir)
	if err != nil {
		return fmt.Errorf("prepend segments: discover target segments: %w", err)
	}
	shift, err := computeTimestampShift(srcDBFiles, tgtDBFiles)
	if err != nil {
		return fmt.Errorf("prepend segments: compute timestamp shift: %w", err)
	}
	copiedDBPaths, err := copySegmentFiles(srcDir, sg.dir, srcDBFiles, shift)
	if err != nil {
		return fmt.Errorf("prepend segments: copy files: %w", err)
	}

	// Step 4: Initialize segments from the copied .db files.
	initialized, err := sg.initPrependedSegments(copiedDBPaths)
	if err != nil {
		// Clean up copied files on failure.
		cleanupCopiedFiles(sg.dir, copiedDBPaths)
		return fmt.Errorf("prepend segments: init segments: %w", err)
	}

	// Step 5: Atomic prepend under maintenanceLock.
	sg.maintenanceLock.Lock()
	newSegments := make([]Segment, 0, len(initialized)+len(sg.segments))
	newSegments = append(newSegments, initialized...)
	newSegments = append(newSegments, sg.segments...)
	sg.segments = newSegments
	sg.maintenanceLock.Unlock()

	// Update metrics for the newly added segments.
	for _, seg := range initialized {
		sg.metrics.IncSegmentTotalByStrategy(sg.strategy)
		sg.metrics.ObserveSegmentSize(sg.strategy, seg.Size())
	}

	return nil
}

// discoverDBFiles walks srcDir (non-recursively) and returns all .db filenames
// sorted lexicographically.
func discoverDBFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	var dbFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == ".db" {
			dbFiles = append(dbFiles, e.Name())
		}
	}
	slices.Sort(dbFiles)
	return dbFiles, nil
}

// copySegmentFiles copies all segment files (the .db and its auxiliary files
// like .bloom, .cna, .metadata) from srcDir to dstDir with a timestamp shift
// applied to the segment ID in the filename.
//
// Files are written with a .tmp suffix first, fsynced, then renamed to strip
// the suffix. A crash mid-copy leaves only .tmp files that are ignored by
// newSegmentGroup on recovery.
//
// Returns the list of final .db filenames (without .tmp) in dstDir.
func copySegmentFiles(srcDir, dstDir string, dbFiles []string, shift int64) ([]string, error) {
	copiedDBPaths := make([]string, 0, len(dbFiles))

	for _, dbFile := range dbFiles {
		// Find all files belonging to this segment (same prefix before first ".").
		segPrefix, _, _ := strings.Cut(dbFile, ".")
		associatedFiles, err := findAssociatedFiles(srcDir, segPrefix)
		if err != nil {
			return nil, fmt.Errorf("find associated files for %s: %w", dbFile, err)
		}

		// Compute the shifted segment prefix.
		shiftedPrefix, err := applyTimestampShift(segPrefix, shift)
		if err != nil {
			return nil, fmt.Errorf("shift timestamp for %s: %w", segPrefix, err)
		}

		// Copy all associated files with the shifted prefix.
		var stagedFiles []string
		for _, srcFile := range associatedFiles {
			dstFile := strings.Replace(srcFile, segPrefix, shiftedPrefix, 1)
			tmpPath := filepath.Join(dstDir, dstFile+".tmp")

			if err := copyFileWithSync(filepath.Join(srcDir, srcFile), tmpPath); err != nil {
				// Clean up any .tmp files from this segment on failure.
				for _, staged := range stagedFiles {
					os.Remove(staged)
				}
				return nil, fmt.Errorf("copy %s: %w", srcFile, err)
			}
			stagedFiles = append(stagedFiles, tmpPath)
		}

		// All files for this segment are written and synced. Strip .tmp suffix.
		for _, tmpPath := range stagedFiles {
			finalPath := strings.TrimSuffix(tmpPath, ".tmp")
			if err := os.Rename(tmpPath, finalPath); err != nil {
				return nil, fmt.Errorf("rename %s to %s: %w", tmpPath, finalPath, err)
			}
		}

		// Record the final .db path.
		dstDBFile := strings.Replace(dbFile, segPrefix, shiftedPrefix, 1)
		copiedDBPaths = append(copiedDBPaths, dstDBFile)
	}

	return copiedDBPaths, nil
}

// findAssociatedFiles returns all filenames in dir that share the given
// segment prefix (e.g., "segment-1234567890"). This includes the .db file
// and any auxiliary files like .bloom, .cna, .metadata, etc.
func findAssociatedFiles(dir, segPrefix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	prefix := segPrefix + "."
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), prefix) {
			files = append(files, e.Name())
		}
	}
	return files, nil
}

// computeTimestampShift computes the nanosecond shift to apply to source
// segment timestamps so that all source segments sort before all target
// segments on disk. The shift guarantees:
//
//	max(source) + shift < min(target)
//
// If target has no segments, a 1-second gap before time.Now() is used.
// A minimum gap of 1 second is enforced between the shifted source and the
// target to leave room for lexicographic ordering.
func computeTimestampShift(srcDBFiles, tgtDBFiles []string) (int64, error) {
	srcMax, err := maxSegmentTimestamp(srcDBFiles)
	if err != nil {
		return 0, fmt.Errorf("source timestamps: %w", err)
	}

	var tgtMin int64
	if len(tgtDBFiles) == 0 {
		// No target segments yet — place source 1 second before now.
		tgtMin = time.Now().UnixNano()
	} else {
		tgtMin, err = minSegmentTimestamp(tgtDBFiles)
		if err != nil {
			return 0, fmt.Errorf("target timestamps: %w", err)
		}
	}

	const gap = int64(time.Second) // 1s gap for safety
	// We need: srcMax + shift < tgtMin
	// So: shift < tgtMin - srcMax
	// Use: shift = tgtMin - srcMax - gap
	shift := tgtMin - srcMax - gap

	if shift >= 0 {
		// Source timestamps already sort before target — no shift needed.
		// But we still apply a small negative shift to ensure they stay
		// before any future flushes that might land between source max
		// and target min.
		return -gap, nil
	}

	return shift, nil
}

// parseSegmentTimestamp extracts the first (or only) nanosecond timestamp
// from a segment filename like "segment-1771258130098421000.db" or a
// compacted segment "segment-123_456.l0.s5.db".
func parseSegmentTimestamp(dbFile string) (int64, error) {
	name := strings.TrimPrefix(dbFile, "segment-")
	name, _, _ = strings.Cut(name, ".") // strip extensions
	// For compacted segments, take the first timestamp (before "_").
	tsStr, _, _ := strings.Cut(name, "_")

	return strconv.ParseInt(tsStr, 10, 64)
}

func maxSegmentTimestamp(dbFiles []string) (int64, error) {
	maxTS := int64(math.MinInt64)
	for _, f := range dbFiles {
		ts, err := parseSegmentTimestamp(f)
		if err != nil {
			return 0, fmt.Errorf("parse %q: %w", f, err)
		}
		if ts > maxTS {
			maxTS = ts
		}
	}
	return maxTS, nil
}

func minSegmentTimestamp(dbFiles []string) (int64, error) {
	minTS := int64(math.MaxInt64)
	for _, f := range dbFiles {
		ts, err := parseSegmentTimestamp(f)
		if err != nil {
			return 0, fmt.Errorf("parse %q: %w", f, err)
		}
		if ts < minTS {
			minTS = ts
		}
	}
	return minTS, nil
}

// applyTimestampShift takes a segment prefix like "segment-1771258130098421000"
// and returns a new prefix with the given nanosecond shift applied.
func applyTimestampShift(segPrefix string, shift int64) (string, error) {
	tsStr := strings.TrimPrefix(segPrefix, "segment-")
	// The timestamp may contain an underscore for compacted segments (e.g.,
	// "segment-123_456"). Take only the first part for the timestamp.
	parts := strings.SplitN(tsStr, "_", 2)

	timestamp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", fmt.Errorf("parse timestamp %q: %w", parts[0], err)
	}

	parts[0] = strconv.FormatInt(timestamp+shift, 10)

	return "segment-" + strings.Join(parts, "_"), nil
}

// copyFileWithSync copies src to dst and fsyncs dst before returning.
func copyFileWithSync(src, dst string) error {
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

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return dstFile.Sync()
}

// initPrependedSegments creates segment objects for the copied .db files.
func (sg *SegmentGroup) initPrependedSegments(dbFiles []string) ([]Segment, error) {
	segments := make([]Segment, 0, len(dbFiles))

	for _, dbFile := range dbFiles {
		path := filepath.Join(sg.dir, dbFile)
		seg, err := newSegment(path, sg.logger, sg.metrics,
			nil, // existsLower: not applicable for RoaringSet strategy
			segmentConfig{
				mmapContents:             sg.mmapContents,
				useBloomFilter:           sg.useBloomFilter,
				calcCountNetAdditions:    sg.calcCountNetAdditions,
				overwriteDerived:         false, // auxiliary files were already copied
				enableChecksumValidation: sg.enableChecksumValidation,
				MinMMapSize:              sg.MinMMapSize,
				allocChecker:             sg.allocChecker,
				writeMetadata:            sg.writeMetadata,
				deleteMarkerCounter:      sg.deleteMarkerCounter.Add(1),
			})
		if err != nil {
			// Close any segments already initialized.
			for _, s := range segments {
				s.close()
			}
			return nil, fmt.Errorf("init segment %s: %w", dbFile, err)
		}
		segments = append(segments, seg)
	}

	return segments, nil
}

// cleanupCopiedFiles removes all files in dir that match the given .db
// filenames and their associated auxiliary files.
func cleanupCopiedFiles(dir string, dbFiles []string) {
	for _, dbFile := range dbFiles {
		segPrefix, _, _ := strings.Cut(dbFile, ".")
		files, err := findAssociatedFiles(dir, segPrefix)
		if err != nil {
			continue
		}
		for _, f := range files {
			os.Remove(filepath.Join(dir, f))
		}
	}
}
