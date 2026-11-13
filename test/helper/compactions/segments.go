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

// Package compactions inspects LSM segment state from outside a running
// Weaviate container — the only blackbox way to detect compaction activity
// in acceptance tests.
package compactions

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// listDir runs `ls -1` on path inside the container and returns the entry names.
func listDir(ctx context.Context, c testcontainers.Container, path string) ([]string, error) {
	return execLines(ctx, c, []string{"ls", "-1", path})
}

func execLines(ctx context.Context, c testcontainers.Container, cmd []string) ([]string, error) {
	code, reader, err := c.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		return nil, fmt.Errorf("exec %q: %w", cmd, err)
	}
	buf := new(strings.Builder)
	if _, err := io.Copy(buf, reader); err != nil {
		return nil, fmt.Errorf("read output of %q: %w", cmd, err)
	}
	if code != 0 {
		return nil, fmt.Errorf("exec %q: exit code %d: %s", cmd, code, buf.String())
	}
	var lines []string
	for line := range strings.SplitSeq(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, nil
}

func bucketPath(col, shard, bucket string) string {
	return fmt.Sprintf("/data/%s/%s/lsm/%s", strings.ToLower(col), shard, bucket)
}

// ListBucketFiles executes `ls -1` inside the container for the given bucket
// directory and returns plain filenames (basenames). It returns nil on error.
func ListBucketFiles(ctx context.Context, c testcontainers.Container, col, shard, bucket string) []string {
	files, _ := listDir(ctx, c, bucketPath(col, shard, bucket))
	return files
}

func ListLSMBuckets(ctx context.Context, c testcontainers.Container, col, shard string) []string {
	dirs, _ := listDir(ctx, c, fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(col), shard))
	return dirs
}

func liveSegments(files []string) []string {
	var segs []string
	for _, f := range files {
		if strings.HasSuffix(f, ".db") {
			segs = append(segs, f)
		}
	}
	return segs
}

// SegmentNames returns the names of the bucket's live segment files (*.db).
func SegmentNames(ctx context.Context, c testcontainers.Container, col, shard, bucket string) ([]string, error) {
	files, err := listDir(ctx, c, bucketPath(col, shard, bucket))
	if err != nil {
		return nil, err
	}
	return liveSegments(files), nil
}

// StagedSegmentCount counts the segment files hard-linked into the staged
// bucket directories matching dirGlob. Backup staging mirrors the data root
// (/data/.backup-staging-*/<col>/<shard>/lsm/<bucket>); replica staging mirrors
// the shard (/data/.replica-staging-*/lsm/<bucket>). No match counts as zero.
func StagedSegmentCount(ctx context.Context, c testcontainers.Container, dirGlob string) (int, error) {
	files, err := execLines(ctx, c, []string{"sh", "-c", "ls -1 " + dirGlob + " 2>/dev/null || true"})
	if err != nil {
		return 0, err
	}
	return len(liveSegments(files)), nil
}

// MergeWatcher detects a compaction that ran after Arm. Only compaction and
// cleanup remove a segment file, so a segment that first appeared after Arm
// and later disappears was merged after Arm.
type MergeWatcher struct {
	baseline map[string]struct{}
	fresh    map[string]struct{}
}

// Arm records the listing taken at the start of the observation window.
func (w *MergeWatcher) Arm(segments []string) {
	w.baseline = make(map[string]struct{}, len(segments))
	for _, s := range segments {
		w.baseline[s] = struct{}{}
	}
	w.fresh = map[string]struct{}{}
}

// Observe takes the latest listing and reports whether a segment that first
// appeared after Arm is gone from it.
func (w *MergeWatcher) Observe(segments []string) bool {
	current := make(map[string]struct{}, len(segments))
	for _, s := range segments {
		current[s] = struct{}{}
	}
	for s := range w.fresh {
		if _, ok := current[s]; !ok {
			return true
		}
	}
	for s := range current {
		if _, ok := w.baseline[s]; !ok {
			w.fresh[s] = struct{}{}
		}
	}
	return false
}

// CountDeletemeSegments counts files ending in ".deleteme".
func CountDeletemeSegments(ctx context.Context, c testcontainers.Container, col, shard, bucket string) int {
	files := ListBucketFiles(ctx, c, col, shard, bucket)
	count := 0
	for _, f := range files {
		if strings.HasSuffix(f, ".deleteme") {
			count++
		}
	}
	return count
}

// TotalSegmentFileCount counts all *.db and *.deleteme files.
// Monotonically increasing while a view is held (proxy for "N flushes occurred").
func TotalSegmentFileCount(ctx context.Context, c testcontainers.Container, col, shard, bucket string) int {
	files := ListBucketFiles(ctx, c, col, shard, bucket)
	count := 0
	for _, f := range files {
		if strings.HasSuffix(f, ".db") || strings.HasSuffix(f, ".deleteme") {
			count++
		}
	}
	return count
}

// reExplicitLevel matches e.g. ".l3.s2.db" or ".l3.s2.db.TIMESTAMP.deleteme"
var reExplicitLevel = regexp.MustCompile(`\.l(\d+)\.s\d+\.db(\..*\.deleteme)?$`)

// SegmentLevel parses the compaction level from a segment filename.
//
//	segment-{nanos}.db                → 0
//	segment-{id1}_{id2}.db            → 1
//	segment-{id1}_{id2}.l{N}.s{M}.db  → N
func SegmentLevel(name string) int {
	if m := reExplicitLevel.FindStringSubmatch(name); m != nil {
		level := 0
		fmt.Sscanf(m[1], "%d", &level)
		return level
	}
	after, ok := strings.CutPrefix(name, "segment-")
	if !ok {
		return 0
	}
	stem := strings.SplitN(after, ".", 2)[0]
	if strings.Contains(stem, "_") {
		return 1
	}
	return 0
}

// IsStable returns true when no bucket has ≥2 segments at the same level,
// meaning all pending compactions have completed and the LSM tree has stabilised.
func IsStable(ctx context.Context, c testcontainers.Container, col, shard string, buckets []string) bool {
	for _, bucket := range buckets {
		files := ListBucketFiles(ctx, c, col, shard, bucket)
		levelCount := map[int]int{}
		for _, f := range files {
			if !strings.HasSuffix(f, ".db") || strings.HasSuffix(f, ".deleteme") {
				continue
			}
			lvl := SegmentLevel(f)
			levelCount[lvl]++
		}
		for _, cnt := range levelCount {
			if cnt >= 2 {
				return false
			}
		}
	}
	return true
}

func MaxLevelAndCount(ctx context.Context, c testcontainers.Container, col, shard, bucket string) (maxLevel, segCount int) {
	for _, f := range ListBucketFiles(ctx, c, col, shard, bucket) {
		if !strings.HasSuffix(f, ".db") || strings.HasSuffix(f, ".deleteme") {
			continue
		}
		segCount++
		if lvl := SegmentLevel(f); lvl > maxLevel {
			maxLevel = lvl
		}
	}
	return maxLevel, segCount
}
