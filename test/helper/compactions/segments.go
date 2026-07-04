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
)

// ListBucketFiles executes `ls -1` inside the container for the given bucket
// directory and returns plain filenames (basenames).
func ListBucketFiles(ctx context.Context, c testcontainers.Container, col, shard, bucket string) []string {
	path := fmt.Sprintf("/data/%s/%s/lsm/%s", strings.ToLower(col), shard, bucket)
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", path})
	if err != nil || code != 0 {
		return nil
	}
	buf := new(strings.Builder)
	if _, err := io.Copy(buf, reader); err != nil {
		return nil
	}
	var files []string
	for line := range strings.SplitSeq(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			files = append(files, line)
		}
	}
	return files
}

func ListLSMBuckets(ctx context.Context, c testcontainers.Container, col, shard string) []string {
	path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(col), shard)
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", path})
	if err != nil || code != 0 {
		return nil
	}
	buf := new(strings.Builder)
	if _, err := io.Copy(buf, reader); err != nil {
		return nil
	}
	var dirs []string
	for line := range strings.SplitSeq(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			dirs = append(dirs, line)
		}
	}
	return dirs
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
