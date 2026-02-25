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

package compaction_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// listBucketFiles executes `ls -1` inside the container for the given bucket
// directory and returns plain filenames (basenames).
func listBucketFiles(ctx context.Context, c testcontainers.Container, col, shard, bucket string) []string {
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

// countDeletemeSegments counts files ending in ".deleteme".
func countDeletemeSegments(ctx context.Context, c testcontainers.Container, col, shard, bucket string) int {
	files := listBucketFiles(ctx, c, col, shard, bucket)
	count := 0
	for _, f := range files {
		if strings.HasSuffix(f, ".deleteme") {
			count++
		}
	}
	return count
}

// totalSegmentFileCount counts all *.db and *.deleteme files.
// Monotonically increasing while a view is held (proxy for "N flushes occurred").
func totalSegmentFileCount(ctx context.Context, c testcontainers.Container, col, shard, bucket string) int {
	files := listBucketFiles(ctx, c, col, shard, bucket)
	count := 0
	for _, f := range files {
		if strings.HasSuffix(f, ".db") || strings.HasSuffix(f, ".deleteme") {
			count++
		}
	}
	return count
}

// reExplicitLevel matches e.g. ".l3.s2.db", ".l3.s2.d1.db", or ".l3.s2.d1.db.TIMESTAMP.deleteme"
var reExplicitLevel = regexp.MustCompile(`\.l(\d+)\.s\d+(\.d1)?\.db(\..*\.deleteme)?$`)

// segmentLevel parses the compaction level from a segment filename.
//
//	segment-{nanos}.db                    → 0
//	segment-{id1}_{id2}.db                → 1
//	segment-{id1}_{id2}.l{N}.s{M}.db      → N
//	segment-{id1}_{id2}.l{N}.s{M}.d1.db   → N
func segmentLevel(name string) int {
	if m := reExplicitLevel.FindStringSubmatch(name); m != nil {
		level := 0
		fmt.Sscanf(m[1], "%d", &level)
		return level
	}
	// Strip prefix "segment-", then look at the id portion before the first "."
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

// isStable returns true when no bucket has ≥2 segments at the same level,
// meaning all pending compactions have completed and the LSM tree has stabilised.
func isStable(ctx context.Context, c testcontainers.Container, col, shard string, buckets []string) bool {
	for _, bucket := range buckets {
		files := listBucketFiles(ctx, c, col, shard, bucket)
		levelCount := map[int]int{}
		for _, f := range files {
			// Only count live segments (not deleteme)
			if !strings.HasSuffix(f, ".db") || strings.HasSuffix(f, ".deleteme") {
				continue
			}
			lvl := segmentLevel(f)
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

// holdView sends POST to the debug endpoint to hold a consistent view on a bucket.
func holdView(t *testing.T, debugHost, col, shard, bucket string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/debug/consistent-view/%s/shards/%s/buckets/%s",
		debugHost, col, shard, bucket)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"holdView failed: status=%d body=%s", resp.StatusCode, body)
}
