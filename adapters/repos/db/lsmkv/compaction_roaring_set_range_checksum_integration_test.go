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

import "testing"

// A checksum leaves trailing bytes behind the segment payload that only the
// pread cursors have to page past. The bucketIntegrationTests matrix pairs
// checksums with mmap alone, so the combination is covered here instead.
func TestCompactionRoaringSetRangeStrategy_ChecksumPread(t *testing.T) {
	compactionRoaringSetRangeStrategy_Random(testCtx(), t, []BucketOption{
		WithStrategy(StrategyRoaringSetRange),
		WithSegmentsChecksumValidationEnabled(true),
		WithPread(true),
	})
}
