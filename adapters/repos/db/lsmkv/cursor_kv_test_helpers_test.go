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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectReplaceKVs iterates a Replace cursor from First() to end, mapping every
// key/value pair through build. Keys and values are copied before build sees
// them, so the result stays valid after the cursor is closed. It lets callers
// assemble a slice of a test-local record type without repeating the
// open/iterate/copy boilerplate. This lives in an untagged file because it is
// shared by both integrationTest-tagged and untagged compaction tests.
func collectReplaceKVs[T any](t *testing.T, b *Bucket, build func(k, v []byte) T) []T {
	t.Helper()
	c, err := b.Cursor()
	require.NoError(t, err)
	defer c.Close()

	var out []T
	for k, v := c.First(); k != nil; k, v = c.Next() {
		kc := make([]byte, len(k))
		copy(kc, k)
		vc := make([]byte, len(v))
		copy(vc, v)
		out = append(out, build(kc, vc))
	}
	return out
}

// assertReplaceKVs collects every entry from a Replace cursor via build and
// asserts the result equals expected. Folding the collect+compare pair into one
// call keeps the verify-before/after-compaction blocks — repeated across the
// compaction suites — from re-appearing as duplicated new code.
func assertReplaceKVs[T any](t *testing.T, b *Bucket, expected []T, build func(k, v []byte) T) {
	t.Helper()
	assert.Equal(t, expected, collectReplaceKVs(t, b, build))
}
