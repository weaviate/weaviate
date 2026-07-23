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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func TestNopCommitLogger_InterfaceSatisfaction(t *testing.T) {
	// Compile-time assertion is in commitlogger_nop.go, but let's also
	// verify at runtime that we can assign to the interface.
	var cl memtableCommitLogger = &nopCommitLogger{}
	assert.NotNil(t, cl)
}

func TestNopCommitLogger_AllMethodsReturnZeroValues(t *testing.T) {
	cl := &nopCommitLogger{}

	assert.NoError(t, cl.writeEntry(CommitTypeReplace, []byte("test")))
	assert.NoError(t, cl.put(segmentReplaceNode{}))
	assert.NoError(t, cl.append(segmentCollectionNode{}))
	assert.NoError(t, cl.add(&roaringset.SegmentNodeList{}))
	assert.Equal(t, "", cl.walPath())
	assert.Equal(t, int64(0), cl.size())
	assert.NoError(t, cl.flushBuffers())
	assert.NoError(t, cl.close())
	assert.NoError(t, cl.delete())
	assert.NoError(t, cl.sync())
}

func TestNopCommitLogger_NoFileSystemSideEffects(t *testing.T) {
	dir := t.TempDir()

	cl := &nopCommitLogger{}

	// Perform operations that would normally create .wal files
	require.NoError(t, cl.writeEntry(CommitTypeReplace, []byte("data")))
	require.NoError(t, cl.put(segmentReplaceNode{}))
	require.NoError(t, cl.flushBuffers())
	require.NoError(t, cl.sync())

	// Verify no .wal files were created in the temp directory
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, entry := range entries {
		assert.NotEqual(t, ".wal", filepath.Ext(entry.Name()),
			"unexpected .wal file found: %s", entry.Name())
	}
}
