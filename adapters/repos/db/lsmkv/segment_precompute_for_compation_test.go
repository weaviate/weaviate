//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrecomputeSegmentMeta_Replace(t *testing.T) {
	// first build a complete reference segment of which we can then strip its
	// meta

	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1))
	require.Nil(t, err)
	defer b.Shutdown(ctx)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable(ctx))

	for _, ext := range []string{".secondary.0.bloom", ".bloom", ".cna"} {
		files, err := os.ReadDir(dirName)
		require.Nil(t, err)
		fname, ok := findFileWithExt(files, ext)
		require.True(t, ok)

		err = os.RemoveAll(path.Join(dirName, fname))
		require.Nil(t, err)

		files, err = os.ReadDir(dirName)
		require.Nil(t, err)
		_, ok = findFileWithExt(files, ext)
		require.False(t, ok, "verify the file is really gone")
	}

	require.Nil(t, b.Shutdown(ctx))

	// now identify the segment file and rename it to be a tmp file
	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".db")
	require.True(t, ok)

	segmentTmp := path.Join(dirName, fmt.Sprintf("%s.tmp", fname))
	err = os.Rename(path.Join(dirName, fname), segmentTmp)
	require.Nil(t, err)

	fileNames, err := preComputeSegmentMeta(segmentTmp, 1, logger)
	require.Nil(t, err)

	// there should be 4 files and they should all have a .tmp suffix:
	// segment.db.tmp
	// segment.cna.tmp
	// segment.bloom.tmp
	// segment.secondary.0.bloom.tmp
	assert.Len(t, fileNames, 4)
	for _, fName := range fileNames {
		assert.True(t, strings.HasSuffix(fName, ".tmp"))
	}
}

// Precomputing of segment is almost identical across segment types, however,
// only Replace supports CNA, so we should test at least one other segment type
// which does not support CNA, represented here by using the "Set" type
func TestPrecomputeSegmentMeta_Set(t *testing.T) {
	// first build a complete reference segment of which we can then strip its
	// meta

	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		WithStrategy(StrategySetCollection))
	require.Nil(t, err)
	defer b.Shutdown(ctx)

	err = b.SetAdd([]byte("greetings"), [][]byte{[]byte("hello"), []byte("hola")})
	require.Nil(t, err)
	require.Nil(t, b.FlushMemtable(ctx))

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".bloom")
	require.True(t, ok)

	err = os.RemoveAll(path.Join(dirName, fname))
	require.Nil(t, err)

	// verify it's actually gone
	files, err = os.ReadDir(dirName)
	require.Nil(t, err)
	_, ok = findFileWithExt(files, ".bloom")
	require.False(t, ok)

	require.Nil(t, b.Shutdown(ctx))

	// now identify the segment file and rename it to be a tmp file
	fname, ok = findFileWithExt(files, ".db")
	require.True(t, ok)

	segmentTmp := path.Join(dirName, fmt.Sprintf("%s.tmp", fname))
	err = os.Rename(path.Join(dirName, fname), segmentTmp)
	require.Nil(t, err)

	fileNames, err := preComputeSegmentMeta(segmentTmp, 1, logger)
	require.Nil(t, err)

	// there should be 2 files and they should all have a .tmp suffix:
	// segment.db.tmp
	// segment.bloom.tmp
	assert.Len(t, fileNames, 2)
	for _, fName := range fileNames {
		assert.True(t, strings.HasSuffix(fName, ".tmp"))
	}
}

func TestPrecomputeSegmentMeta_UnhappyPaths(t *testing.T) {
	t.Run("file without .tmp suffix", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		_, err := preComputeSegmentMeta("a-path-without-the-required-suffix", 7, logger)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "expects a .tmp segment")
	})

	t.Run("file does not exist", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		_, err := preComputeSegmentMeta("i-dont-exist.tmp", 7, logger)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("segment header can't be parsed", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		dirName := t.TempDir()
		segmentName := path.Join(dirName, "my-segment.tmp")

		header := &segmentindex.Header{
			Version: 100, // only supported version as of writing this test is 0
		}

		f, err := os.Create(segmentName)
		require.Nil(t, err)

		_, err = header.WriteTo(f)
		require.Nil(t, err)

		err = f.Close()
		require.Nil(t, err)

		_, err = preComputeSegmentMeta(segmentName, 7, logger)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parse header")
	})

	t.Run("unsupported strategy", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		dirName := t.TempDir()
		segmentName := path.Join(dirName, "my-segment.tmp")

		header := &segmentindex.Header{
			Version:  0,
			Strategy: segmentindex.Strategy(100), // this strategy doesn't exist
		}

		f, err := os.Create(segmentName)
		require.Nil(t, err)

		_, err = header.WriteTo(f)
		require.Nil(t, err)

		err = f.Close()
		require.Nil(t, err)

		_, err = preComputeSegmentMeta(segmentName, 7, logger)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "unsupported strategy")
	})
}
