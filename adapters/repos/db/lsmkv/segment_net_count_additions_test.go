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
	"context"
	"encoding/binary"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCNA(t *testing.T) {
	ctx := context.Background()
	tests := bucketTests{
		{
			name: "createCNAOnFlush",
			f:    createCNAOnFlush,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "createCNAInit",
			f:    createCNAInit,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "repairCorruptedCNAOnInit",
			f:    repairCorruptedCNAOnInit,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
	}
	tests.run(ctx, t)
}

func createCNAOnFlush(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer b.Shutdown(ctx)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)

	_, ok := findFileWithExt(files, ".cna")
	assert.True(t, ok)
}

func createCNAInit(ctx context.Context, t *testing.T, opts []BucketOption) {
	// this test deletes the initial cna and makes sure it gets recreated after
	// the bucket is initialized
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer b.Shutdown(ctx)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".cna")
	require.True(t, ok)

	err = os.RemoveAll(path.Join(dirName, fname))
	require.Nil(t, err)

	files, err = os.ReadDir(dirName)
	require.Nil(t, err)
	_, ok = findFileWithExt(files, ".cna")
	require.False(t, ok, "verify the file is really gone")

	// on Windows we have to shutdown the bucket before opening it again
	require.Nil(t, b.Shutdown(ctx))

	// now create a new bucket and assert that the file is re-created on init
	b2, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	files, err = os.ReadDir(dirName)
	require.Nil(t, err)
	_, ok = findFileWithExt(files, ".cna")
	require.True(t, ok)
}

func repairCorruptedCNAOnInit(ctx context.Context, t *testing.T, opts []BucketOption) {
	// this test deletes the initial cna and makes sure it gets recreated after
	// the bucket is initialized
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer b.Shutdown(ctx)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".cna")
	require.True(t, ok)

	// now corrupt the file by replacing the count value without adapting the checksum
	require.Nil(t, corruptCNAFile(path.Join(dirName, fname), 12345))

	// on Windows we have to shutdown the bucket before opening it again
	require.Nil(t, b.Shutdown(ctx))
	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	assert.Equal(t, 1, b2.Count())
}

func TestCNA_OFF(t *testing.T) {
	ctx := context.Background()
	tests := bucketTests{
		{
			name: "dontCreateCNA",
			f:    dontCreateCNA,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithCalcCountNetAdditions(false),
			},
		},
		{
			name: "dontRecreateCNA",
			f:    dontRecreateCNA,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithCalcCountNetAdditions(false),
			},
		},
		{
			name: "dontPrecomputeCNA",
			f:    dontPrecomputeCNA,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithCalcCountNetAdditions(false),
			},
		},
	}
	tests.run(ctx, t)
}

func dontCreateCNA(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	t.Run("populate", func(t *testing.T) {
		require.NoError(t, b.Put([]byte("hello"), []byte("world")))
		require.NoError(t, b.FlushMemtable())
	})

	t.Run("check files", func(t *testing.T) {
		files, err := os.ReadDir(dirName)
		require.NoError(t, err)

		_, ok := findFileWithExt(files, ".cna")
		assert.False(t, ok)
	})

	t.Run("count", func(t *testing.T) {
		assert.Equal(t, 0, b.Count())
	})
}

func dontRecreateCNA(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	t.Run("create, populate, shutdown", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			opts...)
		require.NoError(t, err)
		defer b.Shutdown(ctx)

		require.NoError(t, b.Put([]byte("hello"), []byte("world")))
		require.NoError(t, b.FlushMemtable())
	})

	b2, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...)
	require.NoError(t, err)
	defer b2.Shutdown(ctx)

	t.Run("check files", func(t *testing.T) {
		files, err := os.ReadDir(dirName)
		require.NoError(t, err)

		_, ok := findFileWithExt(files, ".cna")
		assert.False(t, ok)
	})

	t.Run("count", func(t *testing.T) {
		assert.Equal(t, 0, b2.Count())
	})
}

func dontPrecomputeCNA(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	t.Run("populate, compact", func(t *testing.T) {
		require.NoError(t, b.Put([]byte("hello"), []byte("world")))
		require.NoError(t, b.FlushMemtable())

		require.NoError(t, b.Put([]byte("hello2"), []byte("world2")))
		require.NoError(t, b.FlushMemtable())

		compacted, err := b.disk.compactOnce()
		require.NoError(t, err)
		require.True(t, compacted)
	})

	t.Run("check files", func(t *testing.T) {
		files, err := os.ReadDir(dirName)
		require.NoError(t, err)

		_, ok := findFileWithExt(files, ".cna")
		assert.False(t, ok)
	})

	t.Run("count", func(t *testing.T) {
		assert.Equal(t, 0, b.Count())
	})
}

func findFileWithExt(files []os.DirEntry, ext string) (string, bool) {
	for _, file := range files {
		fname := file.Name()
		if strings.HasSuffix(fname, ext) {
			return fname, true
		}

	}
	return "", false
}

func corruptCNAFile(fname string, corruptValue uint64) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(data[4:12], corruptValue)

	f, err = os.Create(fname)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return f.Close()
}
