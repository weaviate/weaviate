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
	"io"
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCreateBloomOnFlush(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewNoop(), cyclemanager.NewNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)

	_, ok := findFileWithExt(files, ".bloom")
	assert.True(t, ok)

	_, ok = findFileWithExt(files, "secondary.0.bloom")
	assert.True(t, ok)
	// on Windows we have to shutdown the bucket before opening it again
	require.Nil(t, b.Shutdown(ctx))

	b2, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewNoop(), cyclemanager.NewNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	valuePrimary, err := b2.Get([]byte("hello"))
	require.Nil(t, err)
	valueSecondary, err := b2.GetBySecondary(0, []byte("bonjour"))
	require.Nil(t, err)

	assert.Equal(t, []byte("world"), valuePrimary)
	assert.Equal(t, []byte("world"), valueSecondary)
}

func TestCreateBloomInit(t *testing.T) {
	// this test deletes the initial bloom and makes sure it gets recreated after
	// the bucket is initialized
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewNoop(), cyclemanager.NewNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)
	defer b.Shutdown(ctx)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable())

	for _, ext := range []string{".secondary.0.bloom", ".bloom"} {
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

	// now create a new bucket and assert that the file is re-created on init
	b2, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewNoop(), cyclemanager.NewNoop(),
		WithStrategy(StrategyReplace))
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	_, ok := findFileWithExt(files, ".bloom")
	require.True(t, ok)
	_, ok = findFileWithExt(files, ".secondary.0.bloom")
	require.True(t, ok)
}

func TestRepairCorruptedBloomOnInit(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewNoop(), cyclemanager.NewNoop(),
		WithStrategy(StrategyReplace))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".bloom")
	require.True(t, ok)

	// now corrupt the bloom filter by randomly overriding data
	require.Nil(t, corruptBloomFile(path.Join(dirName, fname)))
	// on Windows we have to shutdown the bucket before opening it again
	require.Nil(t, b.Shutdown(ctx))

	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewNoop(), cyclemanager.NewNoop(),
		WithStrategy(StrategyReplace))
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	value, err := b2.Get([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world"), value)
}

func TestRepairCorruptedBloomSecondaryOnInit(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewNoop(), cyclemanager.NewNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, "secondary.0.bloom")
	require.True(t, ok)

	// now corrupt the file by replacing the count value without adapting the checksum
	require.Nil(t, corruptBloomFile(path.Join(dirName, fname)))
	// on Windows we have to shutdown the bucket before opening it again
	require.Nil(t, b.Shutdown(ctx))

	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewNoop(), cyclemanager.NewNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	value, err := b2.GetBySecondary(0, []byte("bonjour"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world"), value)
}

func TestLoadWithChecksumErrorCases(t *testing.T) {
	t.Run("file does not exist", func(t *testing.T) {
		dirName := t.TempDir()
		_, err := loadWithChecksum(path.Join(dirName, "my-file"), -1)
		assert.NotNil(t, err)
	})

	t.Run("file has incorrect length", func(t *testing.T) {
		dirName := t.TempDir()
		fName := path.Join(dirName, "my-file")
		f, err := os.Create(fName)
		require.Nil(t, err)

		_, err = f.Write(make([]byte, 13))
		require.Nil(t, err)

		require.Nil(t, f.Close())

		_, err = loadWithChecksum(path.Join(dirName, "my-file"), 17)
		assert.NotNil(t, err)
	})
}

func corruptBloomFile(fname string) error {
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

	// corrupt it by setting all data bytes to 0x01
	for i := 5; i < len(data); i++ {
		data[i] = 0x01
	}

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
