//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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
)

func TestCreateBloomOnFlush(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable(ctx))

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)

	_, ok := findFileWithExt(files, ".bloom")
	assert.True(t, ok)

	_, ok = findFileWithExt(files, "secondary.0.bloom")
	assert.True(t, ok)
}

func TestCreateBloomInit(t *testing.T) {
	// this test deletes the initial bloom and makes sure it gets recreated after
	// the bucket is initialized
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable(ctx))

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
	_, err = NewBucket(ctx, dirName, "", logger, nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

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

	b, err := NewBucket(ctx, dirName, "", logger, nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable(ctx))

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".bloom")
	require.True(t, ok)

	// now corrupt the bloom filter by randomly overriding data
	require.Nil(t, corruptBloomFile(path.Join(dirName, fname)))

	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucket(ctx, dirName, "", logger, nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	value, err := b2.Get([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world"), value)
}

func TestRepairCorruptedBloomSecondaryOnInit(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil,
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable(ctx))

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, "secondary.0.bloom")
	require.True(t, ok)

	// now corrupt the file by replacing the count value without adapting the checksum
	require.Nil(t, corruptBloomFile(path.Join(dirName, fname)))

	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucket(ctx, dirName, "", logger, nil,
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)

	value, err := b2.GetBySecondary(0, []byte("bonjour"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world"), value)
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
