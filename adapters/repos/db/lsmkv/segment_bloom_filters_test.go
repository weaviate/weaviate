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
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
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

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
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

	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
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

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
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
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	// just to ensure segments are loaded
	cursor := b2.Cursor()
	cursor.Close()

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

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
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
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	value, err := b2.Get([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world"), value)
}

func TestRepairTooShortBloomOnInit(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".bloom")
	require.True(t, ok)
	b.Shutdown(ctx)

	// now corrupt the bloom filter by randomly overriding data
	require.Nil(t, corruptBloomFileByTruncatingIt(path.Join(dirName, fname)))

	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
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

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
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
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	value := make([]byte, 5)
	value, _, err = b2.GetBySecondaryIntoMemory(0, []byte("bonjour"), value)
	assert.Nil(t, err)
	assert.Equal(t, []byte("world"), value)

	err = b2.Delete([]byte("hello"))
	assert.Nil(t, err)

	v, err := b2.Get([]byte("hello"))
	assert.Nil(t, err)
	assert.Nil(t, v)

	value, _, err = b2.GetBySecondaryIntoMemory(0, []byte("bonjour"), value)
	assert.Nil(t, err)
	assert.Nil(t, value)
}

func TestRepairCorruptedBloomSecondaryOnInitIntoMemory(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, "secondary.0.bloom")
	require.True(t, ok)

	b.Shutdown(ctx)

	// now corrupt the file by replacing the count value without adapting the checksum
	require.Nil(t, corruptBloomFile(path.Join(dirName, fname)))

	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)
	defer b2.Shutdown(ctx)

	value, err := b2.GetBySecondary(0, []byte("bonjour"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world"), value)
}

func TestRepairTooShortBloomSecondaryOnInit(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world"),
		WithSecondaryKey(0, []byte("bonjour"))))
	require.Nil(t, b.FlushMemtable())

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, "secondary.0.bloom")
	require.True(t, ok)

	b.Shutdown(ctx)
	// now corrupt the file by replacing the count value without adapting the checksum
	require.Nil(t, corruptBloomFileByTruncatingIt(path.Join(dirName, fname)))

	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
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
		_, err := loadWithChecksum(path.Join(dirName, "my-file"), -1, nil)
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

		_, err = loadWithChecksum(path.Join(dirName, "my-file"), 17, nil)
		assert.NotNil(t, err)
	})
}

func BenchmarkLoading(b *testing.B) {
	for _, val := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", val), func(b *testing.B) {
			dirName := b.TempDir()
			fName := path.Join(dirName, fmt.Sprintf("my-file-%d", val))
			f, err := os.Create(fName)
			require.Nil(b, err)
			data := make([]byte, val)
			for i := 0; i < len(data); i++ {
				data[i] = byte(rand.Intn(100))
			}
			chmsum := crc32.ChecksumIEEE(data[4:])
			binary.LittleEndian.PutUint32(data[:4], chmsum)
			_, err = f.Write(data)
			require.NoError(b, err)

			require.NoError(b, f.Sync())
			require.NoError(b, f.Close())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				loadedData, err := loadWithChecksum(fName, len(data), nil)
				require.NoError(b, err)
				require.Equal(b, loadedData, data[4:])
			}
		})
	}
}

func TestBloom_OFF(t *testing.T) {
	ctx := context.Background()
	tests := bucketTests{
		{
			name: "dontCreateBloom",
			f:    dontCreateBloom,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
				WithUseBloomFilter(false),
			},
		},
		{
			name: "dontRecreateBloom",
			f:    dontRecreateBloom,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
				WithUseBloomFilter(false),
			},
		},
		{
			name: "dontPrecomputeBloom",
			f:    dontPrecomputeBloom,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
				WithUseBloomFilter(false),
			},
		},
	}
	tests.run(ctx, t)
}

func dontCreateBloom(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	t.Run("populate", func(t *testing.T) {
		require.NoError(t, b.Put([]byte("hello"), []byte("world"),
			WithSecondaryKey(0, []byte("bonjour"))))
		require.NoError(t, b.FlushMemtable())
	})

	t.Run("check files", func(t *testing.T) {
		files, err := os.ReadDir(dirName)
		require.NoError(t, err)

		_, ok := findFileWithExt(files, ".bloom")
		assert.False(t, ok)
		_, ok = findFileWithExt(files, "secondary.0.bloom")
		assert.False(t, ok)
	})

	t.Run("search", func(t *testing.T) {
		valuePrimary, err := b.Get([]byte("hello"))
		require.NoError(t, err)
		valueSecondary, err := b.GetBySecondary(0, []byte("bonjour"))
		require.NoError(t, err)

		assert.Equal(t, []byte("world"), valuePrimary)
		assert.Equal(t, []byte("world"), valueSecondary)
	})
}

func dontRecreateBloom(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	t.Run("create, populate, shutdown", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			opts...)
		require.NoError(t, err)
		defer b.Shutdown(ctx)

		require.NoError(t, b.Put([]byte("hello"), []byte("world"),
			WithSecondaryKey(0, []byte("bonjour"))))
		require.NoError(t, b.FlushMemtable())
	})

	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...)
	require.NoError(t, err)
	defer b2.Shutdown(ctx)

	t.Run("check files", func(t *testing.T) {
		files, err := os.ReadDir(dirName)
		require.NoError(t, err)

		_, ok := findFileWithExt(files, ".bloom")
		assert.False(t, ok)
		_, ok = findFileWithExt(files, "secondary.0.bloom")
		assert.False(t, ok)
	})

	t.Run("search", func(t *testing.T) {
		valuePrimary, err := b2.Get([]byte("hello"))
		require.NoError(t, err)
		valueSecondary, err := b2.GetBySecondary(0, []byte("bonjour"))
		require.NoError(t, err)

		assert.Equal(t, []byte("world"), valuePrimary)
		assert.Equal(t, []byte("world"), valueSecondary)
	})
}

func dontPrecomputeBloom(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...)
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	t.Run("populate, compact", func(t *testing.T) {
		require.NoError(t, b.Put([]byte("hello"), []byte("world"),
			WithSecondaryKey(0, []byte("bonjour"))))
		require.NoError(t, b.FlushMemtable())

		require.NoError(t, b.Put([]byte("hello2"), []byte("world2"),
			WithSecondaryKey(0, []byte("bonjour2"))))
		require.NoError(t, b.FlushMemtable())

		compacted, err := b.disk.compactOnce()
		require.NoError(t, err)
		require.True(t, compacted)
	})

	t.Run("check files", func(t *testing.T) {
		files, err := os.ReadDir(dirName)
		require.NoError(t, err)

		_, ok := findFileWithExt(files, ".bloom")
		assert.False(t, ok)
		_, ok = findFileWithExt(files, "secondary.0.bloom")
		assert.False(t, ok)
	})

	t.Run("search", func(t *testing.T) {
		valuePrimary, err := b.Get([]byte("hello"))
		require.NoError(t, err)
		valueSecondary, err := b.GetBySecondary(0, []byte("bonjour"))
		require.NoError(t, err)
		value2Primary, err := b.Get([]byte("hello2"))
		require.NoError(t, err)
		value2Secondary, err := b.GetBySecondary(0, []byte("bonjour2"))
		require.NoError(t, err)

		assert.Equal(t, []byte("world"), valuePrimary)
		assert.Equal(t, []byte("world"), valueSecondary)
		assert.Equal(t, []byte("world2"), value2Primary)
		assert.Equal(t, []byte("world2"), value2Secondary)
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

func corruptBloomFileByTruncatingIt(fname string) error {
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

	data = data[:2]

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

func BenchmarkName(b *testing.B) {
	logger, _ := test.NewNullLogger()
	fn := func(key []byte) (bool, error) { return true, nil }

	for _, val := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", val), func(b *testing.B) {
			dirName := b.TempDir()
			ctx := context.Background()
			bu, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyReplace))
			require.Nil(b, err)

			for i := 0; i < val; i++ {
				require.Nil(b, bu.Put([]byte(fmt.Sprintf("hello-%v", i)), []byte(fmt.Sprintf("world-%v", i))))
			}

			require.Nil(b, bu.FlushMemtable())
			bu.Shutdown(ctx)

			files, err := os.ReadDir(dirName)
			require.NoError(b, err)

			fnames, ok := findFileWithExt(files, ".db")
			assert.True(b, ok)
			assert.NotNil(b, fnames)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := newSegment(path.Join(dirName, fnames), logger, nil, fn, segmentConfig{
					mmapContents:     false,
					useBloomFilter:   true,
					overwriteDerived: true,
				})
				require.NoError(b, err)
			}
		})
	}
}
