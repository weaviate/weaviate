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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func Test_BucketBackup(t *testing.T) {
	ctx := context.Background()
	tests := bucketTests{
		{
			name: "bucketBackup_FlushMemtable",
			f:    bucketBackup_FlushMemtable,
			opts: []BucketOption{WithStrategy(StrategyReplace)},
		},
		{
			name: "bucketBackup_ListFiles",
			f:    bucketBackup_ListFiles,
			opts: []BucketOption{WithStrategy(StrategyReplace), WithCalcCountNetAdditions(true)},
		},
	}
	tests.run(ctx, t)
}

func bucketBackup_FlushMemtable(ctx context.Context, t *testing.T, opts []BucketOption) {
	t.Run("assert that readonly bucket fails to flush", func(t *testing.T) {
		dirName := t.TempDir()

		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, logrus.New(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)
		b.UpdateStatus(storagestate.StatusReadOnly)

		err = b.FlushMemtable()
		require.NotNil(t, err)
		expectedErr := errors.Wrap(storagestate.ErrStatusReadOnly, "flush memtable")
		assert.EqualError(t, expectedErr, err.Error())

		err = b.Shutdown(context.Background())
		require.Nil(t, err)
	})
}

func bucketBackup_ListFiles(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err := b.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
		require.NoError(t, err)
	}

	// flush memtable to generate .db files
	err = b.FlushMemtable()
	require.NoError(t, err)

	// create an arbitrary directory and file that is a leftover of some old process
	leftoverDir := path.Join(dirName, "scratch_leftover")
	require.NoError(t, os.MkdirAll(leftoverDir, 0o755))
	require.NoError(t, os.WriteFile(path.Join(leftoverDir, "partial_segment.db"), []byte("some data"), 0o644))

	files, err := b.ListFiles(ctx, dirName)
	assert.NoError(t, err)
	assert.Len(t, files, 3)

	// make sure all these files are accessible to prove that the paths are correct
	for _, file := range files {
		_, err = os.Stat(file)
		require.NoError(t, err)
	}

	exts := make([]string, 3)
	for i, file := range files {
		exts[i] = filepath.Ext(file)
	}
	assert.Contains(t, exts, ".db")    // the segment itself
	assert.Contains(t, exts, ".bloom") // the segment's bloom filter
	assert.Contains(t, exts, ".cna")   // the segment's count net additions

	require.NoError(t, b.Shutdown(context.Background()))
}
