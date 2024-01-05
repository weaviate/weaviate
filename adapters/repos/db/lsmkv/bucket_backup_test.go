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
			opts: []BucketOption{WithStrategy(StrategyReplace)},
		},
	}
	tests.run(ctx, t)
}

func bucketBackup_FlushMemtable(ctx context.Context, t *testing.T, opts []BucketOption) {
	t.Run("assert that readonly bucket fails to flush", func(t *testing.T) {
		dirName := t.TempDir()

		b, err := NewBucket(ctx, dirName, dirName, logrus.New(), nil,
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

	b, err := NewBucket(ctx, dirName, dirName, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)

	t.Run("insert contents into bucket", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := b.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
			require.Nil(t, err)
		}
		b.FlushMemtable() // flush memtable to generate .db files
	})

	t.Run("assert expected bucket contents", func(t *testing.T) {
		files, err := b.ListFiles(ctx, dirName)
		assert.Nil(t, err)
		assert.Len(t, files, 3)

		exts := make([]string, 3)
		for i, file := range files {
			exts[i] = filepath.Ext(file)
		}
		assert.Contains(t, exts, ".db")    // the segment itself
		assert.Contains(t, exts, ".bloom") // the segment's bloom filter
		assert.Contains(t, exts, ".cna")   // the segment's count net additions
	})

	err = b.Shutdown(context.Background())
	require.Nil(t, err)
}
