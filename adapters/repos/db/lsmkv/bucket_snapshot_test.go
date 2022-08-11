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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_PauseCompaction(t *testing.T) {
	t.Run("assert that context timeout works for long compactions", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		b, err := NewBucket(ctx, dirName, "", logrus.New(), nil, WithStrategy(StrategyReplace))
		require.Nil(t, err)

		ctx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		defer cancel()

		err = b.PauseCompaction(ctx)
		require.NotNil(t, err)
		assert.Equal(t, "long-running compaction in progress: context deadline exceeded", err.Error())

		err = b.Shutdown(context.Background())
		require.Nil(t, err)
	})

	t.Run("assert compaction is successfully paused", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		b, err := NewBucket(ctx, dirName, "", logrus.New(), nil, WithStrategy(StrategyReplace))
		require.Nil(t, err)

		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		t.Run("insert contents into bucket", func(t *testing.T) {
			for i := 0; i < 10; i++ {
				err := b.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
				require.Nil(t, err)
			}
		})

		err = b.PauseCompaction(ctx)
		assert.Nil(t, err)

		err = b.Shutdown(context.Background())
		require.Nil(t, err)
	})
}

func TestSnapshot_FlushMemtable(t *testing.T) {
	t.Run("assert that context timeout works for long flushes", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		b, err := NewBucket(ctx, dirName, "", logrus.New(), nil, WithStrategy(StrategyReplace))
		require.Nil(t, err)

		ctx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		defer cancel()

		err = b.FlushMemtable(ctx)
		require.NotNil(t, err)
		assert.Equal(t, "long-running memtable flush in progress: context deadline exceeded", err.Error())

		err = b.Shutdown(context.Background())
		require.Nil(t, err)
	})

	t.Run("assert that flushes run successfully", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		b, err := NewBucket(ctx, dirName, "", logrus.New(), nil, WithStrategy(StrategyReplace))
		require.Nil(t, err)

		t.Run("insert contents into bucket", func(t *testing.T) {
			for i := 0; i < 10; i++ {
				err := b.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
				require.Nil(t, err)
			}
		})

		ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		err = b.FlushMemtable(ctx)
		assert.Nil(t, err)

		err = b.Shutdown(context.Background())
		require.Nil(t, err)
	})

	t.Run("assert that readonly bucket fails to flush", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		b, err := NewBucket(ctx, dirName, "", logrus.New(), nil, WithStrategy(StrategyReplace))
		require.Nil(t, err)

		b.UpdateStatus(storagestate.StatusReadOnly)

		ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		err = b.FlushMemtable(ctx)
		require.NotNil(t, err)

		expectedErr := errors.Wrap(storagestate.ErrStatusReadOnly, "flush memtable")
		assert.EqualError(t, expectedErr, err.Error())

		err = b.Shutdown(context.Background())
		require.Nil(t, err)
	})
}

func TestSnapshot_ListFiles(t *testing.T) {
	ctx := context.Background()

	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	b, err := NewBucket(ctx, dirName, "", logrus.New(), nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	t.Run("insert contents into bucket", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := b.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
			require.Nil(t, err)
		}
	})

	t.Run("assert expected bucket contents", func(t *testing.T) {
		files, err := b.ListFiles(ctx)
		assert.Nil(t, err)
		assert.Len(t, files, 1)

		expected := fmt.Sprintf("%s.wal", b.active.path)
		assert.Equal(t, expected, files[0])
	})

	err = b.Shutdown(context.Background())
	require.Nil(t, err)
}

func TestSnapshot_ResumeCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	b, err := NewBucket(ctx, dirName, "", logrus.New(), nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	t.Run("insert contents into bucket", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := b.Put([]byte(fmt.Sprint(i)), []byte(fmt.Sprint(i)))
			require.Nil(t, err)
		}
	})

	t.Run("assert compaction restarts after pausing", func(t *testing.T) {
		err = b.PauseCompaction(ctx)
		require.Nil(t, err)

		err = b.ResumeCompaction(ctx)
		assert.Nil(t, err)

		t.Run("assert cycle restarts", func(t *testing.T) {
			assert.True(t, b.flushCycle.Running())
			assert.True(t, b.disk.compactionCycle.Running())
		})
	})

	err = b.Shutdown(context.Background())
	require.Nil(t, err)
}

func makeTestDir(t *testing.T) string {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	if err := os.MkdirAll(dirName, 0o777); err != nil {
		t.Fatalf("failed to make test dir '%s': %s", dirName, err)
	}
	return dirName
}

func removeTestDir(t *testing.T, dirName string) {
	if err := os.RemoveAll(dirName); err != nil {
		t.Errorf("failed to remove test dir '%s': %s", dirName, err)
	}
}
