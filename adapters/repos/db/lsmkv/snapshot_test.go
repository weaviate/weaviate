package lsmkv

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_PauseCompaction(t *testing.T) {
	t.Run("assert that context timeout works for long compactions", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		b, err := NewBucket(ctx, dirName, logrus.New(), nil, WithStrategy(StrategyReplace))
		require.Nil(t, err)

		b.disk.compactionInProgress = true
		err = b.PauseCompaction(ctx, time.Millisecond)
		assert.Equal(t, "long-running compaction in progress, exceeded timeout of 1ms", err.Error())
	})

	t.Run("assert bucket set to READONLY when successful", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		b, err := NewBucket(ctx, dirName, logrus.New(), nil, WithStrategy(StrategyReplace))
		require.Nil(t, err)

		err = b.PauseCompaction(ctx, 100*time.Millisecond)
		assert.Nil(t, err)

		assert.True(t, b.isReadOnly(), "failed to set bucket as READONLY")
	})
}

func TestSnapshot_FlushMemtable(t *testing.T) {
}

func TestSnapshot_ListFiles(t *testing.T) {
}

func TestSnapshot_ResumeCompaction(t *testing.T) {
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
