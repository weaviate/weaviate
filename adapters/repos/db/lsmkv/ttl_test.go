package lsmkv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func BenchmarkBucketDeletion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testDeletion(b, 100000)
	}
}

func TestBucketDeletion(t *testing.T) {
	testDeletion(t, 100000)
}

func testDeletion(t testing.TB, numObjects int) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	// initial bucket, always create segment, even if it is just a single entry
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1), WithKeepTombstones(true), WithCalcCountNetAdditions(true))
	require.NoError(t, err)

	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("hello%d", i)
		val := fmt.Sprintf("world%d", i)
		secKey := fmt.Sprintf("second%d", i)

		require.NoError(t, b.Put([]byte(key), []byte(val), WithSecondaryKey(0, []byte(secKey))))
		if i%10000 == 0 {
			require.NoError(t, b.FlushMemtable())
		}
	}

	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)
	for _, e := range entries {
		fmt.Println(e.Name())
	}

	concurrency := runtime.GOMAXPROCS(0)
	objectsPerGoroutine := numObjects / concurrency
	wg := &sync.WaitGroup{}
	wg.Add(concurrency)

	deletionTime := time.Now()

	for i := range concurrency {
		go func() {
			for j := 0; j < objectsPerGoroutine; j++ {
				idx := i*objectsPerGoroutine + j
				key := fmt.Sprintf("hello%d", idx)
				// simulate batch delete by read before delete
				val, err := b.get([]byte(key))
				require.NotNil(t, val)

				require.NoError(t, err)
				require.NoError(t, b.DeleteWith([]byte(key), deletionTime))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestBucketDeletion2(t *testing.T) {
	testDeletion2(t, 100000)
}

func testDeletion2(t testing.TB, numObjects int) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	// initial bucket, always create segment, even if it is just a single entry
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1), WithKeepTombstones(true), WithCalcCountNetAdditions(true))
	require.NoError(t, err)

	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("hello%d", i)
		val := fmt.Sprintf("world%d", i)
		secKey := fmt.Sprintf("second%d", i)

		require.NoError(t, b.Put([]byte(key), []byte(val), WithSecondaryKey(0, []byte(secKey))))
		if i%10000 == 0 {
			require.NoError(t, b.FlushMemtable())
			fmt.Printf("flushing %d\n", i)
		}
	}

	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		fmt.Println(e.Name())
		if filepath.Ext(name) == ".db" {
			info, err := e.Info()
			require.NoError(t, err)
			fmt.Println(info.Size())
		}
	}

	concurrency := runtime.GOMAXPROCS(0)
	objectsPerGoroutine := numObjects / concurrency
	wg := &sync.WaitGroup{}
	wg.Add(concurrency)

	deletionTime := time.Now()

	for i := range concurrency {
		go func() {
			for j := 0; j < objectsPerGoroutine; j++ {
				idx := i*objectsPerGoroutine + j
				key := fmt.Sprintf("hello%d", idx)
				// simulate batch delete by read before delete
				val, err := b.get([]byte(key))
				require.NotNil(t, val)

				require.NoError(t, err)
				require.NoError(t, b.DeleteWith([]byte(key), deletionTime))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
