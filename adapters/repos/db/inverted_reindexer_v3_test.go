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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessingQueue(t *testing.T) {
	t.Run("single key", func(t *testing.T) {
		expKey := "some_key"
		expTasks := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t1"}}
		interval := 10 * time.Millisecond
		expTime := time.Now().Add(interval)
		q := newShardsQueue()

		q.insert(expKey, expTasks, expTime)
		key, tasks, err := q.getWhenReady(context.Background())
		after := time.Now()

		require.NoError(t, err)
		assert.Equal(t, expKey, key)
		assert.ElementsMatch(t, expTasks, tasks)
		assert.LessOrEqual(t, expTime.UnixNano(), after.UnixNano())
	})

	t.Run("multiple keys", func(t *testing.T) {
		expKey1 := "some_key_1"
		expKey2 := "some_key_2"
		expKey3 := "some_key_3"
		expKey4 := "some_key_4"
		expKey5 := "some_key_5"
		expTasks1 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t1"}}
		expTasks2 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t2"}}
		expTasks3 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t3"}}
		expTasks4 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t4"}}
		expTasks5 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t5"}}
		interval := 10 * time.Millisecond
		expTime1 := time.Now().Add(interval)
		expTime2 := time.Now().Add(interval * 2)
		expTime3 := time.Now().Add(interval * 3)
		expTime4 := time.Now().Add(interval * 4)
		expTime5 := time.Now().Add(interval * 5)

		q := newShardsQueue()

		q.insert(expKey4, expTasks4, expTime4)
		q.insert(expKey3, expTasks3, expTime3)
		q.insert(expKey1, expTasks1, expTime1)
		q.insert(expKey5, expTasks5, expTime5)
		q.insert(expKey2, expTasks2, expTime2)

		key1, tasks1, err1 := q.getWhenReady(context.Background())
		after1 := time.Now()
		key2, tasks2, err2 := q.getWhenReady(context.Background())
		after2 := time.Now()
		key3, tasks3, err3 := q.getWhenReady(context.Background())
		after3 := time.Now()
		key4, tasks4, err4 := q.getWhenReady(context.Background())
		after4 := time.Now()
		key5, tasks5, err5 := q.getWhenReady(context.Background())
		after5 := time.Now()

		require.NoError(t, err1)
		assert.Equal(t, expKey1, key1)
		assert.ElementsMatch(t, expTasks1, tasks1)
		assert.LessOrEqual(t, expTime1.UnixNano(), after1.UnixNano())

		require.NoError(t, err2)
		assert.Equal(t, expKey2, key2)
		assert.ElementsMatch(t, expTasks2, tasks2)
		assert.LessOrEqual(t, expTime2.UnixNano(), after2.UnixNano())

		require.NoError(t, err3)
		assert.Equal(t, expKey3, key3)
		assert.ElementsMatch(t, expTasks3, tasks3)
		assert.LessOrEqual(t, expTime3.UnixNano(), after3.UnixNano())

		require.NoError(t, err4)
		assert.Equal(t, expKey4, key4)
		assert.ElementsMatch(t, expTasks4, tasks4)
		assert.LessOrEqual(t, expTime4.UnixNano(), after4.UnixNano())

		require.NoError(t, err5)
		assert.Equal(t, expKey5, key5)
		assert.ElementsMatch(t, expTasks5, tasks5)
		assert.LessOrEqual(t, expTime5.UnixNano(), after5.UnixNano())
	})

	t.Run("multiple keys, cancelled context", func(t *testing.T) {
		expKey1 := "some_key_1"
		expKey2 := "some_key_2"
		expKey3 := "some_key_3"
		expKey4 := "some_key_4"
		expKey5 := "some_key_5"
		interval := 10 * time.Millisecond
		expTasks1 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t1"}}
		expTasks2 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t2"}}
		expTasks3 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t3"}}
		expTasks4 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t4"}}
		expTasks5 := []ShardReindexTaskV3{&dummyShardReindexTaskV3{name: "t5"}}
		expTime1 := time.Now().Add(interval)
		expTime2 := time.Now().Add(interval * 2)
		expTime3 := time.Now().Add(interval * 3)
		expTime4 := time.Now().Add(interval * 4)
		expTime5 := time.Now().Add(interval * 5)

		q := newShardsQueue()

		q.insert(expKey4, expTasks4, expTime4)
		q.insert(expKey3, expTasks3, expTime3)
		q.insert(expKey1, expTasks1, expTime1)
		q.insert(expKey5, expTasks5, expTime5)
		q.insert(expKey2, expTasks2, expTime2)

		ctx, cancel := context.WithCancel(context.Background())

		key1, tasks1, err1 := q.getWhenReady(ctx)
		after1 := time.Now()
		key2, tasks2, err2 := q.getWhenReady(ctx)
		after2 := time.Now()
		cancel()
		key3, tasks3, err3 := q.getWhenReady(ctx)
		after3 := time.Now()
		key4, tasks4, err4 := q.getWhenReady(ctx)
		after4 := time.Now()
		key5, tasks5, err5 := q.getWhenReady(ctx)
		after5 := time.Now()

		require.NoError(t, err1)
		assert.Equal(t, expKey1, key1)
		assert.ElementsMatch(t, expTasks1, tasks1)
		assert.LessOrEqual(t, expTime1.UnixNano(), after1.UnixNano())

		require.NoError(t, err2)
		assert.Equal(t, expKey2, key2)
		assert.ElementsMatch(t, expTasks2, tasks2)
		assert.LessOrEqual(t, expTime2.UnixNano(), after2.UnixNano())

		require.Error(t, err3)
		assert.Empty(t, key3)
		assert.Empty(t, tasks3)
		assert.Greater(t, expTime3.UnixNano(), after3.UnixNano())

		require.Error(t, err4)
		assert.Empty(t, key4)
		assert.Empty(t, tasks4)
		assert.Greater(t, expTime3.UnixNano(), after4.UnixNano())

		require.Error(t, err5)
		assert.Empty(t, key5)
		assert.Empty(t, tasks5)
		assert.Greater(t, expTime3.UnixNano(), after5.UnixNano())
	})
}

type dummyShardReindexTaskV3 struct {
	name string
}

func (t *dummyShardReindexTaskV3) Name() string {
	return t.name
}

func (t *dummyShardReindexTaskV3) OnBeforeLsmInit(ctx context.Context, shard *Shard) error {
	return nil
}

func (t *dummyShardReindexTaskV3) OnAfterLsmInit(ctx context.Context, shard *Shard) error {
	return nil
}

func (t *dummyShardReindexTaskV3) OnAfterLsmInitAsync(ctx context.Context, shard ShardLike,
) (rerunAt time.Time, reloadShard bool, err error) {
	return time.Time{}, false, nil
}
