//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func benchShard(b *testing.B, props int, tokenization string) (*Shard, string) {
	names := make([]string, props)
	for i := range names {
		names[i] = fmt.Sprintf("p%02d", i)
	}
	className := "WriteBench_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, names)
	for _, p := range class.Properties {
		p.Tokenization = tokenization
	}
	shd, _ := testShardWithSettings(b, testCtx(), class, enthnsw.UserConfig{Skip: true}, false, false, false)
	b.Cleanup(func() { shd.Shutdown(testCtx()) })
	return shd.(*Shard), className
}

func benchObject(className string, id strfmt.UUID, props int, value func(i int) string) *storobj.Object {
	m := make(map[string]interface{}, props)
	for i := 0; i < props; i++ {
		m[fmt.Sprintf("p%02d", i)] = value(i)
	}
	return &storobj.Object{MarshallerVersion: 1, Object: models.Object{
		ID: id, Class: className, Properties: m, LastUpdateTimeUnix: time.Now().UnixMilli(),
	}}
}

func BenchmarkPutObjectInvertedWrite(b *testing.B) {
	word := func(i int) string { return fmt.Sprintf("word%d other%d", i, i%3) }
	b.Run("create", func(b *testing.B) {
		shard, className := benchShard(b, 10, models.PropertyTokenizationWord)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			require.NoError(b, shard.PutObject(testCtx(), benchObject(className, strfmt.UUID(uuid.NewString()), 10, word)))
		}
	})
	b.Run("update 1 of 10 properties", func(b *testing.B) {
		shard, className := benchShard(b, 10, models.PropertyTokenizationWord)
		id := strfmt.UUID(uuid.NewString())
		require.NoError(b, shard.PutObject(testCtx(), benchObject(className, id, 10, word)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			obj := benchObject(className, id, 10, word)
			obj.Object.Properties.(map[string]interface{})["p00"] = fmt.Sprintf("changed%d", i)
			obj.Object.LastUpdateTimeUnix += int64(i + 1)
			require.NoError(b, shard.PutObject(testCtx(), obj))
		}
	})
	b.Run("unchanged re-put", func(b *testing.B) {
		shard, className := benchShard(b, 10, models.PropertyTokenizationWord)
		obj := benchObject(className, strfmt.UUID(uuid.NewString()), 10, word)
		require.NoError(b, shard.PutObject(testCtx(), obj))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			require.NoError(b, shard.PutObject(testCtx(), benchObject(className, obj.ID(), 10, word)))
		}
	})
	b.Run("concurrent creates, 50 indexed properties", func(b *testing.B) {
		shard, className := benchShard(b, 50, models.PropertyTokenizationWord)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				require.NoError(b, shard.PutObject(testCtx(), benchObject(className, strfmt.UUID(uuid.NewString()), 50, word)))
			}
		})
	})
}

// BenchmarkCJKUpdatesInvertedWrite measures kagome_ja updates and how long registerDoubleWriteWithScope
// waits for them (analysis waits for ApacTokenizerThrottle). Run with ENABLE_TOKENIZER_KAGOME_JA=true.
func BenchmarkCJKUpdatesInvertedWrite(b *testing.B) {
	text := strings.Repeat("東京都は日本の首都であり、多くの人が住んでいます。", 40)
	update := func(shard *Shard, className string, id strfmt.UUID, n int64) error {
		obj := benchObject(className, id, 1, func(int) string { return fmt.Sprintf("%s%d", text, n) })
		obj.Object.LastUpdateTimeUnix += n
		return shard.PutObject(testCtx(), obj)
	}

	b.Run("concurrent updates", func(b *testing.B) {
		shard, className := benchShard(b, 1, models.PropertyTokenizationKagomeJa)
		var n atomic.Int64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			id := strfmt.UUID(uuid.NewString())
			for pb.Next() {
				require.NoError(b, update(shard, className, id, n.Add(1)))
			}
		})
	})
	b.Run("arm latency under concurrent updates", func(b *testing.B) {
		shard, className := benchShard(b, 1, models.PropertyTokenizationKagomeJa)
		var stop atomic.Bool
		var n atomic.Int64
		done := make(chan error, 4)
		for range cap(done) {
			id := strfmt.UUID(uuid.NewString())
			enterrors.GoWrapper(func() {
				var err error
				for err == nil && !stop.Load() {
					err = update(shard, className, id, n.Add(1))
				}
				done <- err
			}, shard.index.logger)
		}
		noop := func(*Shard, uint64, *inverted.Property) error { return nil }
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			disarm := shard.registerDoubleWriteWithScope([]string{"unrelated"}, nil,
				func(map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex) {
					return noop, noop
				})
			b.StopTimer()
			disarm("unrelated")
			b.StartTimer()
		}
		b.StopTimer()
		stop.Store(true)
		for range cap(done) {
			require.NoError(b, <-done)
		}
	})
}
