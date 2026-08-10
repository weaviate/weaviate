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

//go:build integrationTest

package db

import (
	"context"
	"encoding/binary"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestShardReadsHonorCtxCancellation: batch read loops must abort on a cancelled ctx.
func TestShardReadsHonorCtxCancellation(t *testing.T) {
	ctx := context.Background()
	_, idx := testShard(t, ctx, "ShardReadCtxCancel")
	s := firstShard(t, idx)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "MultiObjectRawByID",
			call: func(ctx context.Context) error {
				_, err := s.MultiObjectRawByID(ctx, []strfmt.UUID{strfmt.UUID(uuid.NewString())})
				return err
			},
		},
		{
			name: "ObjectDigests",
			call: func(ctx context.Context) error {
				_, err := s.ObjectDigests(ctx, []multi.Identifier{{ID: uuid.NewString()}})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, tc.call(cancelledCtx), context.Canceled)
		})
	}
}

// TestShardObjectByIndexID covers objectByIndexID, which reads into a pooled
// payload buffer: a recycled buffer must never serve one object's bytes for
// another. Only disk segments fill that buffer, so the table covers both
// storage locations.
func TestShardObjectByIndexID(t *testing.T) {
	for _, tc := range []struct {
		name   string
		onDisk bool
	}{
		{name: "from memtable"},
		{name: "from disk segment", onDisk: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "ObjectByIndexID" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx, newTestClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)

			// Flushing moves everything written so far into a disk segment, the
			// only path that reads through the pooled buffer.
			flushIfOnDisk := func(t *testing.T) {
				t.Helper()
				if tc.onDisk {
					require.NoError(t, shard.Store().FlushMemtables(ctx))
				}
			}

			// Sizes alternate so lookups both grow the pooled buffer and
			// under-fill it afterwards.
			texts := []string{
				strings.Repeat("a", 4),
				strings.Repeat("b", 64<<10),
				strings.Repeat("c", 16),
				strings.Repeat("d", 128<<10),
				strings.Repeat("e", 8),
			}
			docIDs := make([]uint64, len(texts))
			for i, text := range texts {
				obj := createTestObjectWithText(className, text)
				require.NoError(t, shard.PutObject(ctx, obj))
				docIDs[i] = obj.DocID
			}
			flushIfOnDisk(t)

			t.Run("objects survive later lookups", func(t *testing.T) {
				got := make([]*storobj.Object, len(docIDs))
				for i, docID := range docIDs {
					obj, err := shard.objectByIndexID(ctx, docID, false)
					require.NoError(t, err)
					got[i] = obj
				}

				// Asserted only after every lookup ran, so a buffer recycled by
				// a later lookup would show up as a corrupted earlier object.
				for i, obj := range got {
					require.Equal(t, docIDs[i], obj.DocID)
					require.Equal(t, texts[i], titleOfTestObject(obj))
				}
			})

			// Production reads shards whose data is only partly flushed. A
			// memtable hit leaves the buffer untouched, so it must not disturb
			// the capacity a segment hit grew.
			t.Run("interleaved with an unflushed object", func(t *testing.T) {
				const freshText = "written after the flush"
				fresh := createTestObjectWithText(className, freshText)
				require.NoError(t, shard.PutObject(ctx, fresh))

				for i, docID := range docIDs {
					obj, err := shard.objectByIndexID(ctx, docID, false)
					require.NoError(t, err)
					require.Equal(t, texts[i], titleOfTestObject(obj))

					got, err := shard.objectByIndexID(ctx, fresh.DocID, false)
					require.NoError(t, err)
					require.Equal(t, freshText, titleOfTestObject(got))
				}
			})

			// Enough rounds for readers to contend over the same pooled buffers,
			// so a buffer handed back before its object is decoded shows up as a
			// race.
			t.Run("concurrent lookups", func(t *testing.T) {
				var wg sync.WaitGroup
				for range 16 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range 50 {
							for i, docID := range docIDs {
								obj, err := shard.objectByIndexID(ctx, docID, false)
								if assert.NoError(t, err) {
									assert.Equal(t, texts[i], titleOfTestObject(obj))
								}
							}
						}
					}()
				}
				wg.Wait()
			})

			t.Run("update shrinking the payload", func(t *testing.T) {
				obj := createTestObjectWithText(className, strings.Repeat("f", 128<<10))
				require.NoError(t, shard.PutObject(ctx, obj))
				flushIfOnDisk(t)

				updated := createTestObjectWithText(className, "short")
				updated.Object.ID = obj.Object.ID
				require.NoError(t, shard.PutObject(ctx, updated))
				flushIfOnDisk(t)

				got, err := shard.objectByIndexID(ctx, updated.DocID, false)
				require.NoError(t, err)
				require.Equal(t, "short", titleOfTestObject(got))
			})

			t.Run("not found", func(t *testing.T) {
				deleted := createTestObjectWithText(className, "to be deleted")
				require.NoError(t, shard.PutObject(ctx, deleted))
				flushIfOnDisk(t)
				require.NoError(t, shard.DeleteObject(ctx, deleted.Object.ID, time.Now()))
				flushIfOnDisk(t)

				for _, missing := range []struct {
					name  string
					docID uint64
				}{
					{name: "doc id never assigned", docID: 1 << 20},
					{name: "deleted object", docID: deleted.DocID},
				} {
					t.Run(missing.name, func(t *testing.T) {
						_, err := shard.objectByIndexID(ctx, missing.docID, false)
						var notFound storobj.ErrNotFound
						require.ErrorAs(t, err, &notFound)
					})
				}
			})
		})
	}
}

// TestShardObjectByIndexIDReusesPayloadBuffer pins the reason objectByIndexID
// reads through a pooled buffer at all: it must allocate about one payload less
// per call than the same read with no buffer. The two are measured against each
// other rather than against a fixed budget because runtime.MemStats counts every
// goroutine's allocations, so an absolute figure moves with whatever else the
// process is doing while a difference does not.
func TestShardObjectByIndexIDReusesPayloadBuffer(t *testing.T) {
	ctx := testCtx()
	className := "ObjectByIndexIDAllocs" + uuid.NewString()[:8]
	shd, _ := testShardWithSettings(t, ctx, newTestClass(className),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)

	const payload = 256 << 10
	obj := createTestObjectWithText(className, strings.Repeat("x", payload))
	require.NoError(t, shard.PutObject(ctx, obj))
	// Only a disk segment reads through the buffer.
	require.NoError(t, shard.Store().FlushMemtables(ctx))

	bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	bucketClassName, err := bucket.ClassName()
	require.NoError(t, err)
	docIDKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDKey, obj.DocID)

	pooled := bytesPerCall(func() {
		got, err := shard.objectByIndexID(ctx, obj.DocID, false)
		require.NoError(t, err)
		runtime.KeepAlive(got)
	})
	// The control: the same lookup and decode reading into no buffer.
	unpooled := bytesPerCall(func() {
		data, err := bucket.GetBySecondary(ctx, 0, docIDKey)
		require.NoError(t, err)
		got, err := storobj.FromBinaryDisk(data, bucketClassName)
		require.NoError(t, err)
		runtime.KeepAlive(got)
	})

	// Measured at 0.75 to 0.81 payloads saved, and at 0 when objectByIndexID
	// reads into no buffer of its own.
	saved := int64(unpooled) - int64(pooled)
	require.Greater(t, saved, int64(payload/2),
		"objectByIndexID saved only %d bytes/call against a bufferless read of a %d byte object; the payload is being copied per read",
		saved, payload)
}

// titleOfTestObject returns "" when the property is missing or not a string, so
// the caller's own assertion reports the mismatch. It asserts nothing itself and
// is therefore safe to call from a non-test goroutine.
func titleOfTestObject(obj *storobj.Object) string {
	props, _ := obj.Properties().(map[string]interface{})
	title, _ := props["title"].(string)
	return title
}
