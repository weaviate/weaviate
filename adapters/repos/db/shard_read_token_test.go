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
	"context"
	"encoding/binary"
	stderrors "errors"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// TestShardReadTokenByIndexID: the token read returns, from the
// objects bucket written as the shard writes it, the token the whole-object
// read decodes, in the memtable and after a flush, and errs for an absent
// doc id, a missing target vector and a position past the object.
func TestShardReadTokenByIndexID(t *testing.T) {
	ctx := context.Background()
	shd, _ := testShard(t, ctx, "TestClass")
	shard := shd.(*Shard)
	bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	const target = "colbert"
	docs := map[uint64][][]float32{
		0: {{1, 2, 3, 4}, {5, 6, 7, 8}},
		1: {{9, 10, 11, 12}, {13, 14, 15, 16}, {17, 18, 19, 20}},
	}
	// written as shard_write_put.go writes an object: the binary under the
	// uuid, the doc id as the secondary key
	put := func(docID uint64, tokens [][]float32) {
		obj := storobj.New(docID)
		uid := uuid.New()
		obj.Object = models.Object{ID: strfmt.UUID(uid.String()), Class: "TestClass"}
		obj.MultiVectors = map[string][][]float32{target: tokens}
		data, err := obj.MarshalBinaryDisk(false)
		require.NoError(t, err)
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		require.NoError(t, bucket.Put(uid[:], data, lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
	}
	for id, tokens := range docs {
		put(id, tokens)
	}

	container := &common.VectorSlice{Buff8: make([]byte, 8)}
	check := func(t *testing.T) {
		for id, tokens := range docs {
			whole, err := shard.multiVectorByIndexID(ctx, id, target)
			require.NoError(t, err)
			require.Equal(t, tokens, whole)
			for pos := range tokens {
				token, err := shard.readTokenByIndexID(ctx, id, pos, target, 4, container)
				require.NoError(t, err)
				require.Equal(t, tokens[pos], token, "doc %d token %d", id, pos)
			}
			_, err = shard.readTokenByIndexID(ctx, id, len(tokens), target, 4, container)
			require.ErrorContains(t, err, "but token")
			_, err = shard.readTokenByIndexID(ctx, id, 0, "other", 4, container)
			require.ErrorContains(t, err, "not found")
			_, err = shard.readTokenByIndexID(ctx, id, 0, target, 5, container)
			require.Error(t, err, "another dimension")
		}
		_, err := shard.readTokenByIndexID(ctx, 7, 0, target, 4, container)
		require.ErrorContains(t, err, "no object for doc id")
	}

	tests := []struct {
		name  string
		setup func(t *testing.T)
	}{
		{name: "in the memtable", setup: func(*testing.T) {}},
		{name: "in a segment", setup: func(t *testing.T) { require.NoError(t, bucket.FlushMemtable()) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(t)
			check(t)
		})
	}
}

// TestShardTokenByIndexIDKeepsItsMemory: the tokens the thunk returned stay as
// they were after later reads: the index keeps them without a copy.
func TestShardTokenByIndexIDKeepsItsMemory(t *testing.T) {
	ctx := context.Background()
	shd, _ := testShard(t, ctx, "TestClass")
	shard := shd.(*Shard)
	bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	const target = "colbert"
	docs := make(map[uint64][][]float32)
	for id := range uint64(8) {
		tokens := make([][]float32, 5)
		for pos := range tokens {
			tokens[pos] = []float32{float32(id), float32(pos), 1, 2}
		}
		docs[id] = tokens
		obj := storobj.New(id)
		uid := uuid.New()
		obj.Object = models.Object{ID: strfmt.UUID(uid.String()), Class: "TestClass"}
		obj.MultiVectors = map[string][][]float32{target: tokens}
		data, err := obj.MarshalBinaryDisk(false)
		require.NoError(t, err)
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, id)
		require.NoError(t, bucket.Put(uid[:], data, lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
	}

	type read struct {
		id    uint64
		pos   int
		token []float32
	}
	var (
		mu    sync.Mutex
		reads []read
		wg    sync.WaitGroup
	)
	for range 4 {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for id, tokens := range docs {
				for pos := range tokens {
					token, err := shard.tokenByIndexID(ctx, id, pos, target, 4)
					assert.NoError(t, err)
					mu.Lock()
					reads = append(reads, read{id, pos, token})
					mu.Unlock()
				}
			}
		}, shard.index.logger)
	}
	wg.Wait()

	require.Len(t, reads, 4*8*5)
	for _, r := range reads {
		require.Equal(t, docs[r.id][r.pos], r.token, "doc %d token %d", r.id, r.pos)
	}
}

// putTokenObject writes an object with the multi-vector tokens under target
// as shard_write_put.go writes it: the binary under the uuid, the doc id as
// the secondary key. It returns the uuid.
func putTokenObject(t *testing.T, bucket *lsmkv.Bucket, docID uint64, target string, tokens [][]float32) []byte {
	t.Helper()
	obj := storobj.New(docID)
	uid := uuid.New()
	obj.Object = models.Object{ID: strfmt.UUID(uid.String()), Class: "TestClass"}
	obj.MultiVectors = map[string][][]float32{target: tokens}
	data, err := obj.MarshalBinaryDisk(false)
	require.NoError(t, err)
	require.NoError(t, bucket.Put(uid[:], data, lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDKey(docID))))
	return uid[:]
}

func docIDKey(docID uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, docID)
	return key
}

// TestShardReadTokenOfADeletedObject: the token read of a deleted object
// returns storobj.ErrNotFound, whether the object and its delete are in
// the memtable or in segments.
func TestShardReadTokenOfADeletedObject(t *testing.T) {
	ctx := context.Background()
	const target = "colbert"
	tests := []struct {
		name                  string
		flushPut, flushDelete bool
	}{
		{name: "put and delete in the memtable"},
		{name: "put flushed, delete in the memtable", flushPut: true},
		{name: "put and delete flushed", flushPut: true, flushDelete: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shd, _ := testShard(t, ctx, "TestClass")
			shard := shd.(*Shard)
			bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
			require.NotNil(t, bucket)
			const docID = 3
			uid := putTokenObject(t, bucket, docID, target, [][]float32{{1, 2, 3, 4}})
			if tt.flushPut {
				require.NoError(t, bucket.FlushMemtable())
			}
			container := &common.VectorSlice{Buff8: make([]byte, 8)}
			_, err := shard.readTokenByIndexID(ctx, docID, 0, target, 4, container)
			require.NoError(t, err)

			require.NoError(t, bucket.Delete(uid, lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDKey(docID))))
			if tt.flushDelete {
				require.NoError(t, bucket.FlushMemtable())
			}
			_, err = shard.readTokenByIndexID(ctx, docID, 0, target, 4, container)
			var notFound storobj.ErrNotFound
			require.True(t, stderrors.As(err, &notFound), "%v", err)
		})
	}
}

// TestShardReadTokenOfOtherDimsThroughOneContainer: one container serves
// reads of tokens of different dimensions in turn, and each read returns
// its own token whole.
func TestShardReadTokenOfOtherDimsThroughOneContainer(t *testing.T) {
	ctx := context.Background()
	shd, _ := testShard(t, ctx, "TestClass")
	shard := shd.(*Shard)
	bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)
	const target = "colbert"
	four := [][]float32{{1, 2, 3, 4}, {5, 6, 7, 8}}
	six := [][]float32{{11, 12, 13, 14, 15, 16}, {17, 18, 19, 20, 21, 22}}
	putTokenObject(t, bucket, 0, target, four)
	putTokenObject(t, bucket, 1, target, six)

	container := &common.VectorSlice{Buff8: make([]byte, 8)}
	for _, read := range []struct {
		docID uint64
		pos   int
		want  []float32
	}{
		{docID: 0, pos: 1, want: four[1]},
		{docID: 1, pos: 0, want: six[0]},
		{docID: 0, pos: 0, want: four[0]},
		{docID: 1, pos: 1, want: six[1]},
	} {
		token, err := shard.readTokenByIndexID(ctx, read.docID, read.pos, target, len(read.want), container)
		require.NoError(t, err)
		require.Equal(t, read.want, token, "doc %d token %d", read.docID, read.pos)
	}
}
