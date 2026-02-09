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

package shard_test

import (
	"context"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/storobj"
	"google.golang.org/protobuf/proto"
)

func TestCompression_RoundTrip(t *testing.T) {
	// Simulate what the replicator does: marshal + compress
	obj := makeTestObject()
	objBytes, err := obj.MarshalBinary()
	require.NoError(t, err)

	putReq := &shardproto.PutObjectRequest{
		Object:        objBytes,
		SchemaVersion: 42,
	}
	subCmd, err := proto.Marshal(putReq)
	require.NoError(t, err)

	compressed := s2.Encode(nil, subCmd)

	// Verify compression actually reduced size (or at least didn't fail)
	assert.NotEmpty(t, compressed)

	// Simulate what the FSM does: decompress + unmarshal
	decompressed, err := s2.Decode(nil, compressed)
	require.NoError(t, err)
	assert.Equal(t, subCmd, decompressed)

	var recovered shardproto.PutObjectRequest
	require.NoError(t, proto.Unmarshal(decompressed, &recovered))

	recoveredObj, err := storobj.FromBinary(recovered.Object)
	require.NoError(t, err)
	assert.Equal(t, obj.Object.ID, recoveredObj.Object.ID)
	assert.Equal(t, obj.Object.Class, recoveredObj.Object.Class)
	assert.InDeltaSlice(t, obj.Vector, recoveredObj.Vector, 1e-6)
}

func TestCompression_UncompressedEntryPassthrough(t *testing.T) {
	// Simulate an old (uncompressed) entry being replayed by the FSM.
	// The Compressed field defaults to false in protobuf.
	obj := makeTestObject()
	objBytes, err := obj.MarshalBinary()
	require.NoError(t, err)

	putReq := &shardproto.PutObjectRequest{
		Object: objBytes,
	}
	subCmd, err := proto.Marshal(putReq)
	require.NoError(t, err)

	// Build an ApplyRequest with Compressed=false (the default)
	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECT,
		Class:      testClassName,
		Shard:      testShardName,
		SubCommand: subCmd,
		Compressed: false,
	}

	// Apply through a real RAFT cluster to ensure the FSM handles it
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	mockShard.EXPECT().PutObject(mock.Anything, mock.Anything).Return(nil)

	_, err = store.Apply(context.Background(), req)
	require.NoError(t, err)

	mockShard.AssertCalled(t, "PutObject", mock.Anything, mock.Anything)
}

func TestCompression_CompressedEntryApplied(t *testing.T) {
	// Build a compressed ApplyRequest and apply through RAFT
	obj := makeTestObject()
	objBytes, err := obj.MarshalBinary()
	require.NoError(t, err)

	putReq := &shardproto.PutObjectRequest{
		Object: objBytes,
	}
	subCmd, err := proto.Marshal(putReq)
	require.NoError(t, err)

	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECT,
		Class:      testClassName,
		Shard:      testShardName,
		SubCommand: compressed,
		Compressed: true,
	}

	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	mockShard.EXPECT().PutObject(mock.Anything, mock.Anything).Return(nil)

	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))

	mockShard.AssertCalled(t, "PutObject", mock.Anything, mock.Anything)
}

func BenchmarkCompression_S2(b *testing.B) {
	// Create a realistic object payload for benchmarking
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object:            makeTestObject().Object,
		Vector:            make([]float32, 768), // Typical embedding dimension
	}
	for i := range obj.Vector {
		obj.Vector[i] = float32(i) * 0.001
	}

	objBytes, err := obj.MarshalBinary()
	require.NoError(b, err)

	putReq := &shardproto.PutObjectRequest{
		Object:        objBytes,
		SchemaVersion: 1,
	}
	subCmd, err := proto.Marshal(putReq)
	require.NoError(b, err)

	b.Run("encode", func(b *testing.B) {
		b.SetBytes(int64(len(subCmd)))
		for i := 0; i < b.N; i++ {
			_ = s2.Encode(nil, subCmd)
		}
	})

	compressed := s2.Encode(nil, subCmd)
	b.Run("decode", func(b *testing.B) {
		b.SetBytes(int64(len(compressed)))
		for i := 0; i < b.N; i++ {
			_, _ = s2.Decode(nil, compressed)
		}
	})

	b.Logf("original=%d compressed=%d ratio=%.2f%%",
		len(subCmd), len(compressed), float64(len(compressed))/float64(len(subCmd))*100)
}
