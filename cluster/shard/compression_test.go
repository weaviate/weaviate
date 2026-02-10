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
	"encoding/json"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
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

func TestCompression_DeleteObject_RoundTrip(t *testing.T) {
	deleteReq := &shardproto.DeleteObjectRequest{
		Id:               "12345678-1234-1234-1234-123456789abc",
		DeletionTimeUnix: 1700000000000000000,
		SchemaVersion:    42,
	}
	subCmd, err := proto.Marshal(deleteReq)
	require.NoError(t, err)

	compressed := s2.Encode(nil, subCmd)
	decompressed, err := s2.Decode(nil, compressed)
	require.NoError(t, err)

	var recovered shardproto.DeleteObjectRequest
	require.NoError(t, proto.Unmarshal(decompressed, &recovered))
	assert.Equal(t, deleteReq.Id, recovered.Id)
	assert.Equal(t, deleteReq.DeletionTimeUnix, recovered.DeletionTimeUnix)
	assert.Equal(t, deleteReq.SchemaVersion, recovered.SchemaVersion)
}

func TestCompression_MergeObject_RoundTrip(t *testing.T) {
	doc := objects.MergeDocument{
		Class:           "TestClass",
		ID:              strfmt.UUID("12345678-1234-1234-1234-123456789abc"),
		PrimitiveSchema: map[string]interface{}{"name": "test"},
	}
	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	mergeReq := &shardproto.MergeObjectRequest{
		MergeDocumentJson: docJSON,
		SchemaVersion:     42,
	}
	subCmd, err := proto.Marshal(mergeReq)
	require.NoError(t, err)

	compressed := s2.Encode(nil, subCmd)
	decompressed, err := s2.Decode(nil, compressed)
	require.NoError(t, err)

	var recovered shardproto.MergeObjectRequest
	require.NoError(t, proto.Unmarshal(decompressed, &recovered))

	var recoveredDoc objects.MergeDocument
	require.NoError(t, json.Unmarshal(recovered.MergeDocumentJson, &recoveredDoc))
	assert.Equal(t, doc.ID, recoveredDoc.ID)
	assert.Equal(t, doc.Class, recoveredDoc.Class)
}

func TestCompression_PutObjectsBatch_RoundTrip(t *testing.T) {
	obj := makeTestObject()
	objBytes, err := obj.MarshalBinary()
	require.NoError(t, err)

	batchReq := &shardproto.PutObjectsBatchRequest{
		Objects:       [][]byte{objBytes, objBytes},
		SchemaVersion: 42,
	}
	subCmd, err := proto.Marshal(batchReq)
	require.NoError(t, err)

	compressed := s2.Encode(nil, subCmd)
	decompressed, err := s2.Decode(nil, compressed)
	require.NoError(t, err)

	var recovered shardproto.PutObjectsBatchRequest
	require.NoError(t, proto.Unmarshal(decompressed, &recovered))
	assert.Len(t, recovered.Objects, 2)

	recoveredObj, err := storobj.FromBinary(recovered.Objects[0])
	require.NoError(t, err)
	assert.Equal(t, obj.Object.ID, recoveredObj.Object.ID)
}

func TestCompression_DeleteObjectsBatch_RoundTrip(t *testing.T) {
	deleteReq := &shardproto.DeleteObjectsBatchRequest{
		Uuids:            []string{"11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"},
		DeletionTimeUnix: 1700000000000000000,
		DryRun:           false,
		SchemaVersion:    42,
	}
	subCmd, err := proto.Marshal(deleteReq)
	require.NoError(t, err)

	compressed := s2.Encode(nil, subCmd)
	decompressed, err := s2.Decode(nil, compressed)
	require.NoError(t, err)

	var recovered shardproto.DeleteObjectsBatchRequest
	require.NoError(t, proto.Unmarshal(decompressed, &recovered))
	assert.Len(t, recovered.Uuids, 2)
	assert.Equal(t, deleteReq.Uuids[0], recovered.Uuids[0])
	assert.Equal(t, deleteReq.DeletionTimeUnix, recovered.DeletionTimeUnix)
}

func TestCompression_AddReferences_RoundTrip(t *testing.T) {
	refs := objects.BatchReferences{
		{OriginalIndex: 0, Tenant: "tenantA"},
		{OriginalIndex: 1, Tenant: "tenantB"},
	}
	refsJSON, err := json.Marshal(refs)
	require.NoError(t, err)

	addReq := &shardproto.AddReferencesRequest{
		ReferencesJson: refsJSON,
		SchemaVersion:  42,
	}
	subCmd, err := proto.Marshal(addReq)
	require.NoError(t, err)

	compressed := s2.Encode(nil, subCmd)
	decompressed, err := s2.Decode(nil, compressed)
	require.NoError(t, err)

	var recovered shardproto.AddReferencesRequest
	require.NoError(t, proto.Unmarshal(decompressed, &recovered))

	var recoveredRefs objects.BatchReferences
	require.NoError(t, json.Unmarshal(recovered.ReferencesJson, &recoveredRefs))
	assert.Len(t, recoveredRefs, 2)
	assert.Equal(t, "tenantA", recoveredRefs[0].Tenant)
	assert.Equal(t, "tenantB", recoveredRefs[1].Tenant)
}

func TestCompression_CompressedDeleteObject_Applied(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	id := strfmt.UUID("12345678-1234-1234-1234-123456789abc")
	mockShard.EXPECT().DeleteObject(mock.Anything, id, mock.Anything).Return(nil)

	deleteReq := &shardproto.DeleteObjectRequest{
		Id:               string(id),
		DeletionTimeUnix: 1700000000000000000,
	}
	subCmd, err := proto.Marshal(deleteReq)
	require.NoError(t, err)
	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_DELETE_OBJECT,
		Class:      testClassName,
		Shard:      testShardName,
		SubCommand: compressed,
		Compressed: true,
	}

	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestCompression_CompressedMergeObject_Applied(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	mockShard.EXPECT().MergeObject(mock.Anything, mock.Anything).Return(nil)

	doc := objects.MergeDocument{
		Class: testClassName,
		ID:    strfmt.UUID("12345678-1234-1234-1234-123456789abc"),
	}
	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	mergeReq := &shardproto.MergeObjectRequest{MergeDocumentJson: docJSON}
	subCmd, err := proto.Marshal(mergeReq)
	require.NoError(t, err)
	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_MERGE_OBJECT,
		Class:      testClassName,
		Shard:      testShardName,
		SubCommand: compressed,
		Compressed: true,
	}

	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestCompression_CompressedPutObjectsBatch_Applied(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	mockShard.EXPECT().PutObjectBatch(mock.Anything, mock.Anything).Return([]error{nil})

	obj := makeTestObject()
	objBytes, err := obj.MarshalBinary()
	require.NoError(t, err)

	batchReq := &shardproto.PutObjectsBatchRequest{Objects: [][]byte{objBytes}}
	subCmd, err := proto.Marshal(batchReq)
	require.NoError(t, err)
	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH,
		Class:      testClassName,
		Shard:      testShardName,
		SubCommand: compressed,
		Compressed: true,
	}

	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestCompression_CompressedDeleteObjectsBatch_Applied(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	uuids := []strfmt.UUID{"11111111-1111-1111-1111-111111111111"}
	mockShard.EXPECT().DeleteObjectBatch(mock.Anything, mock.Anything, mock.Anything, false).Return(objects.BatchSimpleObjects{
		{UUID: uuids[0]},
	})

	deleteReq := &shardproto.DeleteObjectsBatchRequest{
		Uuids:            []string{string(uuids[0])},
		DeletionTimeUnix: 1700000000000000000,
	}
	subCmd, err := proto.Marshal(deleteReq)
	require.NoError(t, err)
	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_DELETE_OBJECTS_BATCH,
		Class:      testClassName,
		Shard:      testShardName,
		SubCommand: compressed,
		Compressed: true,
	}

	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestCompression_CompressedAddReferences_Applied(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	mockShard.EXPECT().AddReferencesBatch(mock.Anything, mock.Anything).Return([]error{nil})

	refs := objects.BatchReferences{{OriginalIndex: 0, Tenant: "tenantA"}}
	refsJSON, err := json.Marshal(refs)
	require.NoError(t, err)

	addReq := &shardproto.AddReferencesRequest{ReferencesJson: refsJSON}
	subCmd, err := proto.Marshal(addReq)
	require.NoError(t, err)
	compressed := s2.Encode(nil, subCmd)

	req := &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_ADD_REFERENCES,
		Class:      testClassName,
		Shard:      testShardName,
		SubCommand: compressed,
		Compressed: true,
	}

	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}
