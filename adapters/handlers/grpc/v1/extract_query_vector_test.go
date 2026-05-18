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

package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// TestExtractQueryVector exercises the gRPC reply helper that hoists the
// vectorized query off results[0]._additional["queryVectorRaw"] into
// SearchReply.QueryVector.
func TestExtractQueryVector(t *testing.T) {
	r := &Replier{}

	t.Run("empty results returns nil", func(t *testing.T) {
		assert.Nil(t, r.extractQueryVector(nil))
		assert.Nil(t, r.extractQueryVector([]interface{}{}))
	})

	t.Run("missing _additional returns nil", func(t *testing.T) {
		res := []interface{}{map[string]interface{}{"foo": "bar"}}
		assert.Nil(t, r.extractQueryVector(res))
	})

	t.Run("missing queryVectorRaw returns nil", func(t *testing.T) {
		res := []interface{}{map[string]interface{}{
			"_additional": models.AdditionalProperties{"other": "stuff"},
		}}
		assert.Nil(t, r.extractQueryVector(res))
	})

	t.Run("single fp32 vector serialized correctly", func(t *testing.T) {
		vec := []float32{0.1, 0.2, 0.3}
		res := []interface{}{map[string]interface{}{
			"_additional": models.AdditionalProperties{
				"queryVectorRaw": models.Vectors{"": vec},
			},
		}}
		out := r.extractQueryVector(res)
		require.Len(t, out, 1)
		assert.Equal(t, "", out[0].Name)
		assert.Equal(t, pb.Vectors_VECTOR_TYPE_SINGLE_FP32, out[0].Type)
		assert.Equal(t, byteops.Fp32SliceToBytes(vec), out[0].VectorBytes)
	})

	t.Run("named single fp32 vectors serialized correctly", func(t *testing.T) {
		titleVec := []float32{0.1, 0.2}
		bodyVec := []float32{0.3, 0.4}
		res := []interface{}{map[string]interface{}{
			"_additional": models.AdditionalProperties{
				"queryVectorRaw": models.Vectors{
					"title": titleVec,
					"body":  bodyVec,
				},
			},
		}}
		out := r.extractQueryVector(res)
		require.Len(t, out, 2)
		byName := map[string]*pb.Vectors{out[0].Name: out[0], out[1].Name: out[1]}
		require.Contains(t, byName, "title")
		require.Contains(t, byName, "body")
		assert.Equal(t, byteops.Fp32SliceToBytes(titleVec), byName["title"].VectorBytes)
		assert.Equal(t, byteops.Fp32SliceToBytes(bodyVec), byName["body"].VectorBytes)
		assert.Equal(t, pb.Vectors_VECTOR_TYPE_SINGLE_FP32, byName["title"].Type)
		assert.Equal(t, pb.Vectors_VECTOR_TYPE_SINGLE_FP32, byName["body"].Type)
	})

	t.Run("multi fp32 vector (ColBERT) serialized correctly", func(t *testing.T) {
		multi := [][]float32{{0.1, 0.2}, {0.3, 0.4}}
		res := []interface{}{map[string]interface{}{
			"_additional": models.AdditionalProperties{
				"queryVectorRaw": models.Vectors{"col": multi},
			},
		}}
		out := r.extractQueryVector(res)
		require.Len(t, out, 1)
		assert.Equal(t, "col", out[0].Name)
		assert.Equal(t, pb.Vectors_VECTOR_TYPE_MULTI_FP32, out[0].Type)
		assert.Equal(t, byteops.Fp32SliceOfSlicesToBytes(multi), out[0].VectorBytes)
	})

	t.Run("accepts plain map[string]interface{} _additional", func(t *testing.T) {
		// The traverser may attach as either models.AdditionalProperties or a
		// plain map[string]interface{} depending on call path. Both must work.
		vec := []float32{0.7, 0.8}
		res := []interface{}{map[string]interface{}{
			"_additional": map[string]interface{}{
				"queryVectorRaw": models.Vectors{"": vec},
			},
		}}
		out := r.extractQueryVector(res)
		require.Len(t, out, 1)
		assert.Equal(t, byteops.Fp32SliceToBytes(vec), out[0].VectorBytes)
	})

	t.Run("empty queryVectorRaw returns nil", func(t *testing.T) {
		res := []interface{}{map[string]interface{}{
			"_additional": models.AdditionalProperties{
				"queryVectorRaw": models.Vectors{},
			},
		}}
		assert.Nil(t, r.extractQueryVector(res))
	})
}

// TestExtractAdditionalPropsFromMetadata_QueryVector verifies that the proto
// MetadataRequest.QueryVector flag round-trips into additional.Properties.
func TestExtractAdditionalPropsFromMetadata_QueryVector(t *testing.T) {
	class := &models.Class{Class: "Whatever"}

	t.Run("query_vector=true sets carrier bit", func(t *testing.T) {
		req := &pb.MetadataRequest{QueryVector: true}
		props, err := extractAdditionalPropsFromMetadata(class, req, nil, false)
		require.NoError(t, err)
		assert.True(t, props.QueryVector)
	})

	t.Run("query_vector=false leaves carrier bit unset", func(t *testing.T) {
		req := &pb.MetadataRequest{QueryVector: false}
		props, err := extractAdditionalPropsFromMetadata(class, req, nil, false)
		require.NoError(t, err)
		assert.False(t, props.QueryVector)
	})
}
