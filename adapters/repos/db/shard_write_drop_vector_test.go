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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestDropVectorIndex_IsDropped(t *testing.T) {
	none := modelsext.VectorIndexTypeNone

	classWith := func(cfg map[string]models.VectorConfig) *models.Class {
		return &models.Class{Class: "TestClass", VectorConfig: cfg}
	}

	tests := []struct {
		name         string
		class        *models.Class
		targetVector string
		want         bool
	}{
		{name: "nil class", class: nil, targetVector: "foo", want: false},
		{name: "legacy vector empty target is never dropped", class: classWith(map[string]models.VectorConfig{"": {VectorIndexType: none}}), targetVector: "", want: false},
		{name: "target absent from config", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}}), targetVector: "bar", want: false},
		{name: "active hnsw", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}}), targetVector: "foo", want: false},
		{name: "active flat", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "flat"}}), targetVector: "foo", want: false},
		{name: "active dynamic", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "dynamic"}}), targetVector: "foo", want: false},
		{name: "dropped", class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}), targetVector: "foo", want: true},
		{name: "empty config map", class: classWith(map[string]models.VectorConfig{}), targetVector: "foo", want: false},
		{name: "nil config map", class: &models.Class{Class: "TestClass"}, targetVector: "foo", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDroppedVectorIndex(tt.class, tt.targetVector))
		})
	}
}

func TestDropVectorIndex_RejectObjectVectors(t *testing.T) {
	none := modelsext.VectorIndexTypeNone

	classWith := func(cfg map[string]models.VectorConfig) *models.Class {
		return &models.Class{Class: "TestClass", VectorConfig: cfg}
	}

	objWith := func(vectors map[string][]float32, multi map[string][][]float32) *storobj.Object {
		return &storobj.Object{Object: models.Object{Class: "TestClass"}, Vectors: vectors, MultiVectors: multi}
	}

	tests := []struct {
		name       string
		class      *models.Class
		object     *storobj.Object
		wantErr    bool
		wantTarget string
	}{
		{
			name:   "no vectors",
			class:  classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			object: objWith(nil, nil),
		},
		{
			name:   "active single vector",
			class:  classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}}),
			object: objWith(map[string][]float32{"foo": {1, 2, 3}}, nil),
		},
		{
			name:       "dropped single vector",
			class:      classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			object:     objWith(map[string][]float32{"foo": {1, 2, 3}}, nil),
			wantErr:    true,
			wantTarget: "foo",
		},
		{
			name:       "dropped multi vector",
			class:      classWith(map[string]models.VectorConfig{"bar": {VectorIndexType: none}}),
			object:     objWith(nil, map[string][][]float32{"bar": {{1, 2}, {3, 4}}}),
			wantErr:    true,
			wantTarget: "bar",
		},
		{
			name:  "mixed - one active one dropped",
			class: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}, "baz": {VectorIndexType: none}}),
			// only the dropped one must trigger; assert the target appears in the error
			object:     objWith(map[string][]float32{"foo": {1}, "baz": {2}}, nil),
			wantErr:    true,
			wantTarget: "baz",
		},
		{
			name:   "all active across vectors and multivectors",
			class:  classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: "hnsw"}, "bar": {VectorIndexType: "hnsw"}}),
			object: objWith(map[string][]float32{"foo": {1}}, map[string][][]float32{"bar": {{1}}}),
		},
		{
			name:   "nil class falls back to allow (existing !ok branch handles it)",
			class:  nil,
			object: objWith(map[string][]float32{"foo": {1}}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectDroppedObjectVectors(tt.class, tt.object)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "vector index not found")
			assert.Contains(t, err.Error(), tt.wantTarget)
		})
	}
}

func TestDropVectorTransformerBuilder(t *testing.T) {
	const className = "TestClass"

	marshal := func(t *testing.T, vecs map[string][]float32, multi map[string][][]float32) []byte {
		t.Helper()
		obj := storobj.FromObject(&models.Object{
			Class:      className,
			ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Properties: map[string]interface{}{"name": "x"},
		}, nil, vecs, multi)
		obj.DocID = 1
		b, err := obj.MarshalBinaryDisk(false)
		require.NoError(t, err)
		return b
	}
	decode := func(t *testing.T, b []byte) *storobj.Object {
		t.Helper()
		obj, err := storobj.FromBinaryDisk(b, className)
		require.NoError(t, err)
		return obj
	}
	op := func(opType lsmkv.OpType, targets ...string) lsmkv.ActiveOp {
		return lsmkv.ActiveOp{ID: "op", Descriptor: lsmkv.OpDescriptor{
			Type: opType, Targets: targets, CreatedAt: 1,
		}}
	}

	transform := func(t *testing.T, in []byte, ops ...lsmkv.ActiveOp) []byte {
		t.Helper()
		fn := dropVectorTransformerBuilder(className, false)(ops)
		out, err := fn(in)
		require.NoError(t, err)
		return out
	}

	t.Run("strips a single named vector, keeps the rest", func(t *testing.T) {
		in := marshal(t, map[string][]float32{"keep": {1, 2}, "drop": {3, 4}}, nil)
		out := transform(t, in, op(lsmkv.OpTypeRemoveTargetVectors, "drop"))
		got := decode(t, out)
		require.Contains(t, got.Vectors, "keep")
		require.NotContains(t, got.Vectors, "drop")
		require.Equal(t, []float32{1, 2}, got.Vectors["keep"])
	})

	t.Run("strips multiple targets across vectors and multi-vectors (C3)", func(t *testing.T) {
		in := marshal(t,
			map[string][]float32{"keep": {1, 2}, "drop": {3, 4}},
			map[string][][]float32{"mkeep": {{5, 6}}, "mdrop": {{7, 8}}})
		out := transform(t, in, op(lsmkv.OpTypeRemoveTargetVectors, "drop", "mdrop"))
		got := decode(t, out)
		require.Contains(t, got.Vectors, "keep")
		require.NotContains(t, got.Vectors, "drop")
		require.Contains(t, got.MultiVectors, "mkeep")
		require.NotContains(t, got.MultiVectors, "mdrop")
	})

	t.Run("absent target is an identity no-op (same bytes)", func(t *testing.T) {
		in := marshal(t, map[string][]float32{"keep": {1, 2}}, nil)
		out := transform(t, in, op(lsmkv.OpTypeRemoveTargetVectors, "missing"))
		require.Equal(t, in, out, "unchanged object must return the original bytes unmodified")
	})

	t.Run("non-matching op type is ignored", func(t *testing.T) {
		in := marshal(t, map[string][]float32{"drop": {3, 4}}, nil)
		out := transform(t, in, op("some_other_op", "drop"))
		require.Equal(t, in, out)
		require.Contains(t, decode(t, out).Vectors, "drop")
	})

	t.Run("idempotent under repeated application (C2)", func(t *testing.T) {
		in := marshal(t, map[string][]float32{"keep": {1, 2}, "drop": {3, 4}}, nil)
		once := transform(t, in, op(lsmkv.OpTypeRemoveTargetVectors, "drop"))
		twice := transform(t, once, op(lsmkv.OpTypeRemoveTargetVectors, "drop"))
		require.Equal(t, once, twice, "re-running the drop on already-clean bytes must be a no-op")
	})

	t.Run("composition is order-independent (C2)", func(t *testing.T) {
		in := marshal(t, map[string][]float32{"a": {1}, "b": {2}, "c": {3}}, nil)
		forward := transform(t, in,
			op(lsmkv.OpTypeRemoveTargetVectors, "a"), op(lsmkv.OpTypeRemoveTargetVectors, "b"))
		reverse := transform(t, in,
			op(lsmkv.OpTypeRemoveTargetVectors, "b"), op(lsmkv.OpTypeRemoveTargetVectors, "a"))
		require.Equal(t, forward, reverse, "drop order must not affect the result")
		got := decode(t, forward)
		require.NotContains(t, got.Vectors, "a")
		require.NotContains(t, got.Vectors, "b")
		require.Contains(t, got.Vectors, "c")
	})
}

// TestDropVectorTransformer_CodecSymmetry pins the load-bearing correctness claim
// behind the changed path: the transformer is a decode→strip→re-marshal round
// trip, so every field it does NOT touch must survive FromBinaryDisk +
// MarshalBinaryDisk unchanged. A future on-disk storobj field wired into one half
// of the codec but not the other would silently drop data during compaction; this
// test fails the moment that asymmetry appears.
func TestDropVectorTransformer_CodecSymmetry(t *testing.T) {
	const className = "TestClass"

	build := func(t *testing.T, vecs map[string][]float32, multi map[string][][]float32) *storobj.Object {
		t.Helper()
		o := storobj.FromObject(&models.Object{
			Class:              className,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			Properties: map[string]interface{}{
				"name":    "MyName",
				"count":   float64(17),
				"active":  true,
				"tags":    []interface{}{"a", "b", "c"},
				"numbers": []interface{}{float64(1), float64(2), float64(3)},
			},
		}, []float32{0.1, 0.2, 0.3}, vecs, multi)
		o.DocID = 42
		return o
	}
	marshalDisk := func(t *testing.T, o *storobj.Object) []byte {
		t.Helper()
		b, err := o.MarshalBinaryDisk(false)
		require.NoError(t, err)
		return b
	}
	decodeDisk := func(t *testing.T, b []byte) *storobj.Object {
		t.Helper()
		o, err := storobj.FromBinaryDisk(b, className)
		require.NoError(t, err)
		return o
	}
	strip := func(t *testing.T, in []byte, targets ...string) []byte {
		t.Helper()
		fn := dropVectorTransformerBuilder(className, false)([]lsmkv.ActiveOp{{
			ID: "op", Descriptor: lsmkv.OpDescriptor{
				Type: lsmkv.OpTypeRemoveTargetVectors, Targets: targets, CreatedAt: 1,
			},
		}})
		out, err := fn(in)
		require.NoError(t, err)
		return out
	}

	t.Run("every untouched field survives across legacy/named/multi vectors and rich props", func(t *testing.T) {
		in := marshalDisk(t, build(t,
			map[string][]float32{"keep": {1, 2}, "drop": {3, 4}},
			map[string][][]float32{"mkeep": {{5, 6}, {7, 8}}}))
		out := strip(t, in, "drop")
		require.Less(t, len(out), len(in), "dropping a vector must shrink the payload")

		// Compare the decoded result against the same logical object re-encoded
		// without "drop": maps compare order-independently, so this isolates field
		// fidelity from map-iteration nondeterminism in the encoder.
		want := decodeDisk(t, marshalDisk(t, build(t,
			map[string][]float32{"keep": {1, 2}},
			map[string][][]float32{"mkeep": {{5, 6}, {7, 8}}})))
		require.Equal(t, want, decodeDisk(t, out))
	})

	t.Run("byte-identical to reference when a single named vector remains", func(t *testing.T) {
		// With exactly one surviving named vector and no multi-vectors the encoder's
		// offsets map has a single entry, so the output is deterministic and we can
		// assert true byte-stability (not just field equality) of the re-marshal.
		in := marshalDisk(t, build(t, map[string][]float32{"keep": {1, 2}, "drop": {3, 4}}, nil))
		out := strip(t, in, "drop")
		want := marshalDisk(t, build(t, map[string][]float32{"keep": {1, 2}}, nil))
		require.Equal(t, want, out, "re-marshaled bytes must match the reference object encoded without the dropped vector")
	})
}

// TestDropVectorTransformer_CorruptBytesSurfacesError exercises the decode-error
// branch (return nil, fmt.Errorf("decode object for vector drop: %w", err)): a
// corrupt segment value must surface a wrapped error, never silently pass through
// or panic the compaction/cleanup pass.
func TestDropVectorTransformer_CorruptBytesSurfacesError(t *testing.T) {
	const className = "TestClass"

	valid := func(t *testing.T) []byte {
		t.Helper()
		obj := storobj.FromObject(&models.Object{
			Class: className,
			ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
		}, nil, map[string][]float32{"drop": {1, 2}}, nil)
		obj.DocID = 1
		b, err := obj.MarshalBinaryDisk(false)
		require.NoError(t, err)
		return b
	}

	cases := []struct {
		name string
		in   func(t *testing.T) []byte
	}{
		{"unsupported version byte (bit-flipped header)", func(t *testing.T) []byte {
			b := valid(t)
			b[0] = 0xFF
			return b
		}},
		{"non-v1 single byte", func(t *testing.T) []byte { return []byte{0x02} }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fn := dropVectorTransformerBuilder(className, false)([]lsmkv.ActiveOp{{
				ID: "op", Descriptor: lsmkv.OpDescriptor{
					Type: lsmkv.OpTypeRemoveTargetVectors, Targets: []string{"drop"}, CreatedAt: 1,
				},
			}})
			_, err := fn(tc.in(t))
			require.Error(t, err)
			require.ErrorContains(t, err, "decode object for vector drop")
		})
	}
}
