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

package storobj

import (
	"encoding/json"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/byteops"
)

func TestExportFieldsFromBinary(t *testing.T) {
	t.Parallel()

	makeVector := func(n int, scale float32) []float32 {
		v := make([]float32, n)
		for i := range v {
			v[i] = float32(i) * scale
		}
		return v
	}

	tests := []struct {
		name      string
		obj       *models.Object
		vector    []float32
		namedVecs map[string][]float32
		multiVecs map[string][][]float32
	}{
		{
			name: "minimal object (no properties, no vectors)",
			obj: &models.Object{
				ID:                 strfmt.UUID("12345678-1234-1234-1234-123456789012"),
				Class:              "TestClass",
				CreationTimeUnix:   1000,
				LastUpdateTimeUnix: 2000,
			},
		},
		{
			name: "with properties",
			obj: &models.Object{
				ID:    strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
				Class: "Article",
				Properties: map[string]any{
					"name":    "Test Object",
					"count":   float64(42),
					"active":  true,
					"score":   float64(0.95),
					"tags":    []any{"tag1", "tag2"},
					"numbers": []any{float64(1), float64(2), float64(3)},
				},
			},
		},
		{
			name: "with primary vector",
			obj: &models.Object{
				ID:    strfmt.UUID("11111111-2222-3333-4444-555555555555"),
				Class: "VecClass",
			},
			vector: []float32{1.5, 2.5, 3.5, 4.5},
		},
		{
			name: "with named vectors",
			obj: &models.Object{
				ID:    strfmt.UUID("22222222-3333-4444-5555-666666666666"),
				Class: "MultiVecClass",
			},
			namedVecs: map[string][]float32{
				"title":       {0.1, 0.2, 0.3},
				"description": {0.4, 0.5, 0.6},
			},
		},
		{
			name: "with multi-vectors",
			obj: &models.Object{
				ID:    strfmt.UUID("33333333-4444-5555-6666-777777777777"),
				Class: "ColbertClass",
			},
			multiVecs: map[string][][]float32{
				"colbert": {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}},
			},
		},
		{
			name: "all fields populated",
			obj: &models.Object{
				ID:                 strfmt.UUID("44444444-5555-6666-7777-888888888888"),
				Class:              "FullClass",
				CreationTimeUnix:   1700000000000,
				LastUpdateTimeUnix: 1700000001000,
				Properties: map[string]any{
					"title":  "Full Object",
					"active": true,
				},
			},
			vector:    makeVector(128, 0.01),
			namedVecs: map[string][]float32{"semantic": {0.1, 0.2, 0.3, 0.4}},
			multiVecs: map[string][][]float32{"colbert": {{1.0, 2.0}, {3.0, 4.0}}},
		},
		{
			name: "empty properties map",
			obj: &models.Object{
				ID:         strfmt.UUID("55555555-6666-7777-8888-999999999999"),
				Class:      "EmptyProps",
				Properties: map[string]any{},
			},
		},
		{
			name: "nil properties filtered to nil",
			obj: &models.Object{
				ID:                 strfmt.UUID("99999999-aaaa-bbbb-cccc-dddddddddddd"),
				Class:              "NilPropsClass",
				CreationTimeUnix:   5000,
				LastUpdateTimeUnix: 6000,
				// Properties intentionally left nil — MarshalBinary produces
				// JSON "null" which ExportFieldsFromBinary must filter to nil.
			},
		},
		{
			name: "individual nil property values preserved",
			obj: &models.Object{
				ID:    strfmt.UUID("bbbbbbbb-1111-2222-3333-444444444444"),
				Class: "RawNilClass",
				Properties: map[string]any{
					"keep_me":   "hello",
					"delete_me": nil,
					"keep_num":  float64(42),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			obj := &Object{
				MarshallerVersion: 1,
				Object:            *tc.obj,
				Vector:            tc.vector,
				VectorLen:         len(tc.vector),
				Vectors:           tc.namedVecs,
				MultiVectors:      tc.multiVecs,
			}
			data, err := obj.MarshalBinary()
			require.NoError(t, err)

			fields, err := ExportFieldsFromBinary(data)
			require.NoError(t, err)

			assert.Equal(t, string(tc.obj.ID), fields.ID)
			assert.Equal(t, tc.obj.CreationTimeUnix, fields.CreateTime)
			assert.Equal(t, tc.obj.LastUpdateTimeUnix, fields.UpdateTime)

			// Vector bytes
			if tc.vector == nil {
				assert.Nil(t, fields.VectorBytes)
			} else {
				require.NotNil(t, fields.VectorBytes)
				assert.Equal(t, byteops.Fp32SliceToBytes(tc.vector), fields.VectorBytes)
			}

			// Properties
			if tc.obj.Properties == nil {
				assert.Nil(t, fields.Properties)
			} else {
				require.NotNil(t, fields.Properties)
				expected, err := json.Marshal(tc.obj.Properties)
				require.NoError(t, err)
				assert.JSONEq(t, string(expected), string(fields.Properties))
			}

			// Named vectors
			if tc.namedVecs == nil {
				assert.Nil(t, fields.NamedVectors)
			} else {
				require.NotNil(t, fields.NamedVectors)
				var actual map[string][]float32
				require.NoError(t, json.Unmarshal(fields.NamedVectors, &actual))
				assert.Equal(t, tc.namedVecs, actual)
			}

			// Multi-vectors
			if tc.multiVecs == nil {
				assert.Nil(t, fields.MultiVectors)
			} else {
				require.NotNil(t, fields.MultiVectors)
				var actual map[string][][]float32
				require.NoError(t, json.Unmarshal(fields.MultiVectors, &actual))
				assert.Equal(t, tc.multiVecs, actual)
			}
		})
	}
}

func TestExportFieldsFromBinary_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "nil data",
			data:    nil,
			wantErr: "empty binary data",
		},
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: "empty binary data",
		},
		{
			name:    "wrong version",
			data:    []byte{2, 0, 0, 0},
			wantErr: "unsupported binary marshaller version",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ExportFieldsFromBinary(tc.data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func BenchmarkExportFromBinary(b *testing.B) {
	vector := make([]float32, 1536)
	for i := range vector {
		vector[i] = float32(i) * 0.001
	}
	props := map[string]any{
		"name":        "Benchmark Object",
		"description": "An object for benchmarking the export path",
		"count":       float64(42),
		"active":      true,
		"tags":        []any{"tag1", "tag2", "tag3"},
		"score":       float64(0.95),
		"category":    "benchmark",
		"rating":      float64(4.5),
		"metadata":    "some metadata string for benchmarking",
		"notes":       "additional notes about the benchmark object",
	}
	namedVecs := map[string][]float32{
		"title_vector":       make([]float32, 768),
		"description_vector": make([]float32, 768),
	}
	for i := range namedVecs["title_vector"] {
		namedVecs["title_vector"][i] = float32(i) * 0.002
	}
	for i := range namedVecs["description_vector"] {
		namedVecs["description_vector"][i] = float32(i) * 0.003
	}

	obj := &Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID("12345678-1234-1234-1234-123456789012"),
			Class:              "BenchClass",
			CreationTimeUnix:   1700000000000,
			LastUpdateTimeUnix: 1700000001000,
			Properties:         props,
		},
		Vector:    vector,
		VectorLen: len(vector),
		Vectors:   namedVecs,
	}

	data, err := obj.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}

	// New path: ExportFieldsFromBinary (replaces both FromBinary + convertToParquetRow)
	b.Run("ExportFieldsFromBinary", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := ExportFieldsFromBinary(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Old path: FromBinary + the re-serialization work that convertToParquetRow does
	b.Run("FromBinary_plus_reserialization", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			o, err := FromBinary(data)
			if err != nil {
				b.Fatal(err)
			}
			// Simulate convertToParquetRow work:
			_ = o.ID().String()
			_ = o.Class().String()
			_ = o.CreationTimeUnix()
			_ = o.LastUpdateTimeUnix()
			if len(o.Vector) > 0 {
				_ = byteops.Fp32SliceToBytes(o.Vector)
			}
			if len(o.Vectors) > 0 {
				_, _ = json.Marshal(o.Vectors)
			}
			if len(o.MultiVectors) > 0 {
				_, _ = json.Marshal(o.MultiVectors)
			}
			if o.Properties() != nil {
				_, _ = json.Marshal(o.Properties())
			}
		}
	})
}
