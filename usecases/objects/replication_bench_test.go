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

package objects

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// benchPropagationObject builds a representative object + its on-disk binary.
func benchPropagationObject(tb testing.TB) (*models.Object, []float32, []byte) {
	tb.Helper()

	vector := make([]float32, 768)
	for i := range vector {
		vector[i] = float32(i) * 0.001
	}

	props := map[string]interface{}{
		"title":    "the quick brown fox jumps over the lazy dog",
		"body":     "lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore",
		"category": "benchmarks",
		"count":    float64(42),
		"score":    float64(3.14159),
	}

	obj := &models.Object{
		ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
		Class:              "Paragraph",
		CreationTimeUnix:   1700000000000,
		LastUpdateTimeUnix: 1700000000123,
		Properties:         props,
	}

	so := storobj.FromObject(obj, vector, nil, nil)
	so.DocID = 12345
	diskBytes, err := so.MarshalBinary()
	if err != nil {
		tb.Fatalf("marshal disk bytes: %v", err)
	}
	return obj, vector, diskBytes
}

// BenchmarkPropagationSourceJSON: decode on-disk object, wrap, JSON-marshal.
func BenchmarkPropagationSourceJSON(b *testing.B) {
	_, _, diskBytes := benchPropagationObject(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		so, err := storobj.FromBinaryNetwork(diskBytes)
		if err != nil {
			b.Fatal(err)
		}
		vo := &VObject{
			ID:                      so.ID(),
			LastUpdateTimeUnixMilli: so.LastUpdateTimeUnix(),
			LatestObject:            &so.Object,
			Vector:                  so.Vector,
			StaleUpdateTime:         1700000000000,
		}
		if _, err := vo.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPropagationSourceRaw: copy the on-disk bytes and frame them.
func BenchmarkPropagationSourceRaw(b *testing.B) {
	_, _, diskBytes := benchPropagationObject(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp := make([]byte, len(diskBytes))
		copy(cp, diskBytes)
		vo := &VObject{StaleUpdateTime: 1700000000000, RawBytes: cp}
		if _, err := vo.MarshalBinaryRaw(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPropagationTargetJSON: JSON-decode the VObject, re-marshal to disk.
func BenchmarkPropagationTargetJSON(b *testing.B) {
	obj, vector, _ := benchPropagationObject(b)
	vo := &VObject{
		ID:                      obj.ID,
		LastUpdateTimeUnixMilli: obj.LastUpdateTimeUnix,
		LatestObject:            obj,
		Vector:                  vector,
		StaleUpdateTime:         1700000000000,
	}
	wire, err := vo.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded VObject
		if err := decoded.UnmarshalBinary(wire); err != nil {
			b.Fatal(err)
		}
		so := storobj.FromObject(decoded.LatestObject, decoded.Vector, decoded.Vectors, decoded.MultiVectors)
		if _, err := so.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPropagationTargetRaw: parse once for indexing; bytes written verbatim.
func BenchmarkPropagationTargetRaw(b *testing.B) {
	_, _, diskBytes := benchPropagationObject(b)
	vo := &VObject{StaleUpdateTime: 1700000000000, RawBytes: diskBytes}
	wire, err := vo.MarshalBinaryRaw()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded VObject
		if err := decoded.UnmarshalBinaryRaw(wire); err != nil {
			b.Fatal(err)
		}
		if _, err := storobj.FromBinaryNetwork(decoded.RawBytes); err != nil {
			b.Fatal(err)
		}
	}
}
