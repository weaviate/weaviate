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

package shared

// Baseline microbenchmarks for the internode fan-out marshal/unmarshal hot
// paths (search results, object lists, and the per-object storobj codec).
// These pin the allocs/op and B/op numbers on main so that each optimization
// slice (buffer pooling / streaming decode, binary props encoding, gRPC
// migration) has a before-number to compare against.
//
// Run with:
//
//	go test -run '^$' -bench 'BenchmarkInternode' -benchmem \
//	  ./adapters/handlers/rest/clusterapi/shared/

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

const benchVectorDims = 768

// makeBenchObject builds a storobj.Object that resembles a typical production
// object: ~10 properties including a longer text field, plus a 768-dim vector.
func makeBenchObject(rnd *rand.Rand, i int) *storobj.Object {
	vec := make([]float32, benchVectorDims)
	for j := range vec {
		vec[j] = rnd.Float32()
	}

	longText := make([]byte, 512)
	for j := range longText {
		longText[j] = byte('a' + rnd.Intn(26))
	}

	props := map[string]interface{}{
		"title":       fmt.Sprintf("object-%d-title-with-some-length", i),
		"description": string(longText),
		"category":    "category-" + fmt.Sprint(i%7),
		"count":       float64(i * 3),
		"score":       rnd.Float64(),
		"active":      i%2 == 0,
		"tags":        []interface{}{"alpha", "beta", "gamma"},
		"nested": map[string]interface{}{
			"fieldA": "valueA",
			"fieldB": float64(42),
		},
		"url":        "https://example.com/objects/" + fmt.Sprint(i),
		"identifier": uuid.NewString(),
	}

	obj := storobj.New(uint64(i))
	obj.MarshallerVersion = 1
	obj.Object = models.Object{
		ID:                 strfmt.UUID(uuid.NewString()),
		Class:              "BenchClass",
		CreationTimeUnix:   1700000000000,
		LastUpdateTimeUnix: 1700000001000,
		Properties:         props,
	}
	obj.Vector = vec
	obj.VectorLen = benchVectorDims
	return obj
}

func makeBenchObjects(n int) ([]*storobj.Object, []float32) {
	rnd := rand.New(rand.NewSource(42))
	objs := make([]*storobj.Object, n)
	dists := make([]float32, n)
	for i := range objs {
		objs[i] = makeBenchObject(rnd, i)
		dists[i] = rnd.Float32()
	}
	return objs, dists
}

var benchAddPropsVariants = []struct {
	name     string
	addProps additional.Properties
}{
	{
		// props + vector: worst case, everything on the wire
		name:     "props_and_vector",
		addProps: additional.Properties{Vector: true},
	},
	{
		// props only: the common search fan-out case (vectors not requested)
		name:     "props_only",
		addProps: additional.Properties{},
	},
	{
		// no props + vector: isolates the JSON props blob share
		name:     "noprops_and_vector",
		addProps: additional.Properties{Vector: true, NoProps: true},
	},
}

// BenchmarkInternodeSearchResultsMarshal measures the server-side encode of a
// shard search response (searchResultsPayload.MarshalWithAdditional), the top
// allocation source in the 12-node load-test profile.
func BenchmarkInternodeSearchResultsMarshal(b *testing.B) {
	for _, size := range []int{10, 100} {
		objs, dists := makeBenchObjects(size)
		for _, v := range benchAddPropsVariants {
			b.Run(fmt.Sprintf("objs_%d/%s", size, v.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					out, err := IndicesPayloads.SearchResults.MarshalWithAdditional(objs, dists, v.addProps, nil)
					if err != nil {
						b.Fatal(err)
					}
					if len(out) == 0 {
						b.Fatal("empty payload")
					}
				}
			})
		}
	}
}

// BenchmarkInternodeSearchResultsUnmarshal measures the coordinator-side decode
// of a shard search response (searchResultsPayload.Unmarshal), including the
// per-object FromBinaryNetwork + JSON props decode.
func BenchmarkInternodeSearchResultsUnmarshal(b *testing.B) {
	for _, size := range []int{10, 100} {
		objs, dists := makeBenchObjects(size)
		for _, v := range benchAddPropsVariants {
			payload, err := IndicesPayloads.SearchResults.MarshalWithAdditional(objs, dists, v.addProps, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("objs_%d/%s", size, v.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					outObjs, outDists, _, err := IndicesPayloads.SearchResults.Unmarshal(payload)
					if err != nil {
						b.Fatal(err)
					}
					if len(outObjs) != size || len(outDists) != size {
						b.Fatal("unexpected result size")
					}
				}
			})
		}
	}
}

// BenchmarkInternodeObjectListMarshal measures objectListPayload.Marshal
// (MethodGet), used by MultiGetObjects responses and inside search results.
func BenchmarkInternodeObjectListMarshal(b *testing.B) {
	for _, size := range []int{10, 100} {
		objs, _ := makeBenchObjects(size)
		b.Run(fmt.Sprintf("objs_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out, err := IndicesPayloads.ObjectList.Marshal(objs, MethodGet)
				if err != nil {
					b.Fatal(err)
				}
				if len(out) == 0 {
					b.Fatal("empty payload")
				}
			}
		})
	}
}

// BenchmarkInternodeObjectListUnmarshal measures objectListPayload.Unmarshal
// (MethodGet), the second-largest allocation source in the profile.
func BenchmarkInternodeObjectListUnmarshal(b *testing.B) {
	for _, size := range []int{10, 100} {
		objs, _ := makeBenchObjects(size)
		payload, err := IndicesPayloads.ObjectList.Marshal(objs, MethodGet)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("objs_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out, err := IndicesPayloads.ObjectList.Unmarshal(payload, MethodGet)
				if err != nil {
					b.Fatal(err)
				}
				if len(out) != size {
					b.Fatal("unexpected result size")
				}
			}
		})
	}
}

// BenchmarkInternodeStorobjMarshal measures the per-object binary encode
// (storobj MarshalBinaryOptional). The noprops variants isolate the cost of
// the JSON-encoded props blob inside the otherwise-binary format.
func BenchmarkInternodeStorobjMarshal(b *testing.B) {
	objs, _ := makeBenchObjects(1)
	obj := objs[0]
	for _, v := range benchAddPropsVariants {
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out, err := obj.MarshalBinaryOptional(v.addProps)
				if err != nil {
					b.Fatal(err)
				}
				if len(out) == 0 {
					b.Fatal("empty payload")
				}
			}
		})
	}
}

// BenchmarkInternodeStorobjUnmarshal measures the per-object binary decode
// (storobj.FromBinaryNetwork), including json.Unmarshal of the props blob and
// enrichSchemaTypes.
func BenchmarkInternodeStorobjUnmarshal(b *testing.B) {
	objs, _ := makeBenchObjects(1)
	obj := objs[0]
	for _, v := range benchAddPropsVariants {
		payload, err := obj.MarshalBinaryOptional(v.addProps)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out, err := storobj.FromBinaryNetwork(payload)
				if err != nil {
					b.Fatal(err)
				}
				if out.DocID != obj.DocID {
					b.Fatal("docID mismatch")
				}
			}
		})
	}
}

// chunkedReader yields data in fixed-size chunks without exposing a length,
// mimicking an HTTP response body so io.ReadAll must grow its buffer.
type chunkedReader struct {
	data []byte
	pos  int
	step int
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	n := c.step
	if n > len(p) {
		n = len(p)
	}
	if c.pos+n > len(c.data) {
		n = len(c.data) - c.pos
	}
	copy(p, c.data[c.pos:c.pos+n])
	c.pos += n
	return n, nil
}

// BenchmarkInternodeReadAllBody measures the io.ReadAll full-body buffering
// pattern used across the remote_index client sites, on a payload the size of
// a typical 100-object search response.
func BenchmarkInternodeReadAllBody(b *testing.B) {
	objs, dists := makeBenchObjects(100)
	payload, err := IndicesPayloads.SearchResults.MarshalWithAdditional(objs, dists, additional.Properties{Vector: true}, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Run(fmt.Sprintf("payload_%dKB", len(payload)/1024), func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			body, err := io.ReadAll(&chunkedReader{data: payload, step: 32 * 1024})
			if err != nil {
				b.Fatal(err)
			}
			if len(body) != len(payload) {
				b.Fatal("short read")
			}
		}
	})
	// contrast: the same read when the length is known upfront (what a
	// content-length-aware or pooled-buffer implementation could do)
	b.Run("preallocated_readfull", func(b *testing.B) {
		b.ReportAllocs()
		buf := make([]byte, len(payload))
		for i := 0; i < b.N; i++ {
			r := bytes.NewReader(payload)
			if _, err := io.ReadFull(r, buf); err != nil {
				b.Fatal(err)
			}
		}
	})
}
