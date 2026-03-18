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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestVObjectMarshalBinaryV2RoundTrip(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		name string
		in   VObject
	}{
		{
			name: "deleted object with ID only",
			in: VObject{
				ID:                      strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
				Deleted:                 true,
				StaleUpdateTime:         now.UnixMilli(),
				LastUpdateTimeUnixMilli: now.Add(time.Hour).UnixMilli(),
				Version:                 7,
			},
		},
		{
			name: "object with primary vector",
			in: VObject{
				LatestObject: &models.Object{
					ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242"),
					Class:              "C1",
					CreationTimeUnix:   now.UnixMilli(),
					LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(),
					Properties: map[string]interface{}{
						"text": "hello world",
					},
				},
				Vector:          []float32{0.1, 0.2, 0.3, 0.4, 0.5},
				StaleUpdateTime: now.UnixMilli(),
				Version:         3,
			},
		},
		{
			name: "object with named vectors",
			in: VObject{
				LatestObject: &models.Object{
					ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168243"),
					Class: "C2",
				},
				Vectors: map[string][]float32{
					"text":  {0.1, 0.2, 0.3},
					"image": {0.9, 0.8, 0.7, 0.6},
				},
				StaleUpdateTime: now.UnixMilli(),
			},
		},
		{
			name: "object with multi-vectors",
			in: VObject{
				LatestObject: &models.Object{
					ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168244"),
					Class: "C3",
				},
				MultiVectors: map[string][][]float32{
					"colbert": {{0.1, 0.2}, {0.3, 0.4}, {0.5, 0.6}},
				},
				StaleUpdateTime: now.UnixMilli(),
			},
		},
		{
			name: "object with all vector types",
			in: VObject{
				LatestObject: &models.Object{
					ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168245"),
					Class: "C4",
				},
				Vector:  []float32{1, 2, 3},
				Vectors: map[string][]float32{"named": {4, 5, 6}},
				MultiVectors: map[string][][]float32{
					"multi": {{7, 8}, {9, 10}},
				},
				StaleUpdateTime:         now.UnixMilli(),
				LastUpdateTimeUnixMilli: now.Add(2 * time.Hour).UnixMilli(),
				Version:                 42,
			},
		},
		{
			name: "empty object (no vector, no latest object)",
			in: VObject{
				ID:              strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168246"),
				Deleted:         true,
				StaleUpdateTime: now.UnixMilli(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.in.MarshalBinaryV2()
			require.NoError(t, err)

			var got VObject
			require.NoError(t, got.UnmarshalBinaryV2(data))

			assert.Equal(t, tc.in.Deleted, got.Deleted)
			assert.Equal(t, tc.in.StaleUpdateTime, got.StaleUpdateTime)
			assert.Equal(t, tc.in.LastUpdateTimeUnixMilli, got.LastUpdateTimeUnixMilli)
			assert.Equal(t, tc.in.Version, got.Version)
			assert.Equal(t, tc.in.Vector, got.Vector)
			assert.Equal(t, tc.in.Vectors, got.Vectors)
			assert.Equal(t, tc.in.MultiVectors, got.MultiVectors)

			if tc.in.ID != "" {
				assert.Equal(t, tc.in.ID, got.ID)
			}
			if tc.in.LatestObject != nil {
				require.NotNil(t, got.LatestObject)
				assert.Equal(t, tc.in.LatestObject.ID, got.LatestObject.ID)
				assert.Equal(t, tc.in.LatestObject.Class, got.LatestObject.Class)
			} else {
				assert.Nil(t, got.LatestObject)
			}
		})
	}
}

// TestVObjectMarshalBinaryV2Golden verifies the exact wire layout produced by
// MarshalBinaryV2. It builds the expected bytes independently using the standard
// library (encoding/binary, uuid.MarshalBinary) so that any field-ordering, field-
// width, or endianness bug in the production helpers is caught — something a
// symmetric round-trip test cannot detect.
func TestVObjectMarshalBinaryV2Golden(t *testing.T) {
	vo := VObject{
		Deleted:                 true,
		StaleUpdateTime:         1000,
		LastUpdateTimeUnixMilli: 2000,
		Version:                 3,
		ID:                      strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
		Vector:                  []float32{1.0, -1.0},
		// No Vectors, MultiVectors, or LatestObject — keeps the golden value minimal.
	}

	got, err := vo.MarshalBinaryV2()
	require.NoError(t, err)

	// Build the expected bytes independently of the production helpers.
	var want bytes.Buffer
	want.WriteByte(0x01)                                  // deleted = true
	binary.Write(&want, binary.LittleEndian, int64(1000)) // StaleUpdateTime
	binary.Write(&want, binary.LittleEndian, int64(2000)) // LastUpdateTimeUnixMilli
	binary.Write(&want, binary.LittleEndian, uint64(3))   // Version
	uid := uuid.MustParse("73f2eb5f-5abf-447a-81ca-74b1dd168241")
	uuidBytes, _ := uid.MarshalBinary()
	want.Write(uuidBytes)                                            // UUID (16 bytes)
	binary.Write(&want, binary.LittleEndian, uint32(2))              // primary vector length
	binary.Write(&want, binary.LittleEndian, math.Float32bits(1.0))  // float32(1.0)
	binary.Write(&want, binary.LittleEndian, math.Float32bits(-1.0)) // float32(-1.0)
	binary.Write(&want, binary.LittleEndian, uint32(0))              // named vectors count
	binary.Write(&want, binary.LittleEndian, uint32(0))              // multi-vectors count
	binary.Write(&want, binary.LittleEndian, uint32(0))              // object length

	// 1 + 8 + 8 + 8 + 16 + 4 + 4 + 4 + 4 + 4 + 4 = 65 bytes
	assert.Len(t, got, 65)
	assert.Equal(t, want.Bytes(), got)
}

// TestVObjectMarshalBinaryV2SpecialFloats verifies that IEEE 754 special values
// (+Inf, -Inf, NaN, -0.0, subnormals) are preserved as exact bit patterns through
// a V2 round-trip. assert.Equal cannot be used for NaN (NaN != NaN in Go), so
// comparisons use math.Float32bits.
func TestVObjectMarshalBinaryV2SpecialFloats(t *testing.T) {
	special := []float32{
		float32(math.Inf(1)),             // +Inf
		float32(math.Inf(-1)),            // -Inf
		math.Float32frombits(0x7FC00000), // canonical quiet NaN
		math.Float32frombits(0x80000000), // -0.0
		math.MaxFloat32,
		-math.MaxFloat32,
		math.Float32frombits(1), // smallest positive subnormal
	}

	vo := VObject{
		ID:     strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
		Vector: special,
	}

	data, err := vo.MarshalBinaryV2()
	require.NoError(t, err)

	var got VObject
	require.NoError(t, got.UnmarshalBinaryV2(data))

	require.Len(t, got.Vector, len(special))
	for i := range special {
		assert.Equalf(t,
			math.Float32bits(special[i]),
			math.Float32bits(got.Vector[i]),
			"bit pattern mismatch at index %d (input bits 0x%08X)", i, math.Float32bits(special[i]),
		)
	}
}

// TestVObjectMarshalBinaryV2EdgeCasesRoundTrip covers value-range edge cases not
// exercised by the main round-trip table: negative timestamps, integer boundaries,
// Deleted=true alongside a populated LatestObject, and a live object with no ID.
func TestVObjectMarshalBinaryV2EdgeCasesRoundTrip(t *testing.T) {
	testCases := []struct {
		name string
		in   VObject
	}{
		{
			name: "deleted=true with latest object present",
			in: VObject{
				ID:      strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
				Deleted: true,
				LatestObject: &models.Object{
					ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
					Class: "Article",
				},
				Version: 1,
			},
		},
		{
			name: "negative timestamps",
			in: VObject{
				ID:                      strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242"),
				StaleUpdateTime:         -1000,
				LastUpdateTimeUnixMilli: math.MinInt64,
				Version:                 1,
			},
		},
		{
			name: "max version and max timestamps",
			in: VObject{
				ID:                      strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168243"),
				StaleUpdateTime:         math.MaxInt64,
				LastUpdateTimeUnixMilli: math.MaxInt64,
				Version:                 math.MaxUint64,
			},
		},
		{
			name: "empty uuid with non-deleted object",
			in: VObject{
				// ID intentionally absent; marshal writes 16 zero bytes, unmarshal
				// leaves ID as "".
				LatestObject: &models.Object{
					ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168244"),
					Class: "Article",
				},
				Version: 2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.in.MarshalBinaryV2()
			require.NoError(t, err)

			var got VObject
			require.NoError(t, got.UnmarshalBinaryV2(data))

			assert.Equal(t, tc.in.Deleted, got.Deleted)
			assert.Equal(t, tc.in.StaleUpdateTime, got.StaleUpdateTime)
			assert.Equal(t, tc.in.LastUpdateTimeUnixMilli, got.LastUpdateTimeUnixMilli)
			assert.Equal(t, tc.in.Version, got.Version)
			if tc.in.ID != "" {
				assert.Equal(t, tc.in.ID, got.ID)
			} else {
				assert.Empty(t, got.ID)
			}
			if tc.in.LatestObject != nil {
				require.NotNil(t, got.LatestObject)
				assert.Equal(t, tc.in.LatestObject.ID, got.LatestObject.ID)
				assert.Equal(t, tc.in.LatestObject.Class, got.LatestObject.Class)
			} else {
				assert.Nil(t, got.LatestObject)
			}
		})
	}
}

func TestMarshalBinaryV2JaggedMultiVector(t *testing.T) {
	testCases := []struct {
		name         string
		multiVectors map[string][][]float32
	}{
		{
			name: "second vector shorter than first",
			multiVectors: map[string][][]float32{
				"colbert": {{1, 2, 3}, {4, 5}},
			},
		},
		{
			name: "second vector longer than first",
			multiVectors: map[string][][]float32{
				"colbert": {{1, 2}, {3, 4, 5}},
			},
		},
		{
			name: "third vector differs",
			multiVectors: map[string][][]float32{
				"colbert": {{1, 2}, {3, 4}, {5, 6, 7}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vo := VObject{MultiVectors: tc.multiVectors}
			_, err := vo.MarshalBinaryV2()
			assert.Error(t, err, "jagged multi-vector must be rejected at marshal time")
		})
	}
}

func TestUnmarshalBinaryV2ReusedInstance(t *testing.T) {
	rich := VObject{
		ID:      strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
		Vector:  []float32{1, 2, 3},
		Vectors: map[string][]float32{"n": {4, 5}},
		MultiVectors: map[string][][]float32{
			"m": {{6, 7}, {8, 9}},
		},
		LatestObject: &models.Object{ID: strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"), Class: "C1"},
	}
	richData, err := rich.MarshalBinaryV2()
	require.NoError(t, err)

	sparse := VObject{Deleted: true, Version: 7}
	sparseData, err := sparse.MarshalBinaryV2()
	require.NoError(t, err)

	// Decode rich into the instance first, then overwrite with sparse.
	var vo VObject
	require.NoError(t, vo.UnmarshalBinaryV2(richData))
	require.NoError(t, vo.UnmarshalBinaryV2(sparseData))

	// No field from the rich decode should survive.
	assert.Equal(t, strfmt.UUID(""), vo.ID)
	assert.Nil(t, vo.Vector)
	assert.Nil(t, vo.Vectors)
	assert.Nil(t, vo.MultiVectors)
	assert.Nil(t, vo.LatestObject)
	assert.True(t, vo.Deleted)
	assert.Equal(t, uint64(7), vo.Version)
}

func TestUnmarshalBinaryV2TruncatedInput(t *testing.T) {
	// Build a valid payload to truncate.
	original := VObject{
		ID:                      strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
		Deleted:                 false,
		StaleUpdateTime:         1000,
		LastUpdateTimeUnixMilli: 2000,
		Version:                 3,
		Vector:                  []float32{1.0, 2.0, 3.0},
		Vectors:                 map[string][]float32{"n": {4.0, 5.0}},
		MultiVectors:            map[string][][]float32{"m": {{6.0, 7.0}, {8.0, 9.0}}},
		LatestObject: &models.Object{
			ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
			Class: "C1",
		},
	}

	full, err := original.MarshalBinaryV2()
	require.NoError(t, err)

	// Every prefix shorter than the full payload must return an error, not panic.
	for truncAt := 0; truncAt < len(full); truncAt++ {
		t.Run(fmt.Sprintf("truncated_at_%d", truncAt), func(t *testing.T) {
			var got VObject
			err := got.UnmarshalBinaryV2(full[:truncAt])
			assert.Error(t, err, "expected error for truncated input at byte %d", truncAt)
		})
	}
}

func TestUnmarshalBinaryV2MalformedInput(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		var got VObject
		assert.Error(t, got.UnmarshalBinaryV2([]byte{}))
	})

	t.Run("oversized primary vector length", func(t *testing.T) {
		// Header (41 bytes) + vecLen claiming 1 000 000 floats (4B) but no actual data.
		buf := make([]byte, 41+4)
		// vecLen = 1_000_000, but no float data follows
		buf[41] = 0x40
		buf[42] = 0x42
		buf[43] = 0x0f
		buf[44] = 0x00
		var got VObject
		assert.Error(t, got.UnmarshalBinaryV2(buf))
	})

	t.Run("oversized named vector name length", func(t *testing.T) {
		// Valid header + empty primary vector + namedCount=1 + name length claiming 65535 bytes
		// but nothing follows.
		base := VObject{}
		data, err := base.MarshalBinaryV2()
		require.NoError(t, err)

		// data ends with: [4B vecLen=0][4B namedCount=0][4B multiCount=0][4B objLen=0]
		// Patch namedCount to 1 and append a 2-byte name length of 0xFFFF with no name body.
		// Find offset of namedCount: headerSize(41) + vecLenField(4) = 45
		patched := make([]byte, len(data))
		copy(patched, data)
		// set namedCount = 1 at offset 45
		patched[45] = 1
		patched[46] = 0
		patched[47] = 0
		patched[48] = 0
		// append 2 bytes for name length = 0xFFFF
		patched = append(patched[:49], 0xFF, 0xFF)
		var got VObject
		assert.Error(t, got.UnmarshalBinaryV2(patched))
	})
}

// TestVObjectMarshalBinaryV2LatestObjectRoundTrip verifies that all fields of
// models.Object survive a full VObject V2 serialization round-trip.
// models.Object.MarshalBinary uses JSON internally, so property values must be
// JSON-compatible types (numbers as float64, strings, bools, nil).
func TestVObjectMarshalBinaryV2LatestObjectRoundTrip(t *testing.T) {
	now := time.Now()
	nowMilli := now.UnixMilli()

	type testCase struct {
		name string
		obj  models.Object
		vobj VObject // extra VObject-level fields beyond LatestObject
	}

	testCases := []testCase{
		{
			name: "all scalar property types",
			obj: models.Object{
				ID:                 strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee01"),
				Class:              "Article",
				CreationTimeUnix:   nowMilli,
				LastUpdateTimeUnix: nowMilli + 1000,
				Tenant:             "tenant-1",
				Properties: map[string]interface{}{
					"title":    "hello world",
					"count":    float64(42),
					"score":    float64(3.14),
					"active":   true,
					"inactive": false,
					"nothing":  nil,
				},
			},
		},
		{
			name: "date and geo property types",
			obj: models.Object{
				ID:    strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee06"),
				Class: "Place",
				Properties: map[string]interface{}{
					"openedAt": "2024-01-15T10:30:00Z",
					"location": map[string]interface{}{
						"latitude":  float64(52.3676),
						"longitude": float64(4.9041),
					},
				},
			},
		},
		{
			name: "cross-reference property",
			obj: models.Object{
				ID:    strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee07"),
				Class: "Article",
				Properties: map[string]interface{}{
					"hasAuthors": []interface{}{
						map[string]interface{}{"beacon": "weaviate://localhost/Author/11111111-1111-1111-1111-111111111111"},
						map[string]interface{}{"beacon": "weaviate://localhost/Author/22222222-2222-2222-2222-222222222222"},
					},
				},
			},
		},
		{
			name: "number and bool array properties",
			obj: models.Object{
				ID:    strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee08"),
				Class: "Article",
				Properties: map[string]interface{}{
					"scores": []interface{}{float64(1.1), float64(2.2), float64(3.3)},
					"flags":  []interface{}{true, false, true},
				},
			},
		},
		{
			name: "nested property map",
			obj: models.Object{
				ID:    strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee02"),
				Class: "Article",
				Properties: map[string]interface{}{
					"meta": map[string]interface{}{
						"author": "alice",
						"views":  float64(100),
					},
				},
			},
		},
		{
			name: "array property",
			obj: models.Object{
				ID:    strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee03"),
				Class: "Article",
				Properties: map[string]interface{}{
					"tags": []interface{}{"go", "weaviate", "vector"},
				},
			},
		},
		{
			name: "tenant and timestamps, no properties",
			obj: models.Object{
				ID:                 strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee04"),
				Class:              "Article",
				CreationTimeUnix:   nowMilli,
				LastUpdateTimeUnix: nowMilli + 5000,
				Tenant:             "tenant-abc",
			},
		},
		{
			name: "empty properties map",
			obj: models.Object{
				ID:         strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee05"),
				Class:      "Article",
				Properties: map[string]interface{}{},
			},
		},
		{
			name: "object-level vector fields",
			obj: models.Object{
				ID:    strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee09"),
				Class: "Article",
				// C11yVector: the legacy primary vector embedded in the object model itself.
				Vector: models.C11yVector{0.1, 0.2, 0.3},
				// Named vectors embedded in the object model (distinct from VObject.Vectors).
				Vectors: models.Vectors{
					"text":  []float32{0.4, 0.5},
					"image": []float32{0.6, 0.7, 0.8},
				},
				VectorWeights: map[string]interface{}{
					"text":  float64(0.7),
					"image": float64(0.3),
				},
			},
		},
		{
			name: "additional properties",
			obj: models.Object{
				ID:    strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee10"),
				Class: "Article",
				Additional: models.AdditionalProperties{
					"score":        float64(0.98),
					"distance":     float64(0.12),
					"certainty":    float64(0.88),
					"explainScore": "(bm25)",
					"featureProjection": map[string]interface{}{
						"vector": []interface{}{float64(1), float64(2)},
					},
				},
			},
		},
		{
			// Exercises framing correctness: a fully-populated LatestObject combined with
			// all three VObject vector types ensures no off-by-one in the binary framing
			// between the object section and the vector sections.
			name: "rich object combined with all vobject vector types",
			obj: models.Object{
				ID:                 strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeee11"),
				Class:              "Article",
				CreationTimeUnix:   nowMilli,
				LastUpdateTimeUnix: nowMilli + 1000,
				Tenant:             "tenant-x",
				Vector:             models.C11yVector{0.9, 0.8, 0.7},
				Properties: map[string]interface{}{
					"title": "combined test",
					"score": float64(99.9),
				},
				Additional: models.AdditionalProperties{"certainty": float64(1.0)},
			},
			vobj: VObject{
				Vector:  []float32{1, 2, 3},
				Vectors: map[string][]float32{"named": {4, 5, 6}},
				MultiVectors: map[string][][]float32{
					"multi": {{7, 8}, {9, 10}},
				},
				StaleUpdateTime:         nowMilli,
				LastUpdateTimeUnixMilli: nowMilli + 500,
				Version:                 99,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.vobj
			in.LatestObject = &tc.obj
			if in.StaleUpdateTime == 0 {
				in.StaleUpdateTime = nowMilli
			}
			if in.LastUpdateTimeUnixMilli == 0 {
				in.LastUpdateTimeUnixMilli = nowMilli + 100
			}
			if in.Version == 0 {
				in.Version = 1
			}

			data, err := in.MarshalBinaryV2()
			require.NoError(t, err)

			var got VObject
			require.NoError(t, got.UnmarshalBinaryV2(data))

			// VObject envelope fields
			assert.Equal(t, in.StaleUpdateTime, got.StaleUpdateTime)
			assert.Equal(t, in.LastUpdateTimeUnixMilli, got.LastUpdateTimeUnixMilli)
			assert.Equal(t, in.Version, got.Version)
			assert.Equal(t, in.Vector, got.Vector)
			assert.Equal(t, in.Vectors, got.Vectors)
			assert.Equal(t, in.MultiVectors, got.MultiVectors)

			// LatestObject fields
			require.NotNil(t, got.LatestObject)
			assert.Equal(t, tc.obj.ID, got.LatestObject.ID)
			assert.Equal(t, tc.obj.Class, got.LatestObject.Class)
			assert.Equal(t, tc.obj.CreationTimeUnix, got.LatestObject.CreationTimeUnix)
			assert.Equal(t, tc.obj.LastUpdateTimeUnix, got.LatestObject.LastUpdateTimeUnix)
			assert.Equal(t, tc.obj.Tenant, got.LatestObject.Tenant)
			assert.Equal(t, tc.obj.Properties, got.LatestObject.Properties)
			assert.Equal(t, tc.obj.Vector, got.LatestObject.Vector)
			assert.Equal(t, tc.obj.Vectors, got.LatestObject.Vectors)
			assert.Equal(t, tc.obj.VectorWeights, got.LatestObject.VectorWeights)
			assert.Equal(t, tc.obj.Additional, got.LatestObject.Additional)
		})
	}
}

// TestVObjectV2SmallerThanV1 asserts that the binary V2 format is more compact than
// the legacy JSON-wrapped format for objects carrying high-dimensional vectors.
func TestVObjectV2SmallerThanV1(t *testing.T) {
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = float32(i) * 0.001
	}
	obj := VObject{
		LatestObject: &models.Object{
			ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
			Class: "C1",
		},
		Vector:          vec,
		StaleUpdateTime: time.Now().UnixMilli(),
	}

	v1, err := obj.MarshalBinary()
	require.NoError(t, err)

	v2, err := obj.MarshalBinaryV2()
	require.NoError(t, err)

	assert.Less(t, len(v2), len(v1),
		"V2 (%d bytes) should be smaller than V1 (%d bytes) for a 1536-dim vector", len(v2), len(v1))
}

// TestVObjectMarshalBinaryV2ConcurrentRace is designed to be run with -race to
// detect data races in MarshalBinaryV2 and UnmarshalBinaryV2. It exercises two
// patterns that mirror the real replication hot-path:
//
//  1. Many goroutines each marshal their own independent VObject (no shared
//     mutable state expected between invocations).
//  2. Many goroutines each unmarshal from the same read-only byte slice into
//     their own VObject instance (the source bytes must not be mutated).
func TestVObjectMarshalBinaryV2ConcurrentRace(t *testing.T) {
	const goroutines = 50

	// Pre-build a shared read-only payload for the unmarshal goroutines.
	template := VObject{
		ID:           strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
		Vector:       []float32{1, 2, 3},
		Vectors:      map[string][]float32{"n": {4, 5}},
		MultiVectors: map[string][][]float32{"m": {{6, 7}, {8, 9}}},
		LatestObject: &models.Object{
			ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
			Class:      "C1",
			Properties: map[string]interface{}{"k": "v"},
		},
		Version: 7,
	}
	sharedData, err := template.MarshalBinaryV2()
	require.NoError(t, err)

	var wg sync.WaitGroup

	// Marshal goroutines: each creates and serializes its own VObject.
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vo := VObject{
				Vector:  []float32{float32(i), float32(i + 1)},
				Version: uint64(i),
				LatestObject: &models.Object{
					Class:      "C1",
					Properties: map[string]interface{}{"idx": float64(i)},
				},
			}
			_, err := vo.MarshalBinaryV2()
			assert.NoError(t, err)
		}(i)
	}

	// Unmarshal goroutines: each decodes the shared bytes into its own instance.
	// The source slice is read-only; each target VObject is goroutine-local.
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var got VObject
			err := got.UnmarshalBinaryV2(sharedData)
			assert.NoError(t, err)
			assert.Equal(t, template.Version, got.Version)
			assert.Equal(t, template.Vector, got.Vector)
		}()
	}

	wg.Wait()
}
