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

// Wire-identity tests: legacy* functions freeze the pre-optimization marshal
// implementations so the optimized path can be asserted byte-identical to
// them. Objects with >1 Vectors/MultiVectors entry are excluded from
// byte-identity checks (msgpack map key order is nondeterministic) and
// covered by the semantic round-trip test instead.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// legacyMarshalBinaryOptional replicates storobj.(*Object).marshalBinaryInternal
// (skipClassName=false) pre-optimization.
func legacyMarshalBinaryOptional(ko *storobj.Object, addProps additional.Properties) ([]byte, error) {
	if ko.MarshallerVersion != 1 {
		return nil, fmt.Errorf("unsupported marshaller version %d", ko.MarshallerVersion)
	}

	kindByte := uint8(1)

	idParsed, err := uuid.Parse(ko.ID().String())
	if err != nil {
		return nil, err
	}
	idBytes, err := idParsed.MarshalBinary()
	if err != nil {
		return nil, err
	}

	var vectorLength uint32
	if addProps.Vector {
		vectorLength = uint32(len(ko.Vector))
	}

	className := []byte(ko.Class())
	classNameLength := uint32(len(className))

	var schema []byte
	var schemaLength uint32
	if !addProps.NoProps {
		schema, err = json.Marshal(ko.Properties())
		if err != nil {
			return nil, err
		}
		schemaLength = uint32(len(schema))
	} else {
		schema = []byte("{}")
		schemaLength = uint32(len(schema))
	}

	meta, err := json.Marshal(ko.AdditionalProperties())
	if err != nil {
		return nil, err
	}
	metaLength := uint32(len(meta))

	vectorWeights, err := json.Marshal(ko.VectorWeights())
	if err != nil {
		return nil, err
	}
	vectorWeightsLength := uint32(len(vectorWeights))

	includeAllTargetVectors := addProps.IncludeAllTargetVectors
	includeSpecificTargetVectors := len(addProps.Vectors) > 0

	var requestedVectors map[string]struct{}
	if includeSpecificTargetVectors {
		requestedVectors = make(map[string]struct{}, len(addProps.Vectors))
		for _, v := range addProps.Vectors {
			requestedVectors[v] = struct{}{}
		}
	}

	var targetVectorsOffsets []byte
	var targetVectorsOffsetsLength uint32
	var targetVectorsSegmentLength int

	targetVectorsOffsetOrder := make([]string, 0, len(ko.Vectors))
	if (includeAllTargetVectors || includeSpecificTargetVectors) && len(ko.Vectors) > 0 {
		offsetsMap := map[string]uint32{}
		for name, vec := range ko.Vectors {
			if includeSpecificTargetVectors {
				if _, ok := requestedVectors[name]; !ok {
					continue
				}
			}
			offsetsMap[name] = uint32(targetVectorsSegmentLength)
			targetVectorsSegmentLength += 2 + 4*len(vec)
			targetVectorsOffsetOrder = append(targetVectorsOffsetOrder, name)
		}

		if len(offsetsMap) > 0 {
			targetVectorsOffsets, err = msgpack.Marshal(offsetsMap)
			if err != nil {
				return nil, err
			}
			targetVectorsOffsetsLength = uint32(len(targetVectorsOffsets))
		}
	}

	var multiVectorsOffsets []byte
	var multiVectorsOffsetsLength uint32
	var multiVectorsSegmentLength int

	multiVectorsOffsetOrder := make([]string, 0, len(ko.MultiVectors))
	if (includeAllTargetVectors || includeSpecificTargetVectors) && len(ko.MultiVectors) > 0 {
		offsetsMap := map[string]uint32{}
		for name, vecs := range ko.MultiVectors {
			if includeSpecificTargetVectors {
				if _, ok := requestedVectors[name]; !ok {
					continue
				}
			}
			offsetsMap[name] = uint32(multiVectorsSegmentLength)
			multiVectorsSegmentLength += 4
			for _, vec := range vecs {
				multiVectorsSegmentLength += 2 + 4*len(vec)
			}
			multiVectorsOffsetOrder = append(multiVectorsOffsetOrder, name)
		}

		if len(offsetsMap) > 0 {
			multiVectorsOffsets, err = msgpack.Marshal(offsetsMap)
			if err != nil {
				return nil, err
			}
			multiVectorsOffsetsLength = uint32(len(multiVectorsOffsets))
		}
	}

	totalBufferLength := 1 + 8 + 1 + 16 + 8 + 8 +
		2 + vectorLength*4 +
		2 + classNameLength +
		4 + schemaLength +
		4 + metaLength +
		4 + vectorWeightsLength +
		4 + targetVectorsOffsetsLength +
		4 + uint32(targetVectorsSegmentLength) +
		4 + multiVectorsOffsetsLength +
		4 + uint32(multiVectorsSegmentLength)

	byteBuffer := make([]byte, totalBufferLength)
	rw := byteops.NewReadWriter(byteBuffer)
	rw.WriteByte(ko.MarshallerVersion)
	rw.WriteUint64(ko.DocID)
	rw.WriteByte(kindByte)

	if err := rw.CopyBytesToBuffer(idBytes); err != nil {
		return nil, err
	}

	rw.WriteUint64(uint64(ko.CreationTimeUnix()))
	rw.WriteUint64(uint64(ko.LastUpdateTimeUnix()))
	rw.WriteUint16(uint16(vectorLength))

	if addProps.Vector {
		byteops.CopySliceToBytes(rw.Buffer[rw.Position:rw.Position+uint64(vectorLength)*byteops.Uint32Len], ko.Vector)
		rw.MoveBufferPositionForward(uint64(vectorLength) * byteops.Uint32Len)
	}

	rw.WriteUint16(uint16(classNameLength))
	if classNameLength > 0 {
		if err := rw.CopyBytesToBuffer(className); err != nil {
			return nil, err
		}
	}

	rw.WriteUint32(schemaLength)
	if schemaLength > 0 {
		if err := rw.CopyBytesToBuffer(schema); err != nil {
			return nil, err
		}
	}

	rw.WriteUint32(metaLength)
	if err := rw.CopyBytesToBuffer(meta); err != nil {
		return nil, err
	}

	rw.WriteUint32(vectorWeightsLength)
	if err := rw.CopyBytesToBuffer(vectorWeights); err != nil {
		return nil, err
	}

	rw.WriteUint32(targetVectorsOffsetsLength)
	if targetVectorsOffsetsLength > 0 {
		if err := rw.CopyBytesToBuffer(targetVectorsOffsets); err != nil {
			return nil, err
		}
	}

	rw.WriteUint32(uint32(targetVectorsSegmentLength))
	for _, name := range targetVectorsOffsetOrder {
		vec := ko.Vectors[name]
		vecLen := len(vec)

		rw.WriteUint16(uint16(vecLen))
		byteops.CopySliceToBytes(rw.Buffer[rw.Position:rw.Position+uint64(vecLen)*byteops.Uint32Len], vec)
		rw.MoveBufferPositionForward(uint64(vecLen) * byteops.Uint32Len)
	}

	rw.WriteUint32(multiVectorsOffsetsLength)
	if multiVectorsOffsetsLength > 0 {
		if err := rw.CopyBytesToBuffer(multiVectorsOffsets); err != nil {
			return nil, err
		}
	}

	rw.WriteUint32(uint32(multiVectorsSegmentLength))
	for _, name := range multiVectorsOffsetOrder {
		vecs := ko.MultiVectors[name]
		rw.WriteUint32(uint32(len(vecs)))
		for _, vec := range vecs {
			vecLen := len(vec)
			rw.WriteUint16(uint16(vecLen))
			byteops.CopySliceToBytes(rw.Buffer[rw.Position:rw.Position+uint64(vecLen)*byteops.Uint32Len], vec)
			rw.MoveBufferPositionForward(uint64(vecLen) * byteops.Uint32Len)
		}
	}

	return byteBuffer, nil
}

// legacyMarshalBinary replicates storobj.(*Object).MarshalBinary pre-optimization.
func legacyMarshalBinary(ko *storobj.Object) ([]byte, error) {
	return legacyMarshalBinaryOptional(ko, additional.Properties{
		Vector:                  true,
		IncludeAllTargetVectors: true,
	})
}

// legacyMarshallStorObj replicates the marshallStorObj PUT-path wrapper pre-optimization.
func legacyMarshallStorObj(in *storobj.Object) ([]byte, error) {
	obj := storobj.Object{
		MarshallerVersion: in.MarshallerVersion,
		Object: models.Object{
			Additional:         in.Object.Additional,
			Class:              in.Object.Class,
			CreationTimeUnix:   in.Object.CreationTimeUnix,
			ID:                 in.Object.ID,
			LastUpdateTimeUnix: in.Object.LastUpdateTimeUnix,
			Properties:         in.Object.Properties,
			Tenant:             in.Object.Tenant,
			Vector:             in.Object.Vector,
			Vectors:            in.Object.Vectors,
		},
		Vector:         in.Vector,
		VectorLen:      in.VectorLen,
		BelongsToNode:  in.BelongsToNode,
		BelongsToShard: in.BelongsToShard,
		IsConsistent:   in.IsConsistent,
		DocID:          in.DocID,
		Vectors:        in.Vectors,
		MultiVectors:   in.MultiVectors,
	}
	return legacyMarshalBinary(&obj)
}

// legacyObjectListMarshal replicates objectListPayload.Marshal pre-optimization.
func legacyObjectListMarshal(in []*storobj.Object, method string) ([]byte, error) {
	out := make([]byte, 0, 1024*len(in))

	reusableLengthBuf := make([]byte, 8)
	for _, ind := range in {
		if ind != nil {
			var bytes []byte
			var err error
			switch method {
			case MethodPut:
				bytes, err = legacyMarshallStorObj(ind)
			case MethodGet:
				bytes, err = legacyMarshalBinary(ind)
			default:
				return nil, fmt.Errorf("unsupported operation type: %s", method)
			}
			if err != nil {
				return nil, err
			}

			length := uint64(len(bytes))
			binary.LittleEndian.PutUint64(reusableLengthBuf, length)

			out = append(out, reusableLengthBuf...)
			out = append(out, bytes...)
		}
	}

	return out, nil
}

// legacyObjectListMarshalWithAdditional replicates
// objectListPayload.MarshalWithAdditional pre-optimization.
func legacyObjectListMarshalWithAdditional(in []*storobj.Object, addProps additional.Properties) ([]byte, error) {
	out := make([]byte, 0, 1024*len(in))

	reusableLengthBuf := make([]byte, 8)
	for _, ind := range in {
		if ind != nil {
			bytes, err := legacyMarshalBinaryOptional(ind, addProps)
			if err != nil {
				return nil, err
			}

			length := uint64(len(bytes))
			binary.LittleEndian.PutUint64(reusableLengthBuf, length)

			out = append(out, reusableLengthBuf...)
			out = append(out, bytes...)
		}
	}

	return out, nil
}

// legacySearchResultsMarshal replicates searchResultsPayload.Marshal pre-optimization.
func legacySearchResultsMarshal(objs []*storobj.Object, dists []float32) ([]byte, error) {
	reusableLengthBuf := make([]byte, 8)
	var out []byte
	objsBytes, err := legacyObjectListMarshal(objs, MethodGet)
	if err != nil {
		return nil, err
	}

	objsLength := uint64(len(objsBytes))
	binary.LittleEndian.PutUint64(reusableLengthBuf, objsLength)

	out = append(out, reusableLengthBuf...)
	out = append(out, objsBytes...)

	distsLength := uint64(len(dists))
	binary.LittleEndian.PutUint64(reusableLengthBuf, distsLength)
	out = append(out, reusableLengthBuf...)

	distsBuf := make([]byte, distsLength*4)
	byteops.CopySliceToBytes(distsBuf, dists)
	out = append(out, distsBuf...)

	return out, nil
}

// legacySearchResultsMarshalWithAdditional replicates
// searchResultsPayload.MarshalWithAdditional pre-optimization.
func legacySearchResultsMarshalWithAdditional(objs []*storobj.Object,
	dists []float32, addProps additional.Properties, queryProfiles []helpers.ShardQueryProfile,
) ([]byte, error) {
	reusableLengthBuf := make([]byte, 8)
	var out []byte
	objsBytes, err := legacyObjectListMarshalWithAdditional(objs, addProps)
	if err != nil {
		return nil, err
	}

	objsLength := uint64(len(objsBytes))
	binary.LittleEndian.PutUint64(reusableLengthBuf, objsLength)

	out = append(out, reusableLengthBuf...)
	out = append(out, objsBytes...)

	distsLength := uint64(len(dists))
	binary.LittleEndian.PutUint64(reusableLengthBuf, distsLength)
	out = append(out, reusableLengthBuf...)

	distsBuf := make([]byte, distsLength*4)
	byteops.CopySliceToBytes(distsBuf, dists)
	out = append(out, distsBuf...)

	var profilesBytes []byte
	if len(queryProfiles) > 0 {
		profilesBytes, err = json.Marshal(queryProfiles)
		if err != nil {
			return nil, err
		}
	}
	binary.LittleEndian.PutUint64(reusableLengthBuf, uint64(len(profilesBytes)))
	out = append(out, reusableLengthBuf...)
	out = append(out, profilesBytes...)

	return out, nil
}

// wireIdentityObject builds a deterministic object for the given variant.
// All variants keep Vectors/MultiVectors at <=1 entry so that the msgpack
// offsets header is deterministic (see file comment).
func wireIdentityObject(variant string) *storobj.Object {
	obj := storobj.New(42)
	obj.MarshallerVersion = 1
	obj.Object = models.Object{
		ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
		Class:              "WireIdentityClass",
		CreationTimeUnix:   1700000000000,
		LastUpdateTimeUnix: 1700000001000,
		Properties: map[string]interface{}{
			"title":  "hello world",
			"count":  float64(7),
			"active": true,
			"tags":   []interface{}{"a", "b"},
			"nested": map[string]interface{}{"x": "y"},
		},
	}

	switch variant {
	case "props_and_vector":
		obj.Vector = []float32{0.1, 0.2, 0.3, -1.5}
		obj.VectorLen = 4
	case "props_only":
		obj.Vector = []float32{0.1, 0.2, 0.3, -1.5}
		obj.VectorLen = 4
	case "nil_vector":
		obj.Vector = nil
		obj.VectorLen = 0
	case "with_additional":
		obj.Vector = []float32{1, 2}
		obj.VectorLen = 2
		obj.Object.Additional = models.AdditionalProperties{
			"distance": float64(0.25),
		}
	case "single_named_vector":
		obj.Vector = []float32{1, 2}
		obj.VectorLen = 2
		obj.Vectors = map[string][]float32{
			"named1": {0.5, 0.6, 0.7},
		}
	case "single_multi_vector":
		obj.Vector = []float32{1, 2}
		obj.VectorLen = 2
		obj.MultiVectors = map[string][][]float32{
			"colbert": {{0.1, 0.2}, {0.3, 0.4}, {0.5, 0.6}},
		}
	case "empty_props":
		obj.Object.Properties = map[string]interface{}{}
		obj.Vector = []float32{1}
		obj.VectorLen = 1
	}

	return obj
}

var wireIdentityVariants = []string{
	"props_and_vector",
	"props_only",
	"nil_vector",
	"with_additional",
	"single_named_vector",
	"single_multi_vector",
	"empty_props",
}

func wireIdentityAddProps(variant string) additional.Properties {
	switch variant {
	case "props_only":
		return additional.Properties{}
	case "nil_vector":
		return additional.Properties{Vector: true}
	case "single_named_vector", "single_multi_vector":
		return additional.Properties{Vector: true, IncludeAllTargetVectors: true}
	default:
		return additional.Properties{Vector: true}
	}
}

func TestWireIdentityStorobjMarshal(t *testing.T) {
	for _, variant := range wireIdentityVariants {
		t.Run(variant, func(t *testing.T) {
			obj := wireIdentityObject(variant)
			addProps := wireIdentityAddProps(variant)

			legacy, err := legacyMarshalBinaryOptional(obj, addProps)
			require.NoError(t, err)

			current, err := obj.MarshalBinaryOptional(addProps)
			require.NoError(t, err)

			require.Equal(t, legacy, current, "MarshalBinaryOptional output must be byte-identical to the legacy implementation")

			legacyFull, err := legacyMarshalBinary(obj)
			require.NoError(t, err)

			currentFull, err := obj.MarshalBinary()
			require.NoError(t, err)

			require.Equal(t, legacyFull, currentFull, "MarshalBinary output must be byte-identical to the legacy implementation")
		})
	}

	t.Run("noprops_and_vector", func(t *testing.T) {
		obj := wireIdentityObject("props_and_vector")
		addProps := additional.Properties{Vector: true, NoProps: true}

		legacy, err := legacyMarshalBinaryOptional(obj, addProps)
		require.NoError(t, err)

		current, err := obj.MarshalBinaryOptional(addProps)
		require.NoError(t, err)

		require.Equal(t, legacy, current)
	})
}

func TestWireIdentityObjectListMarshal(t *testing.T) {
	objs := make([]*storobj.Object, 0, len(wireIdentityVariants)+2)
	for _, variant := range wireIdentityVariants {
		objs = append(objs, wireIdentityObject(variant))
	}
	// nil entries must be skipped identically
	objs = append(objs, nil)
	objs = append(objs, wireIdentityObject("props_and_vector"))

	for _, method := range []string{MethodGet, MethodPut} {
		t.Run(method, func(t *testing.T) {
			legacy, err := legacyObjectListMarshal(objs, method)
			require.NoError(t, err)

			current, err := IndicesPayloads.ObjectList.Marshal(objs, method)
			require.NoError(t, err)

			require.Equal(t, legacy, current, "ObjectList.Marshal output must be byte-identical to the legacy implementation")
		})
	}

	t.Run("empty_list", func(t *testing.T) {
		legacy, err := legacyObjectListMarshal(nil, MethodGet)
		require.NoError(t, err)

		current, err := IndicesPayloads.ObjectList.Marshal(nil, MethodGet)
		require.NoError(t, err)

		require.Equal(t, len(legacy), len(current))
		require.Empty(t, current)
	})

	t.Run("with_additional", func(t *testing.T) {
		for _, addProps := range []additional.Properties{
			{Vector: true, IncludeAllTargetVectors: true},
			{Vector: true},
			{},
			{Vector: true, NoProps: true},
			{Vector: true, Vectors: []string{"named1"}},
		} {
			legacy, err := legacyObjectListMarshalWithAdditional(objs, addProps)
			require.NoError(t, err)

			current, err := IndicesPayloads.ObjectList.MarshalWithAdditional(objs, addProps)
			require.NoError(t, err)

			require.Equal(t, legacy, current, "ObjectList.MarshalWithAdditional output must be byte-identical to the legacy implementation (addProps=%+v)", addProps)
		}
	})
}

func TestWireIdentitySearchResultsMarshal(t *testing.T) {
	objs := make([]*storobj.Object, 0, len(wireIdentityVariants))
	for _, variant := range wireIdentityVariants {
		objs = append(objs, wireIdentityObject(variant))
	}
	dists := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7}

	t.Run("marshal", func(t *testing.T) {
		legacy, err := legacySearchResultsMarshal(objs, dists)
		require.NoError(t, err)

		current, err := IndicesPayloads.SearchResults.Marshal(objs, dists)
		require.NoError(t, err)

		require.Equal(t, legacy, current, "SearchResults.Marshal output must be byte-identical to the legacy implementation")
	})

	t.Run("marshal_empty", func(t *testing.T) {
		legacy, err := legacySearchResultsMarshal(nil, nil)
		require.NoError(t, err)

		current, err := IndicesPayloads.SearchResults.Marshal(nil, nil)
		require.NoError(t, err)

		require.Equal(t, legacy, current)
	})

	profileVariants := map[string][]helpers.ShardQueryProfile{
		"no_profiles": nil,
		"with_profiles": {
			{Name: "shard-1", Node: "node-1"},
		},
	}

	for name, profiles := range profileVariants {
		for _, addProps := range []additional.Properties{
			{Vector: true},
			{},
			{Vector: true, NoProps: true},
		} {
			t.Run(fmt.Sprintf("marshal_with_additional/%s", name), func(t *testing.T) {
				legacy, err := legacySearchResultsMarshalWithAdditional(objs, dists, addProps, profiles)
				require.NoError(t, err)

				current, err := IndicesPayloads.SearchResults.MarshalWithAdditional(objs, dists, addProps, profiles)
				require.NoError(t, err)

				require.Equal(t, legacy, current, "SearchResults.MarshalWithAdditional output must be byte-identical to the legacy implementation")
			})
		}
	}
}

// Multi-entry Vectors/MultiVectors can't be asserted byte-identical (map
// iteration order); this checks semantic round-trip equality instead.
func TestWireIdentityMultiEntryVectorsRoundTrip(t *testing.T) {
	obj := wireIdentityObject("props_and_vector")
	obj.Vectors = map[string][]float32{
		"named1": {0.5, 0.6, 0.7},
		"named2": {1.5, 2.5},
		"named3": {9},
	}
	obj.MultiVectors = map[string][][]float32{
		"colbertA": {{0.1, 0.2}, {0.3, 0.4}},
		"colbertB": {{5, 6, 7}},
	}

	payload, err := IndicesPayloads.ObjectList.Marshal([]*storobj.Object{obj}, MethodGet)
	require.NoError(t, err)

	decodedList, err := IndicesPayloads.ObjectList.Unmarshal(payload, MethodGet)
	require.NoError(t, err)
	require.Len(t, decodedList, 1)

	decoded := decodedList[0]
	require.Equal(t, obj.DocID, decoded.DocID)
	require.Equal(t, obj.ID(), decoded.ID())
	require.Equal(t, obj.Vector, decoded.Vector)
	require.Equal(t, obj.Vectors, decoded.Vectors)
	require.Equal(t, obj.MultiVectors, decoded.MultiVectors)
	require.Equal(t, obj.Object.Class, decoded.Object.Class)
}
