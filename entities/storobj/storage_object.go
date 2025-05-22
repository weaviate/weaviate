//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package storobj

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"runtime/debug"

	"github.com/buger/jsonparser"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/byteops"
)

var bufPool *bufferPool

type Vectors map[string][]float32

func init() {
	// a 10kB buffer should be large enough for typical cases, it can fit a
	// 1536d uncompressed vector and about 3kB of object payload. If the
	// initial size is not large enoug, the caller can always allocate a larger
	// buffer and return that to the pool instead.
	bufPool = newBufferPool(10 * 1024)
}

type Object struct {
	MarshallerVersion uint8
	Object            models.Object `json:"object"`
	Vector            []float32     `json:"vector"`
	VectorLen         int           `json:"-"`
	BelongsToNode     string        `json:"-"`
	BelongsToShard    string        `json:"-"`
	IsConsistent      bool          `json:"-"`
	DocID             uint64
	Vectors           map[string][]float32   `json:"vectors"`
	MultiVectors      map[string][][]float32 `json:"multivectors"`
}

func New(docID uint64) *Object {
	return &Object{
		MarshallerVersion: 1,
		DocID:             docID,
	}
}

// TODO: temporary solution
func FromObject(object *models.Object, vector []float32, vectors map[string][]float32, multivectors map[string][][]float32) *Object {
	// clear out nil entries of properties to make sure leaving a property out and setting it nil is identical
	properties, ok := object.Properties.(map[string]interface{})
	if ok {
		for key, prop := range properties {
			if prop == nil {
				delete(properties, key)
			}
		}
		object.Properties = properties
	}

	var vecs map[string][]float32
	if vectors != nil {
		vecs = make(map[string][]float32)
		for targetVector, vector := range vectors {
			vecs[targetVector] = vector
		}
	}

	var multiVectors map[string][][]float32
	if multivectors != nil {

		multiVectors = make(map[string][][]float32)
		for targetVector, vectors := range multivectors {
			multiVectors[targetVector] = vectors
		}
	}

	return &Object{
		Object:            *object,
		Vector:            vector,
		MarshallerVersion: 1,
		VectorLen:         len(vector),
		Vectors:           vecs,
		MultiVectors:      multiVectors,
	}
}

func FromBinary(data []byte) (*Object, error) {
	ko := &Object{}
	fmt.Printf("Umarshalling %v\n", string(data))
	json.Unmarshal(data, ko)

	return ko, nil
}

func FromBinaryUUIDOnly(data []byte) (*Object, error) {
	return FromBinary(data)

}

func FromBinaryOptional(data []byte,
	addProp additional.Properties, properties *PropertyExtraction,
) (*Object, error) {
	return FromBinary(data)
}

type PropertyExtraction struct {
	PropertyPaths [][]string
}

func NewPropExtraction() *PropertyExtraction {
	return &PropertyExtraction{
		PropertyPaths: [][]string{},
	}
}

func (pe *PropertyExtraction) Add(props ...string) *PropertyExtraction {
	for i := range props {
		pe.PropertyPaths = append(pe.PropertyPaths, []string{props[i]})
	}
	return pe
}

type bucket interface {
	GetBySecondary(int, []byte) ([]byte, error)
	GetBySecondaryWithBuffer(int, []byte, []byte) ([]byte, []byte, error)
}

func ObjectsByDocID(bucket bucket, ids []uint64,
	additional additional.Properties, properties []string, logger logrus.FieldLogger,
) ([]*Object, error) {
	var out = []*Object{}
	fmt.Printf("Getting objects by docID %v\n", ids)

	for _, objId := range ids {
		keyBuf := bytes.NewBuffer(nil)
		err := binary.Write(keyBuf, binary.LittleEndian, objId)
		if err != nil {
			//eh, whatever
		}
		docIDBytes := keyBuf.Bytes()
		binary.LittleEndian.PutUint64(docIDBytes, objId)
		objBytes, err := bucket.GetBySecondary(-1, docIDBytes)

		obj, err := FromBinary(objBytes)
		if err == nil {
			out = append(out, obj)
		} else {
			out = append(out, nil)
		}

	}

	return out, nil
}


func (ko *Object) Class() schema.ClassName {
	return schema.ClassName(ko.Object.Class)
}

func (ko *Object) SetDocID(id uint64) {
	ko.DocID = id
}

func (ko *Object) GetDocID() uint64 {
	return ko.DocID
}

func (ko *Object) CreationTimeUnix() int64 {
	return ko.Object.CreationTimeUnix
}

func (ko *Object) ExplainScore() string {
	props := ko.AdditionalProperties()
	if props != nil {
		iface := props["explainScore"]
		if iface != nil {
			return iface.(string)
		}
	}
	return ""
}

func (ko *Object) ID() strfmt.UUID {
	return ko.Object.ID
}

func (ko *Object) SetID(id strfmt.UUID) {
	ko.Object.ID = id
}

func (ko *Object) SetClass(class string) {
	ko.Object.Class = class
}

func (ko *Object) LastUpdateTimeUnix() int64 {
	return ko.Object.LastUpdateTimeUnix
}

// AdditionalProperties groups all properties which are stored with the
// object and not generated at runtime
func (ko *Object) AdditionalProperties() models.AdditionalProperties {
	return ko.Object.Additional
}

func (ko *Object) Properties() models.PropertySchema {
	return ko.Object.Properties
}

func (ko *Object) PropertiesWithAdditional(
	additional additional.Properties,
) models.PropertySchema {
	properties := ko.Properties()

	if additional.RefMeta {
		// nothing to remove
		return properties
	}

	asMap, ok := properties.(map[string]interface{})
	if !ok || asMap == nil {
		return properties
	}

	for propName, value := range asMap {
		asRefs, ok := value.(models.MultipleRef)
		if !ok {
			// not a ref, we can skip
			continue
		}

		for i := range asRefs {
			asRefs[i].Classification = nil
		}

		asMap[propName] = asRefs
	}

	return asMap
}

func (ko *Object) SetProperties(schema models.PropertySchema) {
	ko.Object.Properties = schema
}

func (ko *Object) VectorWeights() models.VectorWeights {
	return ko.Object.VectorWeights
}

func (ko *Object) SearchResult(additional additional.Properties, tenant string) *search.Result {
	propertiesMap, ok := ko.PropertiesWithAdditional(additional).(map[string]interface{})
	if !ok || propertiesMap == nil {
		propertiesMap = map[string]interface{}{}
	}
	propertiesMap["id"] = ko.ID()
	ko.SetProperties(propertiesMap)

	additionalProperties := models.AdditionalProperties{}
	if ko.AdditionalProperties() != nil {
		if interpretation, ok := additional.ModuleParams["interpretation"]; ok {
			if interpretationValue, ok := interpretation.(bool); ok && interpretationValue {
				additionalProperties["interpretation"] = ko.AdditionalProperties()["interpretation"]
			}
		}
		if additional.Classification {
			additionalProperties["classification"] = ko.AdditionalProperties()["classification"]
		}
		if additional.Group {
			additionalProperties["group"] = ko.AdditionalProperties()["group"]
		}
	}
	if ko.ExplainScore() != "" {
		additionalProperties["explainScore"] = ko.ExplainScore()
	}

	return &search.Result{
		ID:        ko.ID(),
		DocID:     &ko.DocID,
		ClassName: ko.Class().String(),
		Schema:    ko.Properties(),
		Vector:    ko.Vector,
		Vectors:   ko.asVectors(ko.Vectors, ko.MultiVectors),
		Dims:      ko.VectorLen,
		// VectorWeights: ko.VectorWeights(), // TODO: add vector weights
		Created:              ko.CreationTimeUnix(),
		Updated:              ko.LastUpdateTimeUnix(),
		AdditionalProperties: additionalProperties,
		// Score is filled in later
		ExplainScore: ko.ExplainScore(),
		IsConsistent: ko.IsConsistent,
		Tenant:       tenant, // not part of the binary
		// TODO: Beacon?
	}
}

func (ko *Object) asVectors(vectors map[string][]float32, multiVectors map[string][][]float32) models.Vectors {
	if (len(vectors) + len(multiVectors)) > 0 {
		out := make(models.Vectors)
		for targetVector, vector := range vectors {
			out[targetVector] = vector
		}
		for targetVector, vector := range multiVectors {
			out[targetVector] = vector
		}
		return out
	}
	return nil
}

func (ko *Object) SearchResultWithDist(addl additional.Properties, dist float32) search.Result {
	res := ko.SearchResult(addl, "")
	res.Dist = dist
	res.Certainty = float32(additional.DistToCertainty(float64(dist)))
	return *res
}

func (ko *Object) SearchResultWithScore(addl additional.Properties, score float32) search.Result {
	res := ko.SearchResult(addl, "")
	res.Score = score
	return *res
}

func (ko *Object) SearchResultWithScoreAndTenant(addl additional.Properties, score float32, tenant string) search.Result {
	res := ko.SearchResult(addl, tenant)
	res.Score = score
	return *res
}

func (ko *Object) Valid() bool {
	return ko.ID() != "" &&
		ko.Class().String() != ""
}

// IterateThroughVectorDimensions iterates through all vectors present on the Object and invokes
// the callback with target name and dimensions of the vector.
func (ko *Object) IterateThroughVectorDimensions(f func(targetVector string, dims int) error) error {
	if len(ko.Vector) > 0 {
		if err := f("", len(ko.Vector)); err != nil {
			return err
		}
	}

	for targetVector, vector := range ko.Vectors {
		if err := f(targetVector, len(vector)); err != nil {
			return err
		}
	}

	for targetVector, vectors := range ko.MultiVectors {
		var dims int
		for _, vector := range vectors {
			dims += len(vector)
		}
		if err := f(targetVector, dims); err != nil {
			return err
		}
	}
	return nil
}

func SearchResults(in []*Object, additional additional.Properties, tenant string) search.Results {
	out := make(search.Results, len(in))

	for i, elem := range in {
		out[i] = *(elem.SearchResult(additional, tenant))
	}

	return out
}

func SearchResultsWithScore(in []*Object, scores []float32, additional additional.Properties, tenant string) search.Results {
	out := make(search.Results, len(in))

	for i, elem := range in {
		score := float32(0.0)
		if len(scores) > i {
			score = scores[i]
		}
		out[i] = elem.SearchResultWithScoreAndTenant(additional, score, tenant)
	}

	return out
}

func SearchResultsWithDists(in []*Object, addl additional.Properties,
	dists []float32,
) search.Results {
	out := make(search.Results, len(in))

	for i, elem := range in {
		out[i] = elem.SearchResultWithDist(addl, dists[i])
	}

	return out
}

func DocIDFromBinary(in []byte) (uint64, error) {
	if len(in) < 9 {
		return 0, errors.Errorf("binary data too short")
	}
	// first by is kind, then 8 bytes for the docID
	return binary.LittleEndian.Uint64(in[1:9]), nil
}

func DocIDAndTimeFromBinary(in []byte) (docID uint64, updateTime int64, err error) {
	obj, err := FromBinary(in)
	if err != nil {
		return 0, 0, err
	}
	docID = obj.DocID
	updateTime = obj.LastUpdateTimeUnix()

	return docID, updateTime, nil
}

// MarshalBinary creates the binary representation of a kind object. Regardless
// of the marshaller version the first byte is a uint8 indicating the version
// followed by the payload which depends on the specific version
//
// Version 1
// No. of B      | Type                     | Content
// --------------------------------------------------------------
// 1             | uint8                    | MarshallerVersion = 1
// 8             | uint64                   | index id, keep early so id-only lookups are maximum efficient
// 1             | uint8                    | kind, 0=action, 1=thing - deprecated
// 16            | uint128                  | uuid
// 8             | int64                    | create time
// 8             | int64                    | update time
// 2             | uint16                   | VectorLength
// n*4           | []float32                | vector of length n
// 2             | uint16                   | length of class name
// n             | []byte                   | className
// 4             | uint32                   | length of schema json
// n             | []byte                   | schema as json
// 4             | uint32                   | length of meta json
// n             | []byte                   | meta as json
// 4             | uint32                   | length of vectorweights json
// n             | []byte                   | vectorweights as json
// 4             | uint32                   | length of packed target vectors offsets (in bytes)
// n             | []byte                   | packed target vectors offsets map { name : offset_in_bytes }
// 4             | uint32                   | length of target vectors segment (in bytes)
// n             | uint16+[]byte            | target vectors segment: sequence of vec_length + vec (uint16 + []byte), (uint16 + []byte) ...
// 4             | uint32                   | length of packed multivector offsets (in bytes)
// n             | []byte                   | packed multivector offsets map { name : offset_in_bytes }
// 4             | uint32                   | length of multivectors segment (in bytes)
// 4 + (2 + n*4) | uint32 + (uint16+[]byte) | multivectors segment: num vecs + (vec length + vec floats), ...
// TODO vec lengths immediately following num vecs so you can jump straight to specific vec?

const (
	maxVectorLength               int = math.MaxUint16
	maxClassNameLength            int = math.MaxUint16
	maxSchemaLength               int = math.MaxUint32
	maxMetaLength                 int = math.MaxUint32
	maxVectorWeightsLength        int = math.MaxUint32
	maxTargetVectorsSegmentLength int = math.MaxUint32
	maxTargetVectorsOffsetsLength int = math.MaxUint32
	maxMultiVectorsSegmentLength  int = math.MaxUint32
	maxMultiVectorsOffsetsLength  int = math.MaxUint32
)

func (ko *Object) MarshalBinary() ([]byte, error) {
	data, err := json.Marshal(ko)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// UnmarshalPropertiesFromObject accepts marshaled object as data and populates resultProperties map with the properties specified by propertyPaths.
//
// Check MarshalBinary for the order of elements in the input array
func UnmarshalPropertiesFromObject(data []byte, resultProperties map[string]interface{}, propertyPaths [][]string) error {
	if data[0] != uint8(1) {
		logrus.Panicf("unsupported binary marshaller version %d", data[0])
		return errors.Errorf("unsupported binary marshaller version %d", data[0])
	}

	// clear out old values in case an object misses values. This should NOT shrink the capacity of the map, eg there
	// are no allocations when adding the resultProperties of the next object again
	clear(resultProperties)

	startPos := uint64(1 + 8 + 1 + 16 + 8 + 8) // elements at the start
	rw := byteops.NewReadWriterWithOps(data, byteops.WithPosition(startPos))
	// get the length of the vector, each element is a float32 (4 bytes)
	vectorLength := uint64(rw.ReadUint16())
	rw.MoveBufferPositionForward(vectorLength * 4)
	classnameLength := uint64(rw.ReadUint16())
	rw.MoveBufferPositionForward(classnameLength)
	propertyLength := uint64(rw.ReadUint32())

	return UnmarshalProperties(rw.Buffer[rw.Position:rw.Position+propertyLength], resultProperties, propertyPaths)
}

// UnmarshalProperties accepts serialized properties as data and populates resultProperties map with the properties specified by propertyPaths.
func UnmarshalProperties(data []byte, properties map[string]interface{}, propertyPaths [][]string) error {
	obj, err := FromBinary(data)
	if err != nil {
		return err
	}

	for k, v := range obj.Properties().(map[string]interface{}) {
		properties[k] = v
	}

	return err
}

func parseValues(dt jsonparser.ValueType, value []byte) (interface{}, error) {
	switch dt {
	case jsonparser.Number:
		return jsonparser.ParseFloat(value)
	case jsonparser.String:
		return jsonparser.ParseString(value)
	case jsonparser.Boolean:
		return jsonparser.ParseBoolean(value)
	default:
		panic("Unknown data type") // returning an error would be better
	}
}

// UnmarshalBinary is the versioned way to unmarshal a kind object from binary,
// see MarshalBinary for the exact contents of each version
func (ko *Object) UnmarshalBinary(data []byte) error {
	o, err :=  FromBinary(data)
	if err != nil {
		return err
	}
	ko = o
	return nil
}

func unmarshalTargetVectors(rw *byteops.ReadWriter) (map[string][]float32, error) {
	// This check prevents from panic when somebody is upgrading from version that
	// didn't have multiple target vector support. This check is needed bc with named vectors
	// feature storage object can have vectors data appended at the end of the file
	if rw.Position < uint64(len(rw.Buffer)) {
		targetVectorsOffsets := rw.ReadBytesFromBufferWithUint32LengthIndicator()
		targetVectorsSegmentLength := rw.ReadUint32()
		pos := rw.Position

		if len(targetVectorsOffsets) > 0 {
			var tvOffsets map[string]uint32
			if err := msgpack.Unmarshal(targetVectorsOffsets, &tvOffsets); err != nil {
				return nil, fmt.Errorf("could not unmarshal target vectors offset: %w", err)
			}

			targetVectors := map[string][]float32{}
			for name, offset := range tvOffsets {
				rw.MoveBufferToAbsolutePosition(pos + uint64(offset))
				vecLen := rw.ReadUint16()
				vec := make([]float32, vecLen)
				for j := uint16(0); j < vecLen; j++ {
					vec[j] = math.Float32frombits(rw.ReadUint32())
				}
				targetVectors[name] = vec
			}

			rw.MoveBufferToAbsolutePosition(pos + uint64(targetVectorsSegmentLength))
			return targetVectors, nil
		}
	}
	return nil, nil
}

// unmarshalMultiVectors unmarshals the multi vectors from the buffer. If onlyUnmarshalNames is set and non-empty,
// then only the multivectors which names specified as the map's keys will be unmarshaled.
func unmarshalMultiVectors(
	rw *byteops.ReadWriter,
	onlyUnmarshalNames map[string]interface{},
) (map[string][][]float32, error) {
	// This check prevents from panic when somebody is upgrading from version that
	// didn't have multi vector support. This check is needed bc with the multi vectors
	// feature the storage object can have vectors data appended at the end of the file
	if rw.Position < uint64(len(rw.Buffer)) {
		multiVectorsOffsets := rw.ReadBytesFromBufferWithUint32LengthIndicator()
		multiVectorsSegmentLength := rw.ReadUint32()
		pos := rw.Position

		if len(multiVectorsOffsets) > 0 {
			var mvOffsets map[string]uint32
			if err := msgpack.Unmarshal(multiVectorsOffsets, &mvOffsets); err != nil {
				return nil, fmt.Errorf("could not unmarshal multi vectors offset: %w", err)
			}

			// NOTE if you sort mvOffsets by offset, you may be able to speed this up via
			// sequential reads, haven't tried this yet
			multiVectors := map[string][][]float32{}
			for name, offset := range mvOffsets {
				// if onlyUnmarshalNames is not nil and non-empty, only unmarshal the vectors
				// for the names in the map
				if len(onlyUnmarshalNames) > 0 {
					if _, ok := onlyUnmarshalNames[name]; !ok {
						continue
					}
				}
				rw.MoveBufferToAbsolutePosition(pos + uint64(offset))
				numVecs := rw.ReadUint32()
				vecs := make([][]float32, 0)
				for i := 0; i < int(numVecs); i++ {
					vecLen := rw.ReadUint16()
					vec := make([]float32, vecLen)
					for j := uint16(0); j < vecLen; j++ {
						vec[j] = math.Float32frombits(rw.ReadUint32())
					}
					vecs = append(vecs, vec)
				}
				multiVectors[name] = vecs
			}

			rw.MoveBufferToAbsolutePosition(pos + uint64(multiVectorsSegmentLength))
			return multiVectors, nil
		}
	}
	return nil, nil
}

func VectorFromBinary(in []byte, buffer []float32, targetVector string) ([]float32, error) {
	if len(in) == 0 {
		return nil, nil
	}

	version := in[0]
	if version != 1 {
		fmt.Println(string(debug.Stack()))
		return nil, errors.Errorf("unsupported marshaller version %d", version)
	}

	if targetVector != "" {
		startPos := uint64(1 + 8 + 1 + 16 + 8 + 8) // elements at the start
		rw := byteops.NewReadWriterWithOps(in, byteops.WithPosition(startPos))

		vectorLength := uint64(rw.ReadUint16())
		rw.MoveBufferPositionForward(vectorLength * 4)

		classnameLength := uint64(rw.ReadUint16())
		rw.MoveBufferPositionForward(classnameLength)

		schemaLength := uint64(rw.ReadUint32())
		rw.MoveBufferPositionForward(schemaLength)

		metaLength := uint64(rw.ReadUint32())
		rw.MoveBufferPositionForward(metaLength)

		vectorWeightsLength := uint64(rw.ReadUint32())
		rw.MoveBufferPositionForward(vectorWeightsLength)

		targetVectors, err := unmarshalTargetVectors(&rw)
		if err != nil {
			return nil, errors.Errorf("unable to unmarshal vector for target vector: %s", targetVector)
		}
		vector, ok := targetVectors[targetVector]
		if !ok {
			return nil, errors.Errorf("vector not found for target vector: %s", targetVector)
		}
		return vector, nil
	}

	// since we know the version and know that the blob is not len(0), we can
	// assume that we can directly access the vector length field. The only
	// situation where this is not accessible would be on corrupted data - where
	// it would be acceptable to panic
	vecLen := binary.LittleEndian.Uint16(in[42:44])

	var out []float32
	if cap(buffer) >= int(vecLen) {
		out = buffer[:vecLen]
	} else {
		out = make([]float32, vecLen)
	}
	vecStart := 44
	vecEnd := vecStart + int(vecLen*4)

	i := 0
	for start := vecStart; start < vecEnd; start += 4 {
		asUint := binary.LittleEndian.Uint32(in[start : start+4])
		out[i] = math.Float32frombits(asUint)
		i++
	}

	return out, nil
}

func incrementPos(in []byte, pos int, size int) int {
	b := in[pos : pos+size]
	if size == 2 {
		length := binary.LittleEndian.Uint16(b)
		pos += size + int(length)
	} else if size == 4 {
		length := binary.LittleEndian.Uint32(b)
		pos += size + int(length)
		return pos
	} else if size == 8 {
		length := binary.LittleEndian.Uint64(b)
		pos += size + int(length)
	}
	return pos
}

func MultiVectorFromBinary(in []byte, buffer []float32, targetVector string) ([][]float32, error) {
	if len(in) == 0 {
		return nil, nil
	}

	version := in[0]
	if version != 1 {
		fmt.Println(string(debug.Stack()))
		return nil, errors.Errorf("unsupported marshaller version %d", version)
	}

	// since we know the version and know that the blob is not len(0), we can
	// assume that we can directly access the vector length field. The only
	// situation where this is not accessible would be on corrupted data - where
	// it would be acceptable to panic
	vecLen := binary.LittleEndian.Uint16(in[42:44])

	var out []float32
	if cap(buffer) >= int(vecLen) {
		out = buffer[:vecLen]
	} else {
		out = make([]float32, vecLen)
	}
	vecStart := 44
	vecEnd := vecStart + int(vecLen*4)

	i := 0
	for start := vecStart; start < vecEnd; start += 4 {
		asUint := binary.LittleEndian.Uint32(in[start : start+4])
		out[i] = math.Float32frombits(asUint)
		i++
	}

	pos := vecEnd

	pos = incrementPos(in, pos, 2) // classNameLength
	pos = incrementPos(in, pos, 4) // schemaLength
	pos = incrementPos(in, pos, 4) // metaLength
	pos = incrementPos(in, pos, 4) // vectorWeightsLength
	pos = incrementPos(in, pos, 4) // bufLen
	pos = incrementPos(in, pos, 4) // targetVectorsSegmentLength

	// multivector
	var multiVectors map[string][][]float32

	if len(in) > pos {
		rw := byteops.NewReadWriterWithOps(in, byteops.WithPosition(uint64(pos)))
		mv, err := unmarshalMultiVectors(&rw, map[string]interface{}{targetVector: nil})
		if err != nil {
			return nil, errors.Errorf("unable to unmarshal multivector for target vector: %s", targetVector)
		}
		multiVectors = mv
	}

	mvout, ok := multiVectors[targetVector]
	if !ok {
		return nil, errors.Errorf("vector not found for target vector: %s", targetVector)
	}
	return mvout, nil
}

func (ko *Object) parseObject(uuid strfmt.UUID, create, update int64, className string,
	propsB []byte, additionalB []byte, vectorWeightsB []byte, properties *PropertyExtraction, propLength uint32,
) error {
	var returnProps map[string]interface{}
	if properties == nil || propLength == 0 {
		if err := json.Unmarshal(propsB, &returnProps); err != nil {
			return err
		}
	} else if len(propsB) >= int(propLength) {
		// the properties are not read in all cases, skip if not needed
		returnProps = make(map[string]interface{}, len(properties.PropertyPaths))
		if err := UnmarshalProperties(propsB[:propLength], returnProps, properties.PropertyPaths); err != nil {
			return err
		}
	}

	panic("TODO: remove this panic")

	var additionalProperties models.AdditionalProperties
	if len(additionalB) > 0 {
		if err := json.Unmarshal(additionalB, &additionalProperties); err != nil {
			return err
		}

		if prop, ok := additionalProperties["classification"]; ok {
			if classificationMap, ok := prop.(map[string]interface{}); ok {
				marshalled, err := json.Marshal(classificationMap)
				if err != nil {
					return err
				}
				var classification additional.Classification
				err = json.Unmarshal(marshalled, &classification)
				if err != nil {
					return err
				}
				additionalProperties["classification"] = &classification
			}
		}

		if prop, ok := additionalProperties["group"]; ok {
			if groupMap, ok := prop.(map[string]interface{}); ok {
				marshalled, err := json.Marshal(groupMap)
				if err != nil {
					return err
				}
				var group additional.Group
				err = json.Unmarshal(marshalled, &group)
				if err != nil {
					return err
				}

				for i, hit := range group.Hits {
					if groupHitAdditionalMap, ok := hit["_additional"].(map[string]interface{}); ok {
						marshalled, err := json.Marshal(groupHitAdditionalMap)
						if err != nil {
							return err
						}
						var groupHitsAdditional additional.GroupHitAdditional
						err = json.Unmarshal(marshalled, &groupHitsAdditional)
						if err != nil {
							return err
						}
						group.Hits[i]["_additional"] = &groupHitsAdditional
					}
				}

				additionalProperties["group"] = &group
			}
		}
	}

	var vectorWeights interface{}
	if err := json.Unmarshal(vectorWeightsB, &vectorWeights); err != nil {
		return err
	}

	ko.Object = models.Object{
		Class:              className,
		CreationTimeUnix:   create,
		LastUpdateTimeUnix: update,
		ID:                 uuid,
		Properties:         returnProps,
		VectorWeights:      vectorWeights,
		Additional:         additionalProperties,
	}

	return nil
}

// DeepCopyDangerous creates a deep copy of the underlying Object
// WARNING: This was purpose built for the batch ref usecase and only covers
// the situations that are required there. This means that cases which aren't
// reflected in that usecase may still contain references. Thus the suffix
// "Dangerous". If needed, make sure everything is copied and remove the
// suffix.
func (ko *Object) DeepCopyDangerous() *Object {
	o := &Object{
		MarshallerVersion: ko.MarshallerVersion,
		DocID:             ko.DocID,
		Object:            deepCopyObject(ko.Object),
		Vector:            deepCopyVector(ko.Vector),
		Vectors:           deepCopyVectorsMap(ko.Vectors),
		MultiVectors:      deepCopyMultiVectorsMap(ko.MultiVectors),
	}

	return o
}

func AddOwnership(objs []*Object, node, shard string) {
	for i := range objs {
		objs[i].BelongsToNode = node
		objs[i].BelongsToShard = shard
	}
}

func deepCopyVector(orig []float32) []float32 {
	out := make([]float32, len(orig))
	copy(out, orig)
	return out
}

func deepCopyMultiVector(orig [][]float32) [][]float32 {
	out := make([][]float32, len(orig))
	copy(out, orig)
	return out
}

func deepCopyVectors(orig models.Vectors) models.Vectors {
	out := make(models.Vectors, len(orig))
	for key, vec := range orig {
		switch v := any(vec).(type) {
		case []float32:
			out[key] = deepCopyVector(v)
		case [][]float32:
			out[key] = deepCopyMultiVector(v)
		default:
			// do nothing
		}
	}
	return out
}

func deepCopyVectorsMap(orig map[string][]float32) map[string][]float32 {
	out := make(map[string][]float32, len(orig))
	for key, vec := range orig {
		switch v := any(vec).(type) {
		case []float32:
			out[key] = deepCopyVector(v)
		default:
			// do nothing
		}
	}
	return out
}

func deepCopyMultiVectorsMap(orig map[string][][]float32) map[string][][]float32 {
	out := make(map[string][][]float32, len(orig))
	for key, vec := range orig {
		switch v := any(vec).(type) {
		case [][]float32:
			out[key] = deepCopyMultiVector(v)
		default:
			// do nothing
		}
	}
	return out
}

func deepCopyObject(orig models.Object) models.Object {
	return models.Object{
		Class:              orig.Class,
		ID:                 orig.ID,
		CreationTimeUnix:   orig.CreationTimeUnix,
		LastUpdateTimeUnix: orig.LastUpdateTimeUnix,
		Vector:             deepCopyVector(orig.Vector),
		VectorWeights:      orig.VectorWeights,
		Additional:         orig.Additional, // WARNING: not a deep copy!!
		Properties:         deepCopyProperties(orig.Properties),
		Vectors:            deepCopyVectors(orig.Vectors),
	}
}

func deepCopyProperties(orig models.PropertySchema) models.PropertySchema {
	if orig == nil {
		return nil
	}

	asMap, ok := orig.(map[string]interface{})
	if !ok {
		// not a map, don't know what to do with this
		return nil
	}

	out := map[string]interface{}{}

	for key, value := range asMap {
		if mref, ok := value.(models.MultipleRef); ok {
			out[key] = deepCopyMRef(mref)
			continue
		}

		// Note: This is not a true deep copy, value could still be a pointer type,
		// such as *models.GeoCoordinates, thus leading to passing a reference
		// instead of actually making a copy. However, for the purposes we need
		// this method for this is acceptable based on our current knowledge
		out[key] = value
	}

	return out
}

func deepCopyMRef(orig models.MultipleRef) models.MultipleRef {
	if orig == nil {
		return nil
	}

	out := make(models.MultipleRef, len(orig))
	for i, ref := range orig {
		// models.SingleRef contains only pass-by-value props, so a simple deref as
		// the struct creates a copy
		copiedRef := *ref
		out[i] = &copiedRef
	}

	return out
}
