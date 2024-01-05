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

	"github.com/buger/jsonparser"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/byteops"
)

var bufPool *bufferPool

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

	docID uint64
}

func New(docID uint64) *Object {
	return &Object{
		MarshallerVersion: 1,
		docID:             docID,
	}
}

func FromObject(object *models.Object, vector []float32) *Object {
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

	return &Object{
		Object:            *object,
		Vector:            vector,
		MarshallerVersion: 1,
		VectorLen:         len(vector),
	}
}

func FromBinary(data []byte) (*Object, error) {
	ko := &Object{}
	if err := ko.UnmarshalBinary(data); err != nil {
		return nil, err
	}

	return ko, nil
}

func FromBinaryUUIDOnly(data []byte) (*Object, error) {
	ko := &Object{}

	rw := byteops.NewReadWriter(data)
	version := rw.ReadUint8()
	if version != 1 {
		return nil, errors.Errorf("unsupported binary marshaller version %d", version)
	}

	ko.MarshallerVersion = version

	ko.docID = rw.ReadUint64()
	rw.MoveBufferPositionForward(1) // ignore kind-byte
	uuidObj, err := uuid.FromBytes(rw.ReadBytesFromBuffer(16))
	if err != nil {
		return nil, fmt.Errorf("parse uuid: %w", err)
	}
	ko.Object.ID = strfmt.UUID(uuidObj.String())

	rw.MoveBufferPositionForward(16)

	vecLen := rw.ReadUint16()
	rw.MoveBufferPositionForward(uint64(vecLen * 4))
	classNameLen := rw.ReadUint16()

	ko.Object.Class = string(rw.ReadBytesFromBuffer(uint64(classNameLen)))

	return ko, nil
}

func FromBinaryOptional(data []byte,
	addProp additional.Properties,
) (*Object, error) {
	ko := &Object{}

	rw := byteops.NewReadWriter(data)
	ko.MarshallerVersion = rw.ReadUint8()
	if ko.MarshallerVersion != 1 {
		return nil, errors.Errorf("unsupported binary marshaller version %d", ko.MarshallerVersion)
	}
	ko.docID = rw.ReadUint64()
	rw.MoveBufferPositionForward(1) // ignore kind-byte
	uuidObj, err := uuid.FromBytes(rw.ReadBytesFromBuffer(16))
	if err != nil {
		return nil, fmt.Errorf("parse uuid: %w", err)
	}
	uuidParsed := strfmt.UUID(uuidObj.String())

	createTime := int64(rw.ReadUint64())
	updateTime := int64(rw.ReadUint64())
	vectorLength := rw.ReadUint16()
	// The vector length should always be returned (for usage metrics purposes) even if the vector itself is skipped
	ko.VectorLen = int(vectorLength)
	if addProp.Vector {
		ko.Object.Vector = make([]float32, vectorLength)
		vectorBytes := rw.ReadBytesFromBuffer(uint64(vectorLength) * 4)
		for i := 0; i < int(vectorLength); i++ {
			bits := binary.LittleEndian.Uint32(vectorBytes[i*4 : (i+1)*4])
			ko.Object.Vector[i] = math.Float32frombits(bits)
		}
	} else {
		rw.MoveBufferPositionForward(uint64(vectorLength) * 4)
		ko.Object.Vector = nil
	}
	ko.Vector = ko.Object.Vector

	classNameLen := rw.ReadUint16()
	className := string(rw.ReadBytesFromBuffer(uint64(classNameLen)))

	propLength := rw.ReadUint32()
	var props []byte
	if addProp.NoProps {
		rw.MoveBufferPositionForward(uint64(propLength))
	} else {
		props = rw.ReadBytesFromBuffer(uint64(propLength))
	}

	var meta []byte
	metaLength := rw.ReadUint32()
	if addProp.Classification || len(addProp.ModuleParams) > 0 {
		meta = rw.ReadBytesFromBuffer(uint64(metaLength))
	} else {
		rw.MoveBufferPositionForward(uint64(metaLength))
	}

	vectorWeightsLength := rw.ReadUint32()
	vectorWeights := rw.ReadBytesFromBuffer(uint64(vectorWeightsLength))

	// some object members need additional "enrichment". Only do this if necessary, ie if they are actually present
	if len(props) > 0 ||
		len(meta) > 0 ||
		vectorWeightsLength > 0 &&
			!( // if the length is 4 and the encoded value is "null" (in ascii), vectorweights are not actually present
			vectorWeightsLength == 4 &&
				vectorWeights[0] == 110 && // n
				vectorWeights[1] == 117 && // u
				vectorWeights[2] == 108 && // l
				vectorWeights[3] == 108) { // l

		if err := ko.parseObject(
			uuidParsed,
			createTime,
			updateTime,
			className,
			props,
			meta,
			vectorWeights,
		); err != nil {
			return nil, errors.Wrap(err, "parse")
		}
	} else {
		ko.Object.ID = uuidParsed
		ko.Object.CreationTimeUnix = createTime
		ko.Object.LastUpdateTimeUnix = updateTime
		ko.Object.Class = className
	}

	return ko, nil
}

type bucket interface {
	GetBySecondary(int, []byte) ([]byte, error)
	GetBySecondaryWithBuffer(int, []byte, []byte) ([]byte, []byte, error)
}

func ObjectsByDocID(bucket bucket, ids []uint64,
	additional additional.Properties,
) ([]*Object, error) {
	if bucket == nil {
		return nil, fmt.Errorf("objects bucket not found")
	}

	var (
		docIDBuf = make([]byte, 8)
		out      = make([]*Object, len(ids))
		i        = 0
		lsmBuf   = bufPool.Get()
	)

	defer func() {
		bufPool.Put(lsmBuf)
	}()

	for _, id := range ids {
		binary.LittleEndian.PutUint64(docIDBuf, id)
		res, newBuf, err := bucket.GetBySecondaryWithBuffer(0, docIDBuf, lsmBuf)
		if err != nil {
			return nil, err
		}

		lsmBuf = newBuf // may have changed, e.g. because it was grown

		if res == nil {
			continue
		}

		unmarshalled, err := FromBinaryOptional(res, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal data object at position %d", i)
		}

		out[i] = unmarshalled
		i++
	}

	return out[:i], nil
}

func (ko *Object) Class() schema.ClassName {
	return schema.ClassName(ko.Object.Class)
}

func (ko *Object) SetDocID(id uint64) {
	ko.docID = id
}

func (ko *Object) DocID() uint64 {
	return ko.docID
}

func (ko *Object) CreationTimeUnix() int64 {
	return ko.Object.CreationTimeUnix
}

func (ko *Object) Score() float32 {
	props := ko.AdditionalProperties()
	if props != nil {
		iface := props["score"]
		if iface != nil {
			return iface.(float32)
		}
	}
	return 0
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
		ClassName: ko.Class().String(),
		Schema:    ko.Properties(),
		Vector:    ko.Vector,
		Dims:      ko.VectorLen,
		// VectorWeights: ko.VectorWeights(), // TODO: add vector weights
		Created:              ko.CreationTimeUnix(),
		Updated:              ko.LastUpdateTimeUnix(),
		AdditionalProperties: additionalProperties,
		Score:                ko.Score(),
		ExplainScore:         ko.ExplainScore(),
		IsConsistent:         ko.IsConsistent,
		Tenant:               tenant, // not part of the binary
		// TODO: Beacon?
	}
}

func (ko *Object) SearchResultWithDist(addl additional.Properties, dist float32) search.Result {
	res := ko.SearchResult(addl, "")
	res.Dist = dist
	res.Certainty = float32(additional.DistToCertainty(float64(dist)))
	return *res
}

func (ko *Object) Valid() bool {
	return ko.ID() != "" &&
		ko.Class().String() != ""
}

func SearchResults(in []*Object, additional additional.Properties, tenant string) search.Results {
	out := make(search.Results, len(in))

	for i, elem := range in {
		out[i] = *(elem.SearchResult(additional, tenant))
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
	var version uint8
	r := bytes.NewReader(in)
	le := binary.LittleEndian
	if err := binary.Read(r, le, &version); err != nil {
		return 0, err
	}

	if version != 1 {
		return 0, errors.Errorf("unsupported binary marshaller version %d", version)
	}

	var docID uint64
	err := binary.Read(r, le, &docID)
	return docID, err
}

// MarshalBinary creates the binary representation of a kind object. Regardless
// of the marshaller version the first byte is a uint8 indicating the version
// followed by the payload which depends on the specific version
//
// Version 1
// No. of B   | Type      | Content
// ------------------------------------------------
// 1          | uint8     | MarshallerVersion = 1
// 8          | uint64    | index id, keep early so id-only lookups are maximum efficient
// 1          | uint8     | kind, 0=action, 1=thing - deprecated
// 16         | uint128   | uuid
// 8          | int64     | create time
// 8          | int64     | update time
// 2          | uint16    | VectorLength
// n*4        | []float32 | vector of length n
// 2          | uint16    | length of class name
// n          | []byte    | className
// 4          | uint32    | length of schema json
// n          | []byte    | schema as json
// 2          | uint32    | length of meta json
// n          | []byte    | meta as json
// 2          | uint32    | length of vectorweights json
// n          | []byte    | vectorweights as json
func (ko *Object) MarshalBinary() ([]byte, error) {
	if ko.MarshallerVersion != 1 {
		return nil, errors.Errorf("unsupported marshaller version %d", ko.MarshallerVersion)
	}

	kindByte := uint8(0)
	// Deprecated Kind field
	kindByte = 1

	idParsed, err := uuid.Parse(ko.ID().String())
	if err != nil {
		return nil, err
	}
	idBytes, err := idParsed.MarshalBinary()
	if err != nil {
		return nil, err
	}
	vectorLength := uint32(len(ko.Vector))
	className := []byte(ko.Class())
	classNameLength := uint32(len(className))
	schema, err := json.Marshal(ko.Properties())
	if err != nil {
		return nil, err
	}
	schemaLength := uint32(len(schema))
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

	totalBufferLength := 1 + 8 + 1 + 16 + 8 + 8 + 2 + vectorLength*4 + 2 + classNameLength + 4 + schemaLength + 4 + metaLength + 4 + vectorWeightsLength
	byteBuffer := make([]byte, totalBufferLength)
	rw := byteops.NewReadWriter(byteBuffer)
	rw.WriteByte(ko.MarshallerVersion)
	rw.WriteUint64(ko.docID)
	rw.WriteByte(kindByte)

	rw.CopyBytesToBuffer(idBytes)

	rw.WriteUint64(uint64(ko.CreationTimeUnix()))
	rw.WriteUint64(uint64(ko.LastUpdateTimeUnix()))
	rw.WriteUint16(uint16(vectorLength))

	for j := uint32(0); j < vectorLength; j++ {
		rw.WriteUint32(math.Float32bits(ko.Vector[j]))
	}

	rw.WriteUint16(uint16(classNameLength))
	err = rw.CopyBytesToBuffer(className)
	if err != nil {
		return byteBuffer, errors.Wrap(err, "Could not copy className")
	}

	rw.WriteUint32(schemaLength)
	err = rw.CopyBytesToBuffer(schema)
	if err != nil {
		return byteBuffer, errors.Wrap(err, "Could not copy schema")
	}

	rw.WriteUint32(metaLength)
	err = rw.CopyBytesToBuffer(meta)
	if err != nil {
		return byteBuffer, errors.Wrap(err, "Could not copy meta")
	}
	rw.WriteUint32(vectorWeightsLength)
	err = rw.CopyBytesToBuffer(vectorWeights)
	if err != nil {
		return byteBuffer, errors.Wrap(err, "Could not copy vectorWeights")
	}

	return byteBuffer, nil
}

// UnmarshalPropertiesFromObject only unmarshals and returns the properties part of the object
//
// Check MarshalBinary for the order of elements in the input array
func UnmarshalPropertiesFromObject(data []byte, properties *map[string]interface{}, aggregationProperties []string, propStrings [][]string) error {
	if data[0] != uint8(1) {
		return errors.Errorf("unsupported binary marshaller version %d", data[0])
	}

	// clear out old values in case an object misses values. This should NOT shrink the capacity of the map, eg there
	// are no allocations when adding the properties of the next object again
	for k := range *properties {
		delete(*properties, k)
	}

	startPos := uint64(1 + 8 + 1 + 16 + 8 + 8) // elements at the start
	rw := byteops.NewReadWriter(data, byteops.WithPosition(startPos))
	// get the length of the vector, each element is a float32 (4 bytes)
	vectorLength := uint64(rw.ReadUint16())
	rw.MoveBufferPositionForward(vectorLength * 4)

	classnameLength := uint64(rw.ReadUint16())
	rw.MoveBufferPositionForward(classnameLength)
	propertyLength := uint64(rw.ReadUint32())

	jsonparser.EachKey(data[rw.Position:rw.Position+propertyLength], func(idx int, value []byte, dataType jsonparser.ValueType, err error) {
		var errParse error
		switch dataType {
		case jsonparser.Number, jsonparser.String, jsonparser.Boolean:
			val, err := parseValues(dataType, value)
			errParse = err
			(*properties)[aggregationProperties[idx]] = val
		case jsonparser.Array: // can be a beacon or an actual array
			arrayEntries := value[1 : len(value)-1] // without leading and trailing []
			beaconVal, errBeacon := jsonparser.GetUnsafeString(arrayEntries, "beacon")
			if errBeacon == nil {
				(*properties)[aggregationProperties[idx]] = []interface{}{map[string]interface{}{"beacon": beaconVal}}
			} else {
				// check how many entries there are in the array by counting the ",". This allows us to allocate an
				// array with the right size without extending it with every append.
				// The size can be too large for string arrays, when they contain "," as part of their content.
				entryCount := 0
				for _, b := range arrayEntries {
					if b == uint8(44) { // ',' as byte
						entryCount++
					}
				}

				array := make([]interface{}, 0, entryCount)
				jsonparser.ArrayEach(value, func(innerValue []byte, innerDataType jsonparser.ValueType, offset int, innerErr error) {
					var val interface{}

					switch innerDataType {
					case jsonparser.Number, jsonparser.String, jsonparser.Boolean:
						val, errParse = parseValues(innerDataType, innerValue)
					default:
						panic("Unknown data type ArrayEach") // returning an error would be better
					}
					array = append(array, val)
				})
				(*properties)[aggregationProperties[idx]] = array

			}
		default:
			panic("Unknown data type EachKey") // returning an error would be better
		}
		if errParse != nil {
			panic(errParse)
		}
	}, propStrings...)

	return nil
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
	version := data[0]
	if version != 1 {
		return errors.Errorf("unsupported binary marshaller version %d", version)
	}
	ko.MarshallerVersion = version

	rw := byteops.NewReadWriter(data, byteops.WithPosition(1))
	ko.docID = rw.ReadUint64()
	rw.MoveBufferPositionForward(1) // kind-byte

	uuidParsed, err := uuid.FromBytes(data[rw.Position : rw.Position+16])
	if err != nil {
		return err
	}
	rw.MoveBufferPositionForward(16)

	createTime := int64(rw.ReadUint64())
	updateTime := int64(rw.ReadUint64())

	vectorLength := rw.ReadUint16()
	ko.VectorLen = int(vectorLength)
	ko.Vector = make([]float32, vectorLength)
	for j := 0; j < int(vectorLength); j++ {
		ko.Vector[j] = math.Float32frombits(rw.ReadUint32())
	}

	classNameLength := uint64(rw.ReadUint16())
	className, err := rw.CopyBytesFromBuffer(classNameLength, nil)
	if err != nil {
		return errors.Wrap(err, "Could not copy class name")
	}

	schemaLength := uint64(rw.ReadUint32())
	schema, err := rw.CopyBytesFromBuffer(schemaLength, nil)
	if err != nil {
		return errors.Wrap(err, "Could not copy schema")
	}

	metaLength := uint64(rw.ReadUint32())
	meta, err := rw.CopyBytesFromBuffer(metaLength, nil)
	if err != nil {
		return errors.Wrap(err, "Could not copy meta")
	}

	vectorWeightsLength := uint64(rw.ReadUint32())
	vectorWeights, err := rw.CopyBytesFromBuffer(vectorWeightsLength, nil)
	if err != nil {
		return errors.Wrap(err, "Could not copy vectorWeights")
	}

	return ko.parseObject(
		strfmt.UUID(uuidParsed.String()),
		createTime,
		updateTime,
		string(className),
		schema,
		meta,
		vectorWeights,
	)
}

func VectorFromBinary(in []byte, buffer []float32) ([]float32, error) {
	if len(in) == 0 {
		return nil, nil
	}

	version := in[0]
	if version != 1 {
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

	return out, nil
}

func (ko *Object) parseObject(uuid strfmt.UUID, create, update int64, className string,
	propsB []byte, additionalB []byte, vectorWeightsB []byte,
) error {
	var props map[string]interface{}
	if err := json.Unmarshal(propsB, &props); err != nil {
		return err
	}

	if err := enrichSchemaTypes(props, false); err != nil {
		return errors.Wrap(err, "enrich schema datatypes")
	}

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
		Properties:         props,
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
	return &Object{
		MarshallerVersion: ko.MarshallerVersion,
		docID:             ko.docID,
		Object:            deepCopyObject(ko.Object),
		Vector:            deepCopyVector(ko.Vector),
	}
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
