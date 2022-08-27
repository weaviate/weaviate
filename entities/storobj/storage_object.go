//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package storobj

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/byte_operations"
)

type Object struct {
	MarshallerVersion uint8
	Object            models.Object `json:"object"`
	Vector            []float32     `json:"vector"`
	VectorLen         int           `json:"-"`
	docID             uint64
}

func New(docID uint64) *Object {
	return &Object{
		MarshallerVersion: 1,
		docID:             docID,
	}
}

func FromObject(object *models.Object, vector []float32) *Object {
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

func FromBinaryOptional(data []byte,
	addProp additional.Properties,
) (*Object, error) {
	ko := &Object{}

	var version uint8
	r := bytes.NewReader(data)
	le := binary.LittleEndian
	if err := binary.Read(r, le, &version); err != nil {
		return nil, err
	}

	if version != 1 {
		return nil, errors.Errorf("unsupported binary marshaller version %d", version)
	}

	ko.MarshallerVersion = version

	var (
		kindByte            uint8
		uuidBytes           = make([]byte, 16)
		createTime          int64
		updateTime          int64
		vectorLength        uint16
		classNameLength     uint16
		schemaLength        uint32
		metaLength          uint32
		vectorWeightsLength uint32
	)

	ec := &errorcompounder.ErrorCompounder{}
	ec.AddWrap(binary.Read(r, le, &ko.docID), "doc id")
	ec.AddWrap(binary.Read(r, le, &kindByte), "kind")
	_, err := r.Read(uuidBytes)
	ec.AddWrap(err, "uuid")
	ec.AddWrap(binary.Read(r, le, &createTime), "create time")
	ec.AddWrap(binary.Read(r, le, &updateTime), "update time")
	ec.AddWrap(binary.Read(r, le, &vectorLength), "vector length")
	ko.VectorLen = int(vectorLength)
	if addProp.Vector {
		ko.Vector = make([]float32, vectorLength)
		ec.AddWrap(binary.Read(r, le, &ko.Vector), "read vector")
	} else {
		io.CopyN(io.Discard, r, int64(vectorLength*4))
	}
	ec.AddWrap(binary.Read(r, le, &classNameLength), "class name length")
	className := make([]byte, classNameLength)
	_, err = r.Read(className)
	ec.AddWrap(err, "class name")
	ec.AddWrap(binary.Read(r, le, &schemaLength), "schema length")
	schema := make([]byte, schemaLength)
	_, err = r.Read(schema)
	ec.AddWrap(err, "schema")
	ec.AddWrap(binary.Read(r, le, &metaLength), "additional length")
	var meta []byte
	if addProp.Classification || len(addProp.ModuleParams) > 0 {
		meta = make([]byte, metaLength)
		_, err = r.Read(meta)
		ec.AddWrap(err, "read additional")
	} else {
		io.CopyN(io.Discard, r, int64(metaLength))
	}

	ec.AddWrap(binary.Read(r, le, &vectorWeightsLength), "vector weights length")
	vectorWeights := make([]byte, vectorWeightsLength)
	_, err = r.Read(vectorWeights)
	ec.AddWrap(err, "vector weights")

	if err := ec.ToError(); err != nil {
		return nil, errors.Wrap(err, "compound err")
	}

	uuidParsed, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return nil, err
	}

	if err := ko.parseObject(
		strfmt.UUID(uuidParsed.String()),
		createTime,
		updateTime,
		string(className),
		schema,
		meta,
		vectorWeights,
	); err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	return ko, nil
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

func (ko *Object) SearchResult(additional additional.Properties) *search.Result {
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
		Score:                1, // TODO: actually score
		// TODO: Beacon?
	}
}

func (ko *Object) Valid() bool {
	return ko.ID() != "" &&
		ko.Class().String() != ""
}

func SearchResults(in []*Object, additional additional.Properties) search.Results {
	out := make(search.Results, len(in))

	for i, elem := range in {
		out[i] = *(elem.SearchResult(additional))
	}

	return out
}

func SearchResultsWithDists(in []*Object, additional additional.Properties,
	dists []float32,
) search.Results {
	out := make(search.Results, len(in))

	for i, elem := range in {
		out[i] = *(elem.SearchResult(additional))
		out[i].Dist = dists[i]
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
	byteOps := byte_operations.ByteOperations{Buffer: byteBuffer}
	byteOps.WriteByte(ko.MarshallerVersion)
	byteOps.WriteUint64(ko.docID)
	byteOps.WriteByte(kindByte)

	byteOps.CopyBytesToBuffer(idBytes)

	byteOps.WriteUint64(uint64(ko.CreationTimeUnix()))
	byteOps.WriteUint64(uint64(ko.LastUpdateTimeUnix()))
	byteOps.WriteUint16(uint16(vectorLength))

	for j := uint32(0); j < vectorLength; j++ {
		byteOps.WriteUint32(math.Float32bits(ko.Vector[j]))
	}

	byteOps.WriteUint16(uint16(classNameLength))
	err = byteOps.CopyBytesToBuffer(className)
	if err != nil {
		return byteBuffer, errors.Wrap(err, "Could not copy className")
	}

	byteOps.WriteUint32(schemaLength)
	err = byteOps.CopyBytesToBuffer(schema)
	if err != nil {
		return byteBuffer, errors.Wrap(err, "Could not copy schema")
	}

	byteOps.WriteUint32(metaLength)
	err = byteOps.CopyBytesToBuffer(meta)
	if err != nil {
		return byteBuffer, errors.Wrap(err, "Could not copy meta")
	}
	byteOps.WriteUint32(vectorWeightsLength)
	err = byteOps.CopyBytesToBuffer(vectorWeights)
	if err != nil {
		return byteBuffer, errors.Wrap(err, "Could not copy vectorWeights")
	}

	return byteBuffer, nil
}

// UnmarshalPropertiesFromObject only unmarshals and returns the properties part of the object
//
// Check MarshalBinary for the order of elements in the input array
func UnmarshalPropertiesFromObject(data []byte, properties *models.PropertySchema) error {
	if data[0] != uint8(1) {
		return errors.Errorf("unsupported binary marshaller version %d", data[0])
	}

	startPos := uint64(1 + 8 + 1 + 16 + 8 + 8) // elements at the start
	byteOps := byte_operations.ByteOperations{Position: startPos, Buffer: data}
	// get the length of the vector, each element is a float32 (4 bytes)
	vectorLength := uint64(byteOps.ReadUint16())
	byteOps.MoveBufferPositionForward(vectorLength * 4)

	// length of class name
	classnameLength := uint64(byteOps.ReadUint16())
	byteOps.MoveBufferPositionForward(classnameLength)

	// property schema length
	propertyLength := uint64(byteOps.ReadUint32())
	if err := json.Unmarshal(data[byteOps.Position:byteOps.Position+propertyLength], properties); err != nil {
		return err
	}

	return nil
}

// UnmarshalBinary is the versioned way to unmarshal a kind object from binary,
// see MarshalBinary for the exact contents of each version
func (ko *Object) UnmarshalBinary(data []byte) error {
	version := data[0]
	if version != 1 {
		return errors.Errorf("unsupported binary marshaller version %d", version)
	}
	ko.MarshallerVersion = version

	byteOps := byte_operations.ByteOperations{Position: 1, Buffer: data}
	ko.docID = byteOps.ReadUint64()
	byteOps.MoveBufferPositionForward(1) // kind-byte

	uuidParsed, err := uuid.FromBytes(data[byteOps.Position : byteOps.Position+16])
	if err != nil {
		return err
	}
	byteOps.MoveBufferPositionForward(16)

	createTime := int64(byteOps.ReadUint64())
	updateTime := int64(byteOps.ReadUint64())

	vectorLength := byteOps.ReadUint16()
	ko.VectorLen = int(vectorLength)
	ko.Vector = make([]float32, vectorLength)
	for j := 0; j < int(vectorLength); j++ {
		ko.Vector[j] = math.Float32frombits(byteOps.ReadUint32())
	}

	classNameLength := uint64(byteOps.ReadUint16())
	className, err := byteOps.CopyBytesFromBuffer(classNameLength, nil)
	if err != nil {
		return errors.Wrap(err, "Could not copy class name")
	}

	schemaLength := uint64(byteOps.ReadUint32())
	schema, err := byteOps.CopyBytesFromBuffer(schemaLength, nil)
	if err != nil {
		return errors.Wrap(err, "Could not copy schema")
	}

	metaLength := uint64(byteOps.ReadUint32())
	meta, err := byteOps.CopyBytesFromBuffer(metaLength, nil)
	if err != nil {
		return errors.Wrap(err, "Could not copy meta")
	}

	vectorWeightsLength := uint64(byteOps.ReadUint32())
	vectorWeights, err := byteOps.CopyBytesFromBuffer(vectorWeightsLength, nil)
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

func VectorFromBinary(in []byte) ([]float32, error) {
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

	out := make([]float32, vecLen)
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
	schemaB []byte, additionalB []byte, vectorWeightsB []byte,
) error {
	var schema map[string]interface{}
	if err := json.Unmarshal(schemaB, &schema); err != nil {
		return err
	}

	if err := ko.enrichSchemaTypes(schema); err != nil {
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
		Properties:         schema,
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
