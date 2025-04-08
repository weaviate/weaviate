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

package v1

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/byteops"
	"google.golang.org/protobuf/runtime/protoimpl"
)

type Mapper struct {
	uses125 bool
}

func NewMapping(uses125 bool) *Mapper {
	return &Mapper{uses125: uses125}
}

func (m *Mapper) NewPrimitiveValue(v interface{}, dt schema.DataType) (*pb.Value, error) {
	if v == nil {
		return m.NewNilValue(), nil
	}
	innerDt, ok := schema.IsArrayType(dt)
	if ok {
		return m.parsePrimitiveArray(v, dt, innerDt)
	} else {
		switch dt {
		case schema.DataTypeBoolean:
			val, ok := v.(bool)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected bool when serializing bool", v)
			}
			return NewBoolValue(val), nil
		case schema.DataTypeDate:
			val, ok := v.(string)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected string when serializing date", v)
			}
			return NewDateValue(val), nil
		case schema.DataTypeNumber:
			val, ok := v.(float64)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected float64 when serializing number", v)
			}
			return NewNumberValue(val), nil
		case schema.DataTypeInt:
			val, ok := v.(float64)
			if !ok { // integers are returned as float64 from search
				return nil, protoimpl.X.NewError("invalid type: %T expected float64 when serializing int property", v)
			}
			return NewIntValue(int64(val)), nil
		case schema.DataTypeString:
			val, ok := v.(string)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected string when serializing string property", v)
			}
			if m.uses125 {
				return NewTextValue(val), nil
			} else {
				return NewStringValue(val), nil
			}
		case schema.DataTypeText:
			val, ok := v.(string)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected string when serializing text property", v)
			}
			if m.uses125 {
				return NewTextValue(val), nil
			} else {
				return NewStringValue(val), nil
			}
		case schema.DataTypeUUID:
			val, ok := v.(string)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected string when serializing uuid property", v)
			}
			return NewUuidValue(val), nil
		case schema.DataTypeGeoCoordinates:
			val, ok := v.(*models.GeoCoordinates)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected *models.GeoCoordinates when serializing geocoordinate property", v)
			}
			return NewGeoValue(val), nil
		case schema.DataTypeBlob:
			val, ok := v.(string)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected string when serializing blob property", v)
			}
			return newBlobValue(val), nil
		case schema.DataTypePhoneNumber:
			val, ok := v.(*models.PhoneNumber)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected *models.PhoneNumber when serializing phone number property", v)
			}
			return newPhoneNumberValue(val), nil
		default:
			return nil, protoimpl.X.NewError("invalid type: %T", v)
		}
	}
}

func (m *Mapper) NewNestedValue(v interface{}, dt schema.DataType, parent schema.PropertyInterface, prop search.SelectProperty) (*pb.Value, error) {
	if v == nil {
		return m.NewNilValue(), nil
	}
	switch dt {
	case schema.DataTypeObject:
		if _, ok := v.(map[string]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected map[string]interface{}", v)
		}
		obj, err := m.newObject(v.(map[string]interface{}), parent, prop)
		if err != nil {
			return nil, errors.Wrap(err, "creating nested object")
		}
		return NewObjectValue(obj), nil
	case schema.DataTypeObjectArray:
		if _, ok := v.([]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected []map[string]interface{}", v)
		}
		var list *pb.ListValue
		var err error
		if m.uses125 {
			list, err = m.newObjectList125(v.([]interface{}), parent, prop)
		} else {
			list, err = m.newObjectList123(v.([]interface{}), parent, prop)
		}
		if err != nil {
			return nil, errors.Wrap(err, "creating nested object array")
		}
		return newListValue(list), nil
	default:
		return nil, protoimpl.X.NewError("invalid type: %T", v)
	}
}

// NewObject constructs a Object from a general-purpose Go map.
// The map keys must be valid UTF-8.
// The map values are converted using NewValue.
func (m *Mapper) newObject(v map[string]interface{}, parent schema.PropertyInterface, selectProp search.SelectProperty) (*pb.Properties, error) {
	if !selectProp.IsObject {
		return nil, errors.New("select property is not an object")
	}
	x := &pb.Properties{Fields: make(map[string]*pb.Value, len(v))}
	for _, selectProp := range selectProp.Props {
		val, ok := v[selectProp.Name]
		if !ok {
			continue
		}

		dt, err := schema.GetNestedPropertyDataType(parent, selectProp.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "getting data type of nested property %s", selectProp.Name)
		}
		if *dt == schema.DataTypeObject || *dt == schema.DataTypeObjectArray {
			nested, err := schema.GetNestedPropertyByName(parent, selectProp.Name)
			if err != nil {
				return nil, errors.Wrapf(err, "getting nested property %s", selectProp.Name)
			}
			x.Fields[selectProp.Name], err = m.NewNestedValue(val, *dt, &NestedProperty{NestedProperty: nested}, selectProp)
			if err != nil {
				return nil, errors.Wrapf(err, "creating nested object value %s", selectProp.Name)
			}
		} else {
			x.Fields[selectProp.Name], err = m.NewPrimitiveValue(val, *dt)
			if err != nil {
				return nil, errors.Wrapf(err, "creating nested primitive value %s", selectProp.Name)
			}
		}
	}
	return x, nil
}

func parseArray[T float64 | bool | string](v interface{}, dt schema.DataType) ([]T, error) {
	val, ok := v.([]T)
	if !ok {
		return nil, protoimpl.X.NewError("invalid type: %T when serializing %v", v, dt.String())
	}
	return val, nil
}

func (m *Mapper) newListValueBool(v interface{}) (*pb.Value, error) {
	var listValue *pb.ListValue
	makeListValue := func(v []bool) *pb.ListValue {
		return &pb.ListValue{Kind: &pb.ListValue_BoolValues{BoolValues: &pb.BoolValues{Values: v}}}
	}
	if _, ok := v.([]interface{}); ok {
		if m.uses125 {
			listValue = makeListValue([]bool{})
		} else {
			listValue = &pb.ListValue{Values: []*pb.Value{}}
		}
	} else {
		values, err := parseArray[bool](v, schema.DataTypeBooleanArray)
		if err != nil {
			return nil, err
		}
		if m.uses125 {
			listValue = makeListValue(values)
		} else {
			x := make([]*pb.Value, len(values))
			for i, v := range values {
				x[i] = NewBoolValue(v)
			}
			listValue = &pb.ListValue{Values: x}
		}
	}
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: listValue}}, nil
}

func (m *Mapper) newListValueDate(v interface{}) (*pb.Value, error) {
	var listValue *pb.ListValue
	makeListValue := func(v []string) *pb.ListValue {
		return &pb.ListValue{Kind: &pb.ListValue_DateValues{DateValues: &pb.DateValues{Values: v}}}
	}
	if _, ok := v.([]interface{}); ok {
		if m.uses125 {
			listValue = makeListValue([]string{})
		} else {
			listValue = &pb.ListValue{Values: []*pb.Value{}}
		}
	} else {
		values, err := parseArray[string](v, schema.DataTypeDateArray)
		if err != nil {
			return nil, err
		}
		if m.uses125 {
			listValue = makeListValue(values)
		} else {
			x := make([]*pb.Value, len(values))
			for i, v := range values {
				x[i] = NewDateValue(v)
			}
			listValue = &pb.ListValue{Values: x}
		}
	}
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: listValue}}, nil
}

func (m *Mapper) newListValueNumber(v interface{}) (*pb.Value, error) {
	var listValue *pb.ListValue
	makeListValue := func(v []float64) *pb.ListValue {
		return &pb.ListValue{Kind: &pb.ListValue_NumberValues{NumberValues: &pb.NumberValues{Values: byteops.Fp64SliceToBytes(v)}}}
	}
	if _, ok := v.([]interface{}); ok {
		if m.uses125 {
			listValue = makeListValue([]float64{})
		} else {
			listValue = &pb.ListValue{Values: []*pb.Value{}}
		}
	} else {
		values, err := parseArray[float64](v, schema.DataTypeNumberArray)
		if err != nil {
			return nil, err
		}
		if m.uses125 {
			listValue = makeListValue(values)
		} else {
			x := make([]*pb.Value, len(values))
			for i, v := range values {
				x[i] = NewNumberValue(v)
			}
			listValue = &pb.ListValue{Values: x}
		}
	}
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: listValue}}, nil
}

func (m *Mapper) newListValueInt(v interface{}) (*pb.Value, error) {
	var listValue *pb.ListValue
	makeListValue := func(v []float64) *pb.ListValue {
		return &pb.ListValue{Kind: &pb.ListValue_IntValues{IntValues: &pb.IntValues{Values: byteops.IntsToByteVector((v))}}}
	}
	if _, ok := v.([]interface{}); ok {
		if m.uses125 {
			listValue = makeListValue([]float64{})
		} else {
			listValue = &pb.ListValue{Values: []*pb.Value{}}
		}
	} else {
		values, err := parseArray[float64](v, schema.DataTypeIntArray)
		if err != nil {
			return nil, err
		}
		if m.uses125 {
			listValue = makeListValue(values)
		} else {
			x := make([]*pb.Value, len(values))
			for i, v := range values {
				x[i] = NewIntValue(int64(v))
			}
			listValue = &pb.ListValue{Values: x}
		}
	}
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: listValue}}, nil
}

func (m *Mapper) newListValueText(v interface{}) (*pb.Value, error) {
	var listValue *pb.ListValue
	makeListValue := func(v []string) *pb.ListValue {
		return &pb.ListValue{Kind: &pb.ListValue_TextValues{TextValues: &pb.TextValues{Values: v}}}
	}
	if _, ok := v.([]interface{}); ok {
		if m.uses125 {
			listValue = makeListValue([]string{})
		} else {
			listValue = &pb.ListValue{Values: []*pb.Value{}}
		}
	} else {
		values, err := parseArray[string](v, schema.DataTypeTextArray)
		if err != nil {
			return nil, err
		}
		if m.uses125 {
			listValue = makeListValue(values)
		} else {
			x := make([]*pb.Value, len(values))
			for i, v := range values {
				x[i] = NewStringValue(v)
			}
			listValue = &pb.ListValue{Values: x}
		}
	}
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: listValue}}, nil
}

func (m *Mapper) newListValueUuid(v interface{}) (*pb.Value, error) {
	var listValue *pb.ListValue
	makeListValue := func(v []string) *pb.ListValue {
		return &pb.ListValue{Kind: &pb.ListValue_UuidValues{UuidValues: &pb.UuidValues{Values: v}}}
	}
	if _, ok := v.([]interface{}); ok {
		if m.uses125 {
			listValue = makeListValue([]string{})
		} else {
			listValue = &pb.ListValue{Values: []*pb.Value{}}
		}
	} else {
		values, err := parseArray[string](v, schema.DataTypeUUIDArray)
		if err != nil {
			return nil, err
		}
		if m.uses125 {
			listValue = makeListValue(values)
		} else {
			x := make([]*pb.Value, len(values))
			for i, v := range values {
				x[i] = NewUuidValue(v)
			}
			listValue = &pb.ListValue{Values: x}
		}
	}
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: listValue}}, nil
}

func (m *Mapper) parsePrimitiveArray(v interface{}, dt, innerDt schema.DataType) (*pb.Value, error) {
	switch dt {
	case schema.DataTypeBooleanArray:
		return m.newListValueBool(v)
	case schema.DataTypeDateArray:
		return m.newListValueDate(v)
	case schema.DataTypeNumberArray:
		return m.newListValueNumber(v)
	case schema.DataTypeIntArray:
		return m.newListValueInt(v)
	case schema.DataTypeStringArray:
		return m.newListValueText(v)
	case schema.DataTypeTextArray:
		return m.newListValueText(v)
	case schema.DataTypeUUIDArray:
		return m.newListValueUuid(v)
	default:
		return nil, protoimpl.X.NewError("invalid type: %T", v)
	}
}

func (m *Mapper) newObjectList123(v []interface{}, parent schema.PropertyInterface, selectProp search.SelectProperty) (*pb.ListValue, error) {
	if !selectProp.IsObject {
		return nil, errors.New("select property is not an object")
	}
	x := &pb.ListValue{Values: make([]*pb.Value, len(v))}
	for i, v := range v {
		if _, ok := v.(map[string]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected map[string]interface{}", v)
		}
		value, err := m.newObject(v.(map[string]interface{}), parent, selectProp)
		if err != nil {
			return nil, err
		}
		x.Values[i] = NewObjectValue(value)
	}
	return x, nil
}

func (m *Mapper) newObjectList125(v []interface{}, parent schema.PropertyInterface, selectProp search.SelectProperty) (*pb.ListValue, error) {
	if !selectProp.IsObject {
		return nil, errors.New("select property is not an object")
	}
	x := make([]*pb.Properties, len(v))
	for i, v := range v {
		if _, ok := v.(map[string]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected map[string]interface{}", v)
		}
		value, err := m.newObject(v.(map[string]interface{}), parent, selectProp)
		if err != nil {
			return nil, err
		}
		x[i] = value
	}
	return &pb.ListValue{Kind: &pb.ListValue_ObjectValues{ObjectValues: &pb.ObjectValues{Values: x}}}, nil
}

// NewBoolValue constructs a new boolean Value.
func NewBoolValue(v bool) *pb.Value {
	return &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: v}}
}

// NewNumberValue constructs a new number Value.
func NewNumberValue(v float64) *pb.Value {
	return &pb.Value{Kind: &pb.Value_NumberValue{NumberValue: v}}
}

// NewIntValue constructs a new number Value.
func NewIntValue(v int64) *pb.Value {
	return &pb.Value{Kind: &pb.Value_IntValue{IntValue: v}}
}

// NewStringValue constructs a new string Value.
func NewStringValue(v string) *pb.Value {
	return &pb.Value{Kind: &pb.Value_StringValue{StringValue: v}}
}

func NewTextValue(v string) *pb.Value {
	return &pb.Value{Kind: &pb.Value_TextValue{TextValue: v}}
}

// NewDateValue constructs a new string Value.
func NewDateValue(v string) *pb.Value {
	return &pb.Value{Kind: &pb.Value_DateValue{DateValue: v}}
}

// NewUuidValue constructs a new string Value.
func NewUuidValue(v string) *pb.Value {
	return &pb.Value{Kind: &pb.Value_UuidValue{UuidValue: v}}
}

// NewGeoValue constructs a new geo Value.
func NewGeoValue(v *models.GeoCoordinates) *pb.Value {
	return &pb.Value{Kind: &pb.Value_GeoValue{GeoValue: &pb.GeoCoordinate{Latitude: *v.Latitude, Longitude: *v.Longitude}}}
}

// NewObjectValue constructs a new struct Value.
func NewObjectValue(v *pb.Properties) *pb.Value {
	return &pb.Value{Kind: &pb.Value_ObjectValue{ObjectValue: v}}
}

// NewListValue constructs a new list Value.
func newListValue(v *pb.ListValue) *pb.Value {
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: v}}
}

// NewBlobValue constructs a new blob Value.
func newBlobValue(v string) *pb.Value {
	return &pb.Value{Kind: &pb.Value_BlobValue{BlobValue: v}}
}

// NewPhoneNumberValue constructs a new phone number Value.
func newPhoneNumberValue(v *models.PhoneNumber) *pb.Value {
	return &pb.Value{Kind: &pb.Value_PhoneValue{
		PhoneValue: &pb.PhoneNumber{
			CountryCode:            v.CountryCode,
			DefaultCountry:         v.DefaultCountry,
			Input:                  v.Input,
			InternationalFormatted: v.InternationalFormatted,
			National:               v.National,
			NationalFormatted:      v.NationalFormatted,
			Valid:                  v.Valid,
		},
	}}
}

// NewNilValue constructs a new nil Value.
func (m *Mapper) NewNilValue() *pb.Value {
	return &pb.Value{Kind: &pb.Value_NullValue{}}
}
