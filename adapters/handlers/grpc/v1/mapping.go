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
	"google.golang.org/protobuf/runtime/protoimpl"
)

func parseArray[T float64 | bool | string](v interface{}, innerDt schema.DataType) (*pb.Value, error) {
	if _, ok := v.([]interface{}); ok {
		return &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{}}}}, nil
	}
	val, ok := v.([]T)
	if !ok {
		return nil, protoimpl.X.NewError("invalid type: %T when serializing %v", v, innerDt.String())
	}
	list, err := NewPrimitiveList(val, innerDt)
	if err != nil {
		return nil, errors.Wrapf(err, "serializing array with type %v", innerDt)
	}
	return NewListValue(list), nil
}

func NewPrimitiveValue(v interface{}, dt schema.DataType) (*pb.Value, error) {
	innerDt, ok := schema.IsArrayType(dt)
	if ok {
		switch dt {
		case schema.DataTypeBooleanArray:
			return parseArray[bool](v, innerDt)
		case schema.DataTypeDateArray:
			return parseArray[string](v, innerDt)
		case schema.DataTypeNumberArray:
			return parseArray[float64](v, innerDt)
		case schema.DataTypeIntArray:
			return parseArray[float64](v, innerDt)
		case schema.DataTypeStringArray:
			return parseArray[string](v, innerDt)
		case schema.DataTypeTextArray:
			return parseArray[string](v, innerDt)
		case schema.DataTypeUUIDArray:
			return parseArray[string](v, innerDt)
		default:
			return nil, protoimpl.X.NewError("invalid type: %T", v)
		}
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
				return nil, protoimpl.X.NewError("invalid type: %T expected int64 when serializing int property", v)
			}
			return NewIntValue(int64(val)), nil
		case schema.DataTypeString:
			val, ok := v.(string)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected string when serializing string property", v)
			}
			return NewStringValue(val), nil
		case schema.DataTypeText:
			val, ok := v.(string)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected string when serializing text property", v)
			}
			return NewStringValue(val), nil
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
			return NewBlobValue(val), nil
		case schema.DataTypePhoneNumber:
			val, ok := v.(*models.PhoneNumber)
			if !ok {
				return nil, protoimpl.X.NewError("invalid type: %T expected *models.PhoneNumber when serializing phone number property", v)
			}
			return NewPhoneNumberValue(val), nil
		default:
			return nil, protoimpl.X.NewError("invalid type: %T", v)
		}
	}
}

func NewNestedValue[P schema.PropertyInterface](v interface{}, dt schema.DataType, parent P, prop search.SelectProperty) (*pb.Value, error) {
	switch dt {
	case schema.DataTypeObject:
		if _, ok := v.(map[string]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected map[string]interface{}", v)
		}
		obj, err := NewObject(v.(map[string]interface{}), parent, prop)
		if err != nil {
			return nil, errors.Wrap(err, "creating nested object")
		}
		return NewObjectValue(obj), nil
	case schema.DataTypeObjectArray:
		if _, ok := v.([]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected []map[string]interface{}", v)
		}
		list, err := NewObjectList(v.([]interface{}), parent, prop)
		if err != nil {
			return nil, errors.Wrap(err, "creating nested object array")
		}
		return NewListValue(list), nil
	default:
		return nil, protoimpl.X.NewError("invalid type: %T", v)
	}
}

// NewStruct constructs a Struct from a general-purpose Go map.
// The map keys must be valid UTF-8.
// The map values are converted using NewValue.
func NewObject[P schema.PropertyInterface](v map[string]interface{}, parent P, selectProp search.SelectProperty) (*pb.Properties, error) {
	if !selectProp.IsObject {
		return nil, errors.New("select property is not an object")
	}
	x := &pb.Properties{Fields: make(map[string]*pb.Value, len(v))}
	for _, selectProp := range selectProp.Props {
		dt, err := schema.GetNestedPropertyDataType(parent, selectProp.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "getting data type of nested property %s", selectProp.Name)
		}
		if *dt == schema.DataTypeObject || *dt == schema.DataTypeObjectArray {
			nested, err := schema.GetNestedPropertyByName(parent, selectProp.Name)
			if err != nil {
				return nil, errors.Wrapf(err, "getting nested property %s", selectProp.Name)
			}
			x.Fields[selectProp.Name], err = NewNestedValue(v[selectProp.Name], *dt, &NestedProperty{NestedProperty: nested}, selectProp)
			if err != nil {
				return nil, errors.Wrapf(err, "creating nested object value %s", selectProp.Name)
			}
		} else {
			x.Fields[selectProp.Name], err = NewPrimitiveValue(v[selectProp.Name], *dt)
			if err != nil {
				return nil, errors.Wrapf(err, "creating nested primitive value %s", selectProp.Name)
			}
		}
		if err != nil {
			return nil, errors.Wrapf(err, "serializing field %s", selectProp.Name)
		}
	}
	return x, nil
}

// NewList constructs a ListValue from a general-purpose Go slice.
// The slice elements are converted using NewValue.
func NewPrimitiveList[T bool | float64 | string](v []T, dt schema.DataType) (*pb.ListValue, error) {
	var err error
	x := &pb.ListValue{Values: make([]*pb.Value, len(v))}
	for i, v := range v {
		x.Values[i], err = NewPrimitiveValue(v, dt)
		if err != nil {
			return nil, err
		}
	}
	return x, nil
}

// NewList constructs a ListValue from a general-purpose Go slice.
// The slice elements are converted using NewValue.
func NewObjectList[P schema.PropertyInterface](v []interface{}, parent P, selectProp search.SelectProperty) (*pb.ListValue, error) {
	if !selectProp.IsObject {
		return nil, errors.New("select property is not an object")
	}
	x := &pb.ListValue{Values: make([]*pb.Value, len(v))}
	for i, v := range v {
		if _, ok := v.(map[string]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected map[string]interface{}", v)
		}
		value, err := NewObject(v.(map[string]interface{}), parent, selectProp)
		if err != nil {
			return nil, err
		}
		x.Values[i] = NewObjectValue(value)
	}
	return x, nil
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
func NewListValue(v *pb.ListValue) *pb.Value {
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: v}}
}

// NewBlobValue constructs a new blob Value.
func NewBlobValue(v string) *pb.Value {
	return &pb.Value{Kind: &pb.Value_BlobValue{BlobValue: v}}
}

// NewPhoneNumberValue constructs a new phone number Value.
func NewPhoneNumberValue(v *models.PhoneNumber) *pb.Value {
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
