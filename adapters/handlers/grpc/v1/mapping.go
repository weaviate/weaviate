//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package v1

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/runtime/protoimpl"
)

func parseArray[T float64 | bool | string](v interface{}, dt schema.DataType) (*pb.Value, error) {
	val, ok := v.([]T)
	if !ok {
		return nil, protoimpl.X.NewError("invalid type: %T when serializing %v", v, dt.String())
	}
	list, err := NewPrimitiveList(val, dt)
	if err != nil {
		return nil, errors.Wrapf(err, "serializing array with type %v", dt)
	}
	return NewListValue(list), nil
}

// NewValue constructs a Value from a general-purpose Go interface.
//
//	╔════════════════════════╤════════════════════════════════════════════╗
//	║ Go type                │ Conversion                                 ║
//	╠════════════════════════╪════════════════════════════════════════════╣
//	║ bool                   │ stored as BoolValue                        ║
//	║ int, int32, int64      │ stored as NumberValue                      ║
//	║ uint, uint32, uint64   │ stored as NumberValue                      ║
//	║ float32, float64       │ stored as NumberValue                      ║
//	║ string                 │ stored as StringValue; must be valid UTF-8 ║
//	║ []byte                 │ stored as StringValue; base64-encoded      ║
//	║ map[string]interface{} │ stored as StructValue                      ║
//	║ []interface{}          │ stored as ListValue                        ║
//	╚════════════════════════╧════════════════════════════════════════════╝
//
// When converting an int64 or uint64 to a NumberValue, numeric precision loss
// is possible since they are stored as a float64.
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
		default:
			return nil, protoimpl.X.NewError("invalid type: %T", v)
		}
	}
}

func NewNestedValue[P schema.PropertyInterface](v interface{}, dt schema.DataType, prop P) (*pb.Value, error) {
	switch dt {
	case schema.DataTypeObject:
		if _, ok := v.(map[string]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected map[string]interface{}", v)
		}
		obj, err := NewObject(v.(map[string]interface{}), prop)
		if err != nil {
			return nil, errors.Wrap(err, "creating nested object")
		}
		return NewObjectValue(obj), nil
	case schema.DataTypeObjectArray:
		if _, ok := v.([]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected []map[string]interface{}", v)
		}
		list, err := NewObjectList(v.([]interface{}), prop)
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
func NewObject[P schema.PropertyInterface](v map[string]interface{}, prop P) (*pb.Struct, error) {
	x := &pb.Struct{Fields: make(map[string]*pb.Value, len(v))}
	for _, nested := range prop.GetNestedProperties() {
		dt, err := schema.GetNestedPropertyDataType(prop, nested.Name)
		if err != nil {
			return nil, err
		}
		if *dt == schema.DataTypeObject || *dt == schema.DataTypeObjectArray {
			x.Fields[nested.Name], err = NewNestedValue(v[nested.Name], *dt, &NestedProperty{NestedProperty: nested})
		} else {
			x.Fields[nested.Name], err = NewPrimitiveValue(v[nested.Name], *dt)
		}
		if err != nil {
			return nil, err
		}
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

// NewObjectValue constructs a new struct Value.
func NewObjectValue(v *pb.Struct) *pb.Value {
	return &pb.Value{Kind: &pb.Value_ObjectValue{ObjectValue: v}}
}

// NewListValue constructs a new list Value.
func NewListValue(v *pb.ListValue) *pb.Value {
	return &pb.Value{Kind: &pb.Value_ListValue{ListValue: v}}
}

// NewList constructs a ListValue from a general-purpose Go slice.
// The slice elements are converted using NewValue.
func NewPrimitiveList[T bool | float64 | string](v []T, dt schema.DataType) (*pb.ListValue, error) {
	x := &pb.ListValue{Values: make([]*pb.Value, len(v))}
	for i, v := range v {
		var err error
		x.Values[i], err = NewPrimitiveValue(v, dt)
		if err != nil {
			return nil, err
		}
	}
	return x, nil
}

// NewList constructs a ListValue from a general-purpose Go slice.
// The slice elements are converted using NewValue.
func NewObjectList[P schema.PropertyInterface](v []interface{}, prop P) (*pb.ListValue, error) {
	x := &pb.ListValue{Values: make([]*pb.Value, len(v))}
	for i, v := range v {
		if _, ok := v.(map[string]interface{}); !ok {
			return nil, protoimpl.X.NewError("invalid type: %T expected map[string]interface{}", v)
		}
		value, err := NewObject(v.(map[string]interface{}), prop)
		if err != nil {
			return nil, err
		}
		x.Values[i] = NewObjectValue(value)
	}
	return x, nil
}
