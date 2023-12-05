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

package protocol

import (
	"math"

	"google.golang.org/protobuf/encoding/protojson"
)

// AsInterface converts x to a general-purpose Go interface.
//
// Calling Value.MarshalJSON and "encoding/json".Marshal on this output produce
// semantically equivalent JSON (assuming no errors occur).
//
// Floating-point values (i.e., "NaN", "Infinity", and "-Infinity") are
// converted as strings to remain compatible with MarshalJSON.
func (x *Value) AsInterface() interface{} {
	switch v := x.GetKind().(type) {
	case *Value_NumberValue:
		if v != nil {
			switch {
			case math.IsNaN(v.NumberValue):
				return "NaN"
			case math.IsInf(v.NumberValue, +1):
				return "Infinity"
			case math.IsInf(v.NumberValue, -1):
				return "-Infinity"
			default:
				return v.NumberValue
			}
		}
	case *Value_StringValue:
		if v != nil {
			return v.StringValue
		}
	case *Value_BoolValue:
		if v != nil {
			return v.BoolValue
		}
	case *Value_ObjectValue:
		if v != nil {
			return v.ObjectValue.AsMap()
		}
	case *Value_ListValue:
		if v != nil {
			return v.ListValue.AsSlice()
		}
	}
	return nil
}

func (x *Value) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(x)
}

func (x *Value) UnmarshalJSON(b []byte) error {
	return protojson.Unmarshal(b, x)
}

// AsSlice converts x to a general-purpose Go slice.
// The slice elements are converted by calling Value.AsInterface.
func (x *ListValue) AsSlice() []interface{} {
	vals := x.GetValues()
	vs := make([]interface{}, len(vals))
	for i, v := range vals {
		vs[i] = v.AsInterface()
	}
	return vs
}

func (x *ListValue) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(x)
}

func (x *ListValue) UnmarshalJSON(b []byte) error {
	return protojson.Unmarshal(b, x)
}

// AsMap converts x to a general-purpose Go map.
// The map values are converted by calling Value.AsInterface.
func (x *Struct) AsMap() map[string]interface{} {
	f := x.GetFields()
	vs := make(map[string]interface{}, len(f))
	for k, v := range f {
		vs[k] = v.AsInterface()
	}
	return vs
}

func (x *Struct) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(x)
}

func (x *Struct) UnmarshalJSON(b []byte) error {
	return protojson.Unmarshal(b, x)
}
