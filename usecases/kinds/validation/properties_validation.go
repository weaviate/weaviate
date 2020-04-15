//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

const (
	// ErrorInvalidSingleRef message
	ErrorInvalidSingleRef string = "only direct references supported at the moment, concept references not supported yet: class '%s' with property '%s' requires exactly 1 arguments: 'beacon'. Check your input schema, got: %#v"
	// ErrorMissingSingleRefCRef message
	ErrorMissingSingleRefCRef string = "only direct references supported at the moment, concept references not supported yet:  class '%s' with property '%s' requires exactly 1 argument: 'beacon' is missing, check your input schema"
	// ErrorCrefInvalidURI message
	ErrorCrefInvalidURI string = "class '%s' with property '%s' is not a valid URI: %s"
	// ErrorCrefInvalidURIPath message
	ErrorCrefInvalidURIPath string = "class '%s' with property '%s' does not contain a valid path, must have 2 segments: /<kind>/<id>"
	// ErrorMissingSingleRefLocationURL message
	ErrorMissingSingleRefLocationURL string = "class '%s' with property '%s' requires exactly 3 arguments: 'beacon', 'locationUrl' and 'type'. 'locationUrl' is missing, check your input schema"
	// ErrorMissingSingleRefType message
	ErrorMissingSingleRefType string = "class '%s' with property '%s' requires exactly 3 arguments: 'beacon', 'locationUrl' and 'type'. 'type' is missing, check your input schema"
)

func (v *Validator) properties(ctx context.Context, k kind.Kind, object interface{}) error {
	var isp interface{}
	var vectorWeights interface{}

	var className string
	if k == kind.Action {
		className = object.(*models.Action).Class
		isp = object.(*models.Action).Schema
		vectorWeights = object.(*models.Action).VectorWeights
	} else if k == kind.Thing {
		className = object.(*models.Thing).Class
		isp = object.(*models.Thing).Schema
		vectorWeights = object.(*models.Thing).VectorWeights
	} else {
		return fmt.Errorf(schema.ErrorInvalidRefType)
	}

	class := v.schema.GetClass(k, schema.ClassName(className))
	if class == nil {
		return fmt.Errorf("class '%s' not present in schema", className)
	}

	if vectorWeights != nil {
		res, err := v.validateVectorWeights(vectorWeights)
		if err != nil {
			return fmt.Errorf("vector weights: %v", err)
		}

		vectorWeights = res
	}

	if isp == nil {
		// no properties means nothing to validate
		return nil
	}

	inputSchema := isp.(map[string]interface{})
	returnSchema := map[string]interface{}{}

	for propertyKey, propertyValue := range inputSchema {
		dataType, err := schema.GetPropertyDataType(class, propertyKey)
		if err != nil {
			return err
		}

		data, err := v.extractAndValidateProperty(ctx, propertyKey, propertyValue, className, dataType)
		if err != nil {
			return err
		}

		returnSchema[propertyKey] = data
	}

	if k == kind.Action {
		object.(*models.Action).Schema = returnSchema
		object.(*models.Action).VectorWeights = vectorWeights
	} else if k == kind.Thing {
		object.(*models.Thing).Schema = returnSchema
		object.(*models.Thing).VectorWeights = vectorWeights
	} else {
		return fmt.Errorf(schema.ErrorInvalidRefType)
	}

	return nil
}

func (v *Validator) extractAndValidateProperty(ctx context.Context, propertyName string, pv interface{},
	className string, dataType *schema.DataType) (interface{}, error) {
	var (
		data interface{}
		err  error
	)

	switch *dataType {
	case schema.DataTypeCRef:
		data, err = v.cRef(ctx, propertyName, pv, className)
		if err != nil {
			return nil, fmt.Errorf("invalid cref: %s", err)
		}
	case schema.DataTypeString:
		data, err = stringVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid string property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeText:
		data, err = stringVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid text property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeInt:
		data, err = intVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid integer property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeNumber:
		data, err = numberVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid number property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeBoolean:
		data, err = boolVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeDate:
		data, err = dateVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid date property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeGeoCoordinates:
		data, err = geoCoordinates(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid geoCoordinates property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypePhoneNumber:
		data, err = phoneNumber(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid phoneNumber property '%s' on class '%s': %s", propertyName, className, err)
		}

	default:
		return nil, fmt.Errorf("unrecognized data type '%s'", *dataType)
	}

	return data, nil
}

func (v *Validator) cRef(ctx context.Context, propertyName string, pv interface{},
	className string) (interface{}, error) {
	switch refValue := pv.(type) {
	case map[string]interface{}:
		return nil, fmt.Errorf("reference must be an array, but got a map: %#v", refValue)
	case []interface{}:
		crefs := models.MultipleRef{}
		for _, ref := range refValue {

			refTyped, ok := ref.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("Multiple references in %s.%s should be a list of maps, but we got: %T",
					className, propertyName, ref)
			}

			cref, err := v.parseAndValidateSingleRef(ctx, propertyName, refTyped, className)
			if err != nil {
				return nil, err
			}

			crefs = append(crefs, cref)
		}

		return crefs, nil
	default:
		return nil, fmt.Errorf("invalid ref type. Needs to be []map, got %T", pv)
	}
}

func stringVal(val interface{}) (string, error) {
	typed, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("not a string, but %T", val)
	}

	return typed, nil
}

func boolVal(val interface{}) (bool, error) {
	typed, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("not a bool, but %T", val)
	}

	return typed, nil
}

func dateVal(val interface{}) (time.Time, error) {
	var data time.Time
	var err error
	var ok bool

	errorInvalidDate := "requires a string with a RFC3339 formatted date, but the given value is '%v'"

	var dateString string
	if dateString, ok = val.(string); !ok {
		return time.Time{}, fmt.Errorf(errorInvalidDate, val)
	}

	// Parse the time as this has to be correct
	data, err = time.Parse(time.RFC3339, dateString)

	// Return if there is an error while parsing
	if err != nil {
		return time.Time{}, fmt.Errorf(errorInvalidDate, val)
	}

	return data, nil
}

func intVal(val interface{}) (interface{}, error) {
	var data interface{}
	var ok bool
	var err error

	errInvalidInteger := "requires an integer, the given value is '%v'"
	errInvalidIntegerConvertion := "the JSON number '%v' could not be converted to an int"

	// Return err when the input can not be casted to json.Number
	if data, ok = val.(json.Number); !ok {
		// If value is not a json.Number, it could be an int, which is fine
		if data, ok = val.(int64); !ok {
			// If value is not a json.Number, it could be an int, which is fine when the float does not contain a decimal
			if data, ok = val.(float64); ok {
				// Check whether the float is containing a decimal
				if data != float64(int64(data.(float64))) {
					return nil, fmt.Errorf(errInvalidInteger, val)
				}
			} else {
				// If it is not a float, it is cerntainly not a integer, return the err
				return nil, fmt.Errorf(errInvalidInteger, val)
			}
		}
	} else if data, err = val.(json.Number).Int64(); err != nil {
		// Return err when the input can not be converted to an int
		return nil, fmt.Errorf(errInvalidIntegerConvertion, val)
	}

	return data, nil
}

func numberVal(val interface{}) (interface{}, error) {
	var data interface{}
	var ok bool
	var err error

	errInvalidFloat := "requires a float, the given value is '%v'"
	errInvalidFloatConvertion := "the JSON number '%v' could not be converted to a float."

	if data, ok = val.(json.Number); !ok {
		if data, ok = val.(float64); !ok {
			return nil, fmt.Errorf(errInvalidFloat, val)
		}
	} else if data, err = val.(json.Number).Float64(); err != nil {
		return nil, fmt.Errorf(errInvalidFloatConvertion, val)
	}

	return data, nil
}

func geoCoordinates(input interface{}) (*models.GeoCoordinates, error) {
	inputMap, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("geoCoordinates must be a map, but got: %T", input)
	}

	lon, ok := inputMap["longitude"]
	if !ok {
		return nil, fmt.Errorf("geoCoordinates is missing required field 'longitude'")
	}

	lat, ok := inputMap["latitude"]
	if !ok {
		return nil, fmt.Errorf("geoCoordinates is missing required field 'latitude'")
	}

	lonFloat, err := parseCoordinate(lon)
	if err != nil {
		return nil, fmt.Errorf("invalid longitude: %s", err)
	}

	latFloat, err := parseCoordinate(lat)
	if err != nil {
		return nil, fmt.Errorf("invalid latitude: %s", err)
	}

	return &models.GeoCoordinates{
		Longitude: ptFloat32(float32(lonFloat)),
		Latitude:  ptFloat32(float32(latFloat)),
	}, nil
}

func ptFloat32(in float32) *float32 {
	return &in
}

func phoneNumber(data interface{}) (*models.PhoneNumber, error) {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("phoneNumber must be a map, but got: %T", data)
	}

	input, ok := dataMap["input"]
	if !ok {
		return nil, fmt.Errorf("phoneNumber is missing required field 'input'")
	}

	inputString, ok := input.(string)
	if !ok {
		return nil, fmt.Errorf("phoneNumber.input must be a string")
	}

	var defaultCountryString string
	defaultCountry, ok := dataMap["defaultCountry"]
	if !ok {
		defaultCountryString = ""
	} else {
		defaultCountryString, ok = defaultCountry.(string)
		if !ok {
			return nil, fmt.Errorf("phoneNumber.defaultCountry must be a string")
		}
	}

	return parsePhoneNumber(inputString, defaultCountryString)
}

func parseCoordinate(raw interface{}) (float64, error) {
	switch v := raw.(type) {
	case json.Number:
		asFloat, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("cannot interpret as float: %s", err)
		}
		return asFloat, nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("must be json.Number or float, but got %T", raw)
	}
}

func (v *Validator) parseAndValidateSingleRef(ctx context.Context, propertyName string,
	pvcr map[string]interface{}, className string) (*models.SingleRef, error) {

	if _, ok := pvcr["href"]; ok {
		// delete read only field href
		delete(pvcr, "href")
	}

	// Return different types of errors for cref input
	if len(pvcr) != 1 {
		// Give an error if the cref is not filled with correct number of properties
		return nil, fmt.Errorf(
			ErrorInvalidSingleRef,
			className,
			propertyName,
			pvcr,
		)
	} else if _, ok := pvcr["beacon"]; !ok {
		// Give an error if the cref is not filled with correct properties (beacon)
		return nil, fmt.Errorf(
			ErrorMissingSingleRefCRef,
			className,
			propertyName,
		)
	}

	ref, err := crossref.Parse(pvcr["beacon"].(string))
	if err != nil {
		return nil, fmt.Errorf("invalid reference: %s", err)
	}
	errVal := fmt.Sprintf("'cref' %s %s:%s", ref.Kind.Name(), className, propertyName)
	err = v.ValidateSingleRef(ctx, ref.SingleRef(), errVal)
	if err != nil {
		return nil, err
	}

	// Validate whether reference exists based on given Type
	return ref.SingleRef(), nil
}

// vectorWeights are passed as a non-typed interface{}, this is due to a
// limition in go-swagger which itself is coming from swagger 2.0 which does
// not have support for arbitrary key/value objects
//
// we must thus validate that it's a map and they keys are strings
// NOTE: We are not validating the semantic correctness of the equations
// themselves, as they are in the contextinoary's resopnsibility
func (v *Validator) validateVectorWeights(in interface{}) (map[string]string, error) {

	asMap, ok := in.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("must be key/value object with strings as keys and values, got %#v", in)
	}

	out := make(map[string]string, len(asMap))
	for key, value := range asMap {
		asString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("key '%s': incorrect datatype: must be string, got %T", key, value)
		}

		out[key] = asString
	}

	return out, nil
}
