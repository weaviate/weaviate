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

package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
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

func (v *Validator) properties(ctx context.Context, class *models.Class,
	incomingObject *models.Object, existingObject *models.Object,
) error {
	className := incomingObject.Class
	isp := incomingObject.Properties
	vectorWeights := incomingObject.VectorWeights
	tenant := incomingObject.Tenant

	if existingObject != nil && tenant != existingObject.Tenant {
		return fmt.Errorf("tenant mismatch, expected %s but got %s", existingObject.Tenant, tenant)
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

	inputSchema, ok := isp.(map[string]interface{})
	if !ok {
		return fmt.Errorf("could not recognize object's properties: %v", isp)
	}
	returnSchema := map[string]interface{}{}

	for propertyKey, propertyValue := range inputSchema {
		if propertyValue == nil {
			continue // nil values are removed and filtered out
		}

		// properties in the class are saved with lower case first letter
		propertyKeyLowerCase := strings.ToLower(propertyKey[:1])
		if len(propertyKey) > 1 {
			propertyKeyLowerCase += propertyKey[1:]
		}
		property, err := schema.GetPropertyByName(class, propertyKeyLowerCase)
		if err != nil {
			return err
		}
		dataType, err := schema.GetPropertyDataType(class, propertyKeyLowerCase)
		if err != nil {
			return err
		}

		// autodetect to_class in references
		if dataType.String() == schema.DataTypeCRef.String() {
			propertyValueSlice, ok := propertyValue.([]interface{})
			if !ok {
				return fmt.Errorf("reference property is not a slice %v", propertyValue)
			}
			for i := range propertyValueSlice {
				propertyValueMap, ok := propertyValueSlice[i].(map[string]interface{})
				if !ok {
					return fmt.Errorf("reference property is not a map %v", propertyValueMap)
				}
				beacon := propertyValueMap["beacon"].(string)
				beaconParsed, err := crossref.Parse(beacon)
				if err != nil {
					return err
				}

				if beaconParsed.Class == "" {
					prop, err := schema.GetPropertyByName(class, schema.LowercaseFirstLetter(propertyKey))
					if err != nil {
						return err
					}
					if len(prop.DataType) > 1 {
						continue
					}
					toClass := prop.DataType[0] // datatype is the name of the class that is referenced
					toBeacon := crossref.NewLocalhost(toClass, beaconParsed.TargetID).String()

					propertyValue.([]interface{})[i].(map[string]interface{})["beacon"] = toBeacon
				}
			}
		}

		var data interface{}
		if schema.IsNested(*dataType) {
			data, err = v.extractAndValidateNestedProperty(ctx, propertyKeyLowerCase, propertyValue, className,
				dataType, property.NestedProperties)
		} else {
			data, err = v.extractAndValidateProperty(ctx, propertyKeyLowerCase, propertyValue, className, dataType, tenant)
		}
		if err != nil {
			return err
		}

		returnSchema[propertyKeyLowerCase] = data
	}

	incomingObject.Properties = returnSchema
	incomingObject.VectorWeights = vectorWeights

	return nil
}

func nestedPropertiesToMap(nestedProperties []*models.NestedProperty) map[string]*models.NestedProperty {
	nestedPropertiesMap := map[string]*models.NestedProperty{}
	for _, nestedProperty := range nestedProperties {
		nestedPropertiesMap[nestedProperty.Name] = nestedProperty
	}
	return nestedPropertiesMap
}

// TODO nested
// refactor/simplify + improve recurring error msgs on nested properties
func (v *Validator) extractAndValidateNestedProperty(ctx context.Context, propertyName string,
	val interface{}, className string, dataType *schema.DataType, nestedProperties []*models.NestedProperty,
) (interface{}, error) {
	var data interface{}
	var err error

	switch *dataType {
	case schema.DataTypeObject:
		data, err = objectVal(ctx, v, val, propertyName, className, nestedPropertiesToMap(nestedProperties))
		if err != nil {
			return nil, fmt.Errorf("invalid object property '%s' on class '%s': %w", propertyName, className, err)
		}
	case schema.DataTypeObjectArray:
		data, err = objectArrayVal(ctx, v, val, propertyName, className, nestedPropertiesToMap(nestedProperties))
		if err != nil {
			return nil, fmt.Errorf("invalid object[] property '%s' on class '%s': %w", propertyName, className, err)
		}
	default:
		return nil, fmt.Errorf("unrecognized data type '%s'", *dataType)
	}

	return data, nil
}

func objectVal(ctx context.Context, v *Validator, val interface{}, propertyPrefix string,
	className string, nestedPropertiesMap map[string]*models.NestedProperty,
) (map[string]interface{}, error) {
	typed, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("object must be a map, but got: %T", val)
	}

	for nestedKey, nestedValue := range typed {
		propertyName := propertyPrefix + "." + nestedKey
		nestedProperty, ok := nestedPropertiesMap[nestedKey]
		if !ok {
			return nil, fmt.Errorf("unknown property '%s'", propertyName)
		}

		nestedDataType, err := schema.GetValueDataTypeFromString(nestedProperty.DataType[0])
		if err != nil {
			return nil, fmt.Errorf("property '%s': %w", propertyName, err)
		}

		var data interface{}
		if schema.IsNested(*nestedDataType) {
			data, err = v.extractAndValidateNestedProperty(ctx, propertyName, nestedValue,
				className, nestedDataType, nestedProperty.NestedProperties)
		} else {
			data, err = v.extractAndValidateProperty(ctx, propertyName, nestedValue,
				className, nestedDataType, "")
			// tenant isn't relevant for nested properties since crossrefs are not allowed
		}
		if err != nil {
			return nil, fmt.Errorf("property '%s': %w", propertyName, err)
		}
		typed[nestedKey] = data
	}

	return typed, nil
}

func objectArrayVal(ctx context.Context, v *Validator, val interface{}, propertyPrefix string,
	className string, nestedPropertiesMap map[string]*models.NestedProperty,
) (interface{}, error) {
	typed, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("not an object array, but %T", val)
	}

	for i := range typed {
		data, err := objectVal(ctx, v, typed[i], propertyPrefix, className, nestedPropertiesMap)
		if err != nil {
			return nil, fmt.Errorf("invalid object '%d' in array: %w", i, err)
		}
		typed[i] = data
	}

	return typed, nil
}

func (v *Validator) extractAndValidateProperty(ctx context.Context, propertyName string, pv interface{},
	className string, dataType *schema.DataType, tenant string,
) (interface{}, error) {
	var (
		data interface{}
		err  error
	)

	switch *dataType {
	case schema.DataTypeCRef:
		data, err = v.cRef(ctx, propertyName, pv, className, tenant)
		if err != nil {
			return nil, fmt.Errorf("invalid cref: %s", err)
		}
	case schema.DataTypeText:
		data, err = stringVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid text property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeUUID:
		asStr, err := stringVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid uuid property '%s' on class '%s': %s", propertyName, className, err)
		}

		data, err = uuid.Parse(asStr)
		if err != nil {
			return nil, fmt.Errorf("invalid uuid property '%s' on class '%s': %s", propertyName, className, err)
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
	case schema.DataTypeBlob:
		data, err = blobVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid blob property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeTextArray:
		data, err = stringArrayVal(pv, "text")
		if err != nil {
			return nil, fmt.Errorf("invalid text array property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeIntArray:
		data, err = intArrayVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid integer array property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeNumberArray:
		data, err = numberArrayVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid number array property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeBooleanArray:
		data, err = boolArrayVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean array property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeDateArray:
		data, err = dateArrayVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid date array property '%s' on class '%s': %s", propertyName, className, err)
		}
	case schema.DataTypeUUIDArray:
		data, err = ParseUUIDArray(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid uuid array property '%s' on class '%s': %s", propertyName, className, err)
		}
	// deprecated string
	case schema.DataTypeString:
		data, err = stringVal(pv)
		if err != nil {
			return nil, fmt.Errorf("invalid string property '%s' on class '%s': %s", propertyName, className, err)
		}
	// deprecated string
	case schema.DataTypeStringArray:
		data, err = stringArrayVal(pv, "string")
		if err != nil {
			return nil, fmt.Errorf("invalid string array property '%s' on class '%s': %s", propertyName, className, err)
		}

	default:
		return nil, fmt.Errorf("unrecognized data type '%s'", *dataType)
	}

	return data, nil
}

func (v *Validator) cRef(ctx context.Context, propertyName string, pv interface{},
	className, tenant string,
) (interface{}, error) {
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

			cref, err := v.parseAndValidateSingleRef(ctx, propertyName, refTyped, className, tenant)
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
	if _, ok = val.(json.Number); !ok {
		// If value is not a json.Number, it could be an int, which is fine
		if data, ok = val.(int64); !ok {
			// If value is not a json.Number, it could be an int, which is fine when the float does not contain a decimal
			if data, ok = val.(float64); ok {
				// Check whether the float is containing a decimal
				if data != float64(int64(data.(float64))) {
					return nil, fmt.Errorf(errInvalidInteger, val)
				}
			} else {
				// If it is not a float, it is certainly not a integer, return the err
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

	if _, ok = val.(json.Number); !ok {
		if data, ok = val.(float64); !ok {
			data64, ok := val.(int64)
			if !ok {
				return nil, fmt.Errorf(errInvalidFloat, val)
			}
			data = float64(data64)
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

func blobVal(val interface{}) (string, error) {
	typed, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("not a blob base64 string, but %T", val)
	}

	base64Regex := regexp.MustCompile(`^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})$`)
	ok = base64Regex.MatchString(typed)
	if !ok {
		return "", fmt.Errorf("not a valid blob base64 string")
	}

	return typed, nil
}

func (v *Validator) parseAndValidateSingleRef(ctx context.Context, propertyName string,
	pvcr map[string]interface{}, className, tenant string,
) (*models.SingleRef, error) {
	delete(pvcr, "href")

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
	errVal := fmt.Sprintf("'cref' %s:%s", className, propertyName)
	ref, err = v.ValidateSingleRef(ref.SingleRef())
	if err != nil {
		return nil, err
	}

	if err = v.ValidateExistence(ctx, ref, errVal, tenant); err != nil {
		return nil, err
	}

	// Validate whether reference exists based on given Type
	return ref.SingleRef(), nil
}

// vectorWeights are passed as a non-typed interface{}, this is due to a
// limitation in go-swagger which itself is coming from swagger 2.0 which does
// not have support for arbitrary key/value objects
//
// we must thus validate that it's a map and they keys are strings
// NOTE: We are not validating the semantic correctness of the equations
// themselves, as they are in the contextinoary's responsibility
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

func stringArrayVal(val interface{}, typeName string) ([]interface{}, error) {
	typed, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("not a %s array, but %T", typeName, val)
	}

	for i := range typed {
		if _, err := stringVal(typed[i]); err != nil {
			return nil, fmt.Errorf("invalid %s array value: %s", typeName, val)
		}
	}

	return typed, nil
}

func intArrayVal(val interface{}) ([]interface{}, error) {
	typed, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("not an integer array, but %T", val)
	}

	for i := range typed {
		if _, err := intVal(typed[i]); err != nil {
			return nil, fmt.Errorf("invalid integer array value: %s", val)
		}
	}

	return typed, nil
}

func numberArrayVal(val interface{}) ([]interface{}, error) {
	typed, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("not an integer array, but %T", val)
	}

	for i := range typed {
		data, err := numberVal(typed[i])
		if err != nil {
			return nil, fmt.Errorf("invalid integer array value: %s", val)
		}
		typed[i] = data
	}

	return typed, nil
}

func boolArrayVal(val interface{}) ([]interface{}, error) {
	typed, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("not a boolean array, but %T", val)
	}

	for i := range typed {
		if _, err := boolVal(typed[i]); err != nil {
			return nil, fmt.Errorf("invalid boolean array value: %s", val)
		}
	}

	return typed, nil
}

func dateArrayVal(val interface{}) ([]interface{}, error) {
	typed, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("not a date array, but %T", val)
	}

	for i := range typed {
		if _, err := dateVal(typed[i]); err != nil {
			return nil, fmt.Errorf("invalid date array value: %s", val)
		}
	}

	return typed, nil
}

func ParseUUIDArray(in any) ([]uuid.UUID, error) {
	var err error

	if parsed, ok := in.([]uuid.UUID); ok {
		return parsed, nil
	}

	asSlice, ok := in.([]any)
	if !ok {
		return nil, fmt.Errorf("not a slice type: %T", in)
	}

	d := make([]uuid.UUID, len(asSlice))
	for i, elem := range asSlice {
		asUUID, ok := elem.(uuid.UUID)
		if ok {
			d[i] = asUUID
			continue
		}

		asStr, ok := elem.(string)
		if !ok {
			return nil, fmt.Errorf("array element neither uuid.UUID nor str, but: %T", elem)
		}

		d[i], err = uuid.Parse(asStr)
		if err != nil {
			return nil, fmt.Errorf("at pos %d: %w", i, err)
		}
	}

	return d, nil
}
