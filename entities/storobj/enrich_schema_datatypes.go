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
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func enrichSchemaTypes(schema map[string]interface{}, ofNestedProp bool) error {
	if schema == nil {
		return nil
	}

	for propName, value := range schema {
		switch typed := value.(type) {
		case []interface{}:
			if isArrayValue(typed) {
				switch typed[0].(type) {
				case float64:
					parsed, err := parseNumberArrayValue(typed)
					if err != nil {
						return errors.Wrapf(err, "property %q of type string array", propName)
					}

					schema[propName] = parsed
				case bool:
					parsed, err := parseBoolArrayValue(typed)
					if err != nil {
						return errors.Wrapf(err, "property %q of type boolean array", propName)
					}

					schema[propName] = parsed
				default:
					parsed, err := parseStringArrayValue(typed)
					if err != nil {
						return errors.Wrapf(err, "property %q of type string array", propName)
					}

					schema[propName] = parsed
				}
			} else if len(typed) == 0 {
				// empty arrays. Here we use []interface{} as a placeholder
				// type for an empty array, since we cannot determine its
				// actual type. in the future, we should persist the schema
				// property type information alongside the value to avoid
				// this situation
				schema[propName] = typed
			} else {
				// nested properties does not support refs
				if !ofNestedProp {
					parsed, err := parseCrossRef(typed)
					if err == nil {
						schema[propName] = parsed
						continue
					}
				}
				// apperently object[]
				for i := range typed {
					t2, ok := typed[i].(map[string]interface{})
					if !ok {
						return fmt.Errorf("expected element [%d] of '%s' to be map, %T found", i, propName, typed[i])
					}
					enrichSchemaTypes(t2, true)
				}
				schema[propName] = typed
			}
		case map[string]interface{}:
			parsed, err := parseMapProp(typed, ofNestedProp)
			if err != nil {
				return errors.Wrapf(err, "property %q of type map", propName)
			}

			schema[propName] = parsed
		default:
			continue
		}
	}

	return nil
}

// nested properties does not support phone or geo data types
func parseMapProp(input map[string]interface{}, ofNestedProp bool) (interface{}, error) {
	if !ofNestedProp && isGeoProp(input) {
		return parseGeoProp(input)
	}
	if !ofNestedProp && isPhoneProp(input) {
		return parsePhoneNumber(input)
	}
	// apparently object
	err := enrichSchemaTypes(input, true)
	return input, err
}

func isGeoProp(input map[string]interface{}) bool {
	expectedProps := []string{"latitude", "longitude"}

	if len(input) != len(expectedProps) {
		return false
	}
	for _, prop := range expectedProps {
		if _, ok := input[prop]; !ok {
			return false
		}
	}
	return true
}

func isPhoneProp(input map[string]interface{}) bool {
	validExpectedProps := [][]string{
		{"input", "internationalFormatted", "nationalFormatted", "national", "countryCode", "valid"},
		{"input", "internationalFormatted", "nationalFormatted", "national", "countryCode", "valid", "defaultCountry"},
		{"input", "internationalFormatted", "nationalFormatted", "national", "countryCode"},
		{"input", "internationalFormatted", "nationalFormatted", "national", "countryCode", "defaultCountry"},
	}

	for _, expectedProps := range validExpectedProps {
		match := true
		if len(expectedProps) != len(input) {
			match = false
		} else {
			for _, prop := range expectedProps {
				if _, ok := input[prop]; !ok {
					match = false
					break
				}
			}
		}
		if match {
			return true
		}
	}
	return false
}

func parseGeoProp(input map[string]interface{}) (*models.GeoCoordinates, error) {
	latFloat, ok := input["latitude"].(float64)
	if !ok {
		return nil, fmt.Errorf("explected lat to be float64, but is %T", input["latitude"])
	}

	lonFloat, ok := input["longitude"].(float64)
	if !ok {
		return nil, fmt.Errorf("explected lon to be float64, but is %T", input["longitude"])
	}

	return &models.GeoCoordinates{
		Latitude:  ptFloat32(float32(latFloat)),
		Longitude: ptFloat32(float32(lonFloat)),
	}, nil
}

func ptFloat32(in float32) *float32 {
	return &in
}

func parsePhoneNumber(input map[string]interface{}) (*models.PhoneNumber, error) {
	out := &models.PhoneNumber{}

	phoneInput, err := extractStringFromMap(input, "input")
	if err != nil {
		return nil, err
	}
	out.Input = phoneInput

	international, err := extractStringFromMap(input, "internationalFormatted")
	if err != nil {
		return nil, err
	}
	out.InternationalFormatted = international

	nationalFormatted, err := extractStringFromMap(input, "nationalFormatted")
	if err != nil {
		return nil, err
	}
	out.NationalFormatted = nationalFormatted

	national, err := extractNumberFromMap(input, "national")
	if err != nil {
		return nil, err
	}
	out.National = uint64(national)

	countryCode, err := extractNumberFromMap(input, "countryCode")
	if err != nil {
		return nil, err
	}
	out.CountryCode = uint64(countryCode)

	defaultCountry, err := extractStringFromMap(input, "defaultCountry")
	if err != nil {
		return nil, err
	}
	out.DefaultCountry = defaultCountry

	valid, err := extractBoolFromMap(input, "valid")
	if err != nil {
		return nil, err
	}
	out.Valid = valid

	return out, nil
}

func extractNumberFromMap(input map[string]interface{}, key string) (float64, error) {
	field, ok := input[key]
	if ok {
		asFloat, ok := field.(float64)
		if !ok {
			return 0, fmt.Errorf("expected '%s' to be float64, but is %T", key, field)
		}

		return asFloat, nil
	}
	return 0, nil
}

func extractStringFromMap(input map[string]interface{}, key string) (string, error) {
	field, ok := input[key]
	if ok {
		asString, ok := field.(string)
		if !ok {
			return "", fmt.Errorf("expected '%s' to be string, but is %T", key, field)
		}

		return asString, nil
	}
	return "", nil
}

func extractBoolFromMap(input map[string]interface{}, key string) (bool, error) {
	field, ok := input[key]
	if ok {
		asBool, ok := field.(bool)
		if !ok {
			return false, fmt.Errorf("expected '%s' to be bool, but is %T", key, field)
		}

		return asBool, nil
	}
	return false, nil
}

func isArrayValue(value []interface{}) bool {
	if len(value) > 0 {
		_, ok := value[0].(map[string]interface{})
		return !ok
	}
	return false
}

func parseStringArrayValue(value []interface{}) ([]string, error) {
	parsed := make([]string, len(value))
	for i := range value {
		asString, ok := value[i].(string)
		if !ok {
			return nil, fmt.Errorf("string array: expected element %d to be string - got %T", i, value[i])
		}
		parsed[i] = asString
	}
	return parsed, nil
}

func parseNumberArrayValue(value []interface{}) ([]float64, error) {
	parsed := make([]float64, len(value))
	for i := range value {
		asFloat, ok := value[i].(float64)
		if !ok {
			return nil, fmt.Errorf("number array: expected element %d to be float - got %T", i, value[i])
		}
		parsed[i] = asFloat
	}
	return parsed, nil
}

func parseBoolArrayValue(value []interface{}) ([]bool, error) {
	parsed := make([]bool, len(value))
	for i := range value {
		asBool, ok := value[i].(bool)
		if !ok {
			return nil, fmt.Errorf("boolean array: expected element %d to be bool - got %T", i, value[i])
		}
		parsed[i] = asBool
	}
	return parsed, nil
}

func parseCrossRef(value []interface{}) (models.MultipleRef, error) {
	parsed := make(models.MultipleRef, len(value))
	for i, elem := range value {
		asMap, ok := elem.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("crossref: expected element %d to be map - got %T", i, elem)
		}

		beacon, ok := asMap["beacon"]
		if !ok {
			return nil, fmt.Errorf("crossref: expected element %d to have key %q - got %v", i, "beacon", elem)
		}

		beaconStr, ok := beacon.(string)
		if !ok {
			return nil, fmt.Errorf("crossref: expected element %d.beacon to be string - got %T", i, beacon)
		}

		parsed[i] = &models.SingleRef{
			Beacon: strfmt.URI(beaconStr),
		}

		c, ok := asMap["classification"]
		if ok {
			classification, err := parseRefClassificationMeta(c)
			if err != nil {
				return nil, errors.Wrap(err, "crossref: parse classifiation meta")
			}

			parsed[i].Classification = classification
		}
	}

	return parsed, nil
}

func parseRefClassificationMeta(in interface{}) (*models.ReferenceMetaClassification, error) {
	out := &models.ReferenceMetaClassification{}
	asMap, ok := in.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected classification to be map - got %T", in)
	}

	if cod, err := extractFloat64(asMap, "closestOverallDistance"); err != nil {
		return nil, err
	} else {
		out.ClosestOverallDistance = cod
	}

	if mwd, err := extractFloat64(asMap, "meanWinningDistance"); err != nil {
		return nil, err
	} else {
		out.WinningDistance = mwd // deprecated remove in 0.23.0
		out.MeanWinningDistance = mwd
	}

	if cwd, err := extractFloat64(asMap, "closestWinningDistance"); err != nil {
		return nil, err
	} else {
		out.ClosestWinningDistance = cwd
	}

	if mcd, err := extractFloat64(asMap, "meanLosingDistance"); err != nil {
		return nil, err
	} else {
		out.LosingDistance = &mcd // deprecated remove in 0.23.0
		out.MeanLosingDistance = &mcd
	}

	if ccd, err := extractFloat64(asMap, "closestLosingDistance"); err != nil {
		return nil, err
	} else {
		out.ClosestLosingDistance = &ccd
	}

	if oc, err := extractFloat64(asMap, "overallCount"); err != nil {
		return nil, err
	} else {
		out.OverallCount = int64(oc)
	}

	if wc, err := extractFloat64(asMap, "winningCount"); err != nil {
		return nil, err
	} else {
		out.WinningCount = int64(wc)
	}

	if lc, err := extractFloat64(asMap, "losingCount"); err != nil {
		return nil, err
	} else {
		out.LosingCount = int64(lc)
	}

	return out, nil
}

func extractFloat64(source map[string]interface{}, key string) (float64, error) {
	value, ok := source[key]
	if !ok {
		return 0, nil
	}

	asFloat, ok := value.(float64)
	if !ok {
		return 0, fmt.Errorf("expected %s to be float64 - got %T", key, value)
	}

	return asFloat, nil
}
