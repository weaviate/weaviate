//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package storobj

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
)

func (ko *Object) enrichSchemaTypes(schema map[string]interface{}) error {
	if schema == nil {
		return nil
	}

	for propName, value := range schema {
		switch typed := value.(type) {
		case []interface{}:
			parsed, err := parseCrossRef(typed)
			if err != nil {
				return errors.Wrapf(err, "property %q of type cross-ref", propName)
			}

			schema[propName] = parsed
		case map[string]interface{}:
			parsed, err := parseMapProp(typed)
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

func parseMapProp(input map[string]interface{}) (interface{}, error) {
	lat, latOK := input["latitude"]
	lon, lonOK := input["longitude"]
	_, phoneInputOK := input["input"]

	if latOK && lonOK {
		// this is a geoCoordinates prop
		return parseGeoProp(lat, lon)
	}

	if phoneInputOK {
		// this is a phone number
		return parsePhoneNumber(input)
	}

	return nil, fmt.Errorf("unknown map prop which is not a geo prop or phone: %v", input)
}

func parseGeoProp(lat interface{}, lon interface{}) (*models.GeoCoordinates, error) {
	latFloat, ok := lat.(float64)
	if !ok {
		return nil, fmt.Errorf("explected lat to be float64, but is %T", lat)
	}

	lonFloat, ok := lon.(float64)
	if !ok {
		return nil, fmt.Errorf("explected lon to be float64, but is %T", lon)
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
			// TODO: gh-1150 support underscore RefMeta
		}
	}

	return parsed, nil
}
