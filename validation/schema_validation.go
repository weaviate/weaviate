/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/config"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/crossref"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network"
)

const (
	// ErrorInvalidSingleRef message
	ErrorInvalidSingleRef string = "class '%s' with property '%s' requires exactly 1 arguments: '$cref'. Check your input schema, got: %#v"
	// ErrorMissingSingleRefCRef message
	ErrorMissingSingleRefCRef string = "class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. '$cref' is missing, check your input schema"
	// ErrorCrefInvalidURI message
	ErrorCrefInvalidURI string = "class '%s' with property '%s' is not a valid URI: %s"
	// ErrorCrefInvalidURIPath message
	ErrorCrefInvalidURIPath string = "class '%s' with property '%s' does not contain a valid path, must have 2 segments: /<kind>/<id>"
	// ErrorMissingSingleRefLocationURL message
	ErrorMissingSingleRefLocationURL string = "class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'locationUrl' is missing, check your input schema"
	// ErrorMissingSingleRefType message
	ErrorMissingSingleRefType string = "class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'type' is missing, check your input schema"
	// ErrorInvalidString message
	ErrorInvalidString string = "class '%s' with property '%s' requires a string. The given value is '%v'"
	// ErrorInvalidText message
	ErrorInvalidText string = "class '%s' with property '%s' requires a text (string). The given value is '%v'"
	// ErrorInvalidInteger message
	ErrorInvalidInteger string = "class '%s' with property '%s' requires an integer. The given value is '%v'"
	// ErrorInvalidIntegerConvertion message
	ErrorInvalidIntegerConvertion string = ErrorInvalidInteger + "The JSON number could not be converted to an int."
	// ErrorInvalidFloat message
	ErrorInvalidFloat string = "class '%s' with property '%s' requires a float. The given value is '%v'"
	// ErrorInvalidFloatConvertion message
	ErrorInvalidFloatConvertion string = ErrorInvalidFloat + "The JSON number could not be converted to a float."
	// ErrorInvalidBool message
	ErrorInvalidBool string = "class '%s' with property '%s' requires a bool. The given value is '%v'"
	// ErrorInvalidDate message
	ErrorInvalidDate string = "class '%s' with property '%s' requires a string with a RFC3339 formatted date. The given value is '%v'"
)

// ValidateSchemaInBody Validate the schema in the given body
func ValidateSchemaInBody(ctx context.Context, weaviateSchema *models.SemanticSchema, object interface{},
	refType connutils.RefType, dbConnector dbconnector.DatabaseConnector, network network.Network,
	serverConfig *config.WeaviateConfig) error {
	// Initialize class object
	var isp interface{}
	var className string
	if refType == connutils.RefTypeAction {
		className = object.(*models.ActionCreate).AtClass
		isp = object.(*models.ActionCreate).Schema
	} else if refType == connutils.RefTypeThing {
		className = object.(*models.ThingCreate).AtClass
		isp = object.(*models.ThingCreate).Schema
	} else {
		return fmt.Errorf(schema.ErrorInvalidRefType)
	}

	// Validate whether the class exists in the given schema
	// Get the class by its name
	class, err := schema.GetClassByName(weaviateSchema, className)

	// Return the error, in this case that the class is not found
	if err != nil {
		return err
	}

	// Validate whether the properties exist in the given schema
	// Get the input properties from the bodySchema in readable format
	if isp == nil {
		return nil
	}

	inputSchema := isp.(map[string]interface{})

	// Init variable for schema-data to put back in the object
	returnSchema := map[string]interface{}{}

	// For each property in the input schema
	for pk, pv := range inputSchema {
		// Get the property data type from the schema
		dt, err := schema.GetPropertyDataType(class, pk)

		// Return the error, in this case that the property is not found
		if err != nil {
			return err
		}

		var data interface{}

		// Check whether the datatypes are correct
		if *dt == schema.DataTypeCRef {
			switch refValue := pv.(type) {
			case map[string]interface{}:
				cref, err := parseAndValidateSingleRef(ctx, dbConnector, network, serverConfig, refValue, class.Class, pk)
				if err != nil {
					return err
				}

				data = cref
			case []interface{}:
				crefs := models.MultipleRef{}
				for _, ref := range refValue {

					refTyped, ok := ref.(map[string]interface{})
					if !ok {
						return fmt.Errorf("Multiple references in %s.%s should be a list of maps, but we got: %t",
							class.Class, pk, ref)
					}

					cref, err := parseAndValidateSingleRef(ctx, dbConnector, network, serverConfig, refTyped, class.Class, pk)
					if err != nil {
						return err
					}

					crefs = append(crefs, cref)
				}

				data = crefs
			}
		} else if *dt == schema.DataTypeString {
			// Return error when the input can not be casted to a string
			var ok bool
			data, ok = pv.(string)
			if !ok {
				return fmt.Errorf(
					ErrorInvalidString,
					class.Class,
					pk,
					pv,
				)
			}
		} else if *dt == schema.DataTypeText {
			// Return error when the input can not be casted to a string
			var ok bool
			data, ok = pv.(string)
			if !ok {
				return fmt.Errorf(
					ErrorInvalidText,
					class.Class,
					pk,
					pv,
				)
			}
		} else if *dt == schema.DataTypeInt {
			var ok bool
			// Return error when the input can not be casted to json.Number
			if data, ok = pv.(json.Number); !ok {
				// If value is not a json.Number, it could be an int, which is fine
				if data, ok = pv.(int64); !ok {
					// If value is not a json.Number, it could be an int, which is fine when the float does not contain a decimal
					if data, ok = pv.(float64); ok {
						// Check whether the float is containing a decimal
						if data != float64(int64(data.(float64))) {
							return fmt.Errorf(
								ErrorInvalidInteger,
								class.Class,
								pk,
								pv,
							)
						}
					} else {
						// If it is not a float, it is cerntainly not a integer, return the error
						return fmt.Errorf(
							ErrorInvalidInteger,
							class.Class,
							pk,
							pv,
						)
					}
				}
			} else if data, err = pv.(json.Number).Int64(); err != nil {
				// Return error when the input can not be converted to an int
				return fmt.Errorf(
					ErrorInvalidIntegerConvertion,
					class.Class,
					pk,
					pv,
				)
			}

		} else if *dt == schema.DataTypeNumber {
			var ok bool
			// Return error when the input can not be casted to json.Number
			if data, ok = pv.(json.Number); !ok {
				if data, ok = pv.(float64); !ok {
					return fmt.Errorf(
						ErrorInvalidFloat,
						class.Class,
						pk,
						pv,
					)
				}
			} else if data, err = pv.(json.Number).Float64(); err != nil {
				// Return error when the input can not be converted to a float
				return fmt.Errorf(
					ErrorInvalidFloatConvertion,
					class.Class,
					pk,
					pv,
				)
			}
		} else if *dt == schema.DataTypeBoolean {
			var ok bool
			// Return error when the input can not be casted to a boolean
			if data, ok = pv.(bool); !ok {
				return fmt.Errorf(
					ErrorInvalidBool,
					class.Class,
					pk,
					pv,
				)
			}
		} else if *dt == schema.DataTypeDate {
			var ok bool
			// Return error when the input can not be casted to a string
			if data, ok = pv.(string); !ok {
				return fmt.Errorf(
					ErrorInvalidDate,
					class.Class,
					pk,
					pv,
				)
			}

			// Parse the time as this has to be correct
			data, err = time.Parse(time.RFC3339, pv.(string))

			// Return if there is an error while parsing
			if err != nil {
				return fmt.Errorf(
					ErrorInvalidDate,
					class.Class,
					pk,
					pv,
				)
			}
		} else if *dt == schema.DataTypeGeoCoordinates {
			data, err = geoCoordinate(pv)
			if err != nil {
				return fmt.Errorf("invalid field '%s': %s", pk, err)
			}

		} else {
			return fmt.Errorf("unrecoginzed data type '%s'", *dt)
		}
		// Put the right and validated types into the schema.
		returnSchema[pk] = data
	}

	// Put the right and validated types into the object-schema.
	if refType == connutils.RefTypeAction {
		object.(*models.ActionCreate).Schema = returnSchema
	} else if refType == connutils.RefTypeThing {
		object.(*models.ThingCreate).Schema = returnSchema
	} else {
		return fmt.Errorf(schema.ErrorInvalidRefType)
	}

	return nil
}

func geoCoordinate(input interface{}) (*models.GeoCoordinate, error) {
	inputMap, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("geoCoordinate must be a map, but got: %T", input)
	}

	lon, ok := inputMap["longitude"]
	if !ok {
		return nil, fmt.Errorf("geoCoordinate is missing required field 'longitude'")
	}

	lat, ok := inputMap["latitude"]
	if !ok {
		return nil, fmt.Errorf("geoCoordinate is missing required field 'latitude'")
	}

	lonNum, ok := lon.(json.Number)
	if !ok {
		return nil, fmt.Errorf("longitude must be a number, but got %T", lon)
	}

	latNum, ok := lat.(json.Number)
	if !ok {
		return nil, fmt.Errorf("latitude must be a number, but got %T", lat)
	}

	lonFloat, err := lonNum.Float64()
	if err != nil {
		return nil, fmt.Errorf("cannot interpret longitude as a float: %s", err)
	}

	latFloat, err := latNum.Float64()
	if err != nil {
		return nil, fmt.Errorf("cannot interpret latitude as a float: %s", err)
	}

	return &models.GeoCoordinate{
		Longitude: float32(lonFloat),
		Latitude:  float32(latFloat),
	}, nil
}

func parseAndValidateSingleRef(ctx context.Context, dbConnector dbconnector.DatabaseConnector, network network.Network,
	serverConfig *config.WeaviateConfig, pvcr map[string]interface{},
	className, propertyName string) (*models.SingleRef, error) {

	// Return different types of errors for cref input
	if len(pvcr) != 1 {
		// Give an error if the cref is not filled with correct number of properties
		return nil, fmt.Errorf(
			ErrorInvalidSingleRef,
			className,
			propertyName,
			pvcr,
		)
	} else if _, ok := pvcr["$cref"]; !ok {
		// Give an error if the cref is not filled with correct properties ($cref)
		return nil, fmt.Errorf(
			ErrorMissingSingleRefCRef,
			className,
			propertyName,
		)
	}

	ref, err := crossref.Parse(pvcr["$cref"].(string))
	if err != nil {
		return nil, fmt.Errorf("invalid reference: %s", err)
	}
	errVal := fmt.Sprintf("'cref' %s %s:%s", ref.Kind.Name(), className, propertyName)
	err = ValidateSingleRef(ctx, serverConfig, ref.SingleRef(), dbConnector, network, errVal)
	if err != nil {
		return nil, err
	}

	// Validate whether reference exists based on given Type
	return ref.SingleRef(), nil
}
