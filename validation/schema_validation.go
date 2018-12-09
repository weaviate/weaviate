/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-openapi/strfmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
)

const (
	// ErrorInvalidSingleRef message
	ErrorInvalidSingleRef string = "class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. Check your input schema"
	// ErrorMissingSingleRefCRef message
	ErrorMissingSingleRefCRef string = "class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. '$cref' is missing, check your input schema"
	// ErrorMissingSingleRefLocationURL message
	ErrorMissingSingleRefLocationURL string = "class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'locationUrl' is missing, check your input schema"
	// ErrorMissingSingleRefType message
	ErrorMissingSingleRefType string = "class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'type' is missing, check your input schema"
	// ErrorInvalidClassType message
	ErrorInvalidClassType string = "class '%s' with property '%s' requires one of the following values in 'type': '%s', '%s' or '%s'"
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
func ValidateSchemaInBody(ctx context.Context, weaviateSchema *models.SemanticSchema, object interface{}, refType connutils.RefType, dbConnector dbconnector.DatabaseConnector, serverConfig *config.WeaviateConfig, keyToken *models.KeyTokenGetResponse) error {
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
				pvcr := refValue

				// Return different types of errors for cref input
				if len(pvcr) != 3 {
					// Give an error if the cref is not filled with correct number of properties
					return fmt.Errorf(
						ErrorInvalidSingleRef,
						class.Class,
						pk,
					)
				} else if _, ok := pvcr["$cref"]; !ok {
					// Give an error if the cref is not filled with correct properties ($cref)
					return fmt.Errorf(
						ErrorMissingSingleRefCRef,
						class.Class,
						pk,
					)
				} else if _, ok := pvcr["locationUrl"]; !ok {
					// Give an error if the cref is not filled with correct properties (locationUrl)
					return fmt.Errorf(
						ErrorMissingSingleRefLocationURL,
						class.Class,
						pk,
					)
				} else if _, ok := pvcr["type"]; !ok {
					// Give an error if the cref is not filled with correct properties (type)
					return fmt.Errorf(
						ErrorMissingSingleRefType,
						class.Class,
						pk,
					)
				}

				// Return error if type is not right, when it is not one of the 3 possible types
				refType := pvcr["type"].(string)
				if !validateRefType(connutils.RefType(refType)) {
					return fmt.Errorf(
						ErrorInvalidClassType,
						class.Class, pk,
						connutils.RefTypeAction,
						connutils.RefTypeThing,
						connutils.RefTypeKey,
					)
				}

				// Validate whether reference exists based on given Type
				cref := &models.SingleRef{}
				cref.Type = refType
				locationURL := pvcr["locationUrl"].(string)
				cref.LocationURL = &locationURL
				cref.NrDollarCref = strfmt.UUID(pvcr["$cref"].(string))
				err = ValidateSingleRef(ctx, serverConfig, cref, dbConnector, fmt.Sprintf("'cref' %s %s:%s", cref.Type, class.Class, pk), keyToken)
				if err != nil {
					return err
				}

				data = cref
			case []interface{}:
				crefs := models.MultipleRef{}
				for _, ref := range refValue {
					pvcr, ok := ref.(map[string]interface{})
					if !ok {
						return fmt.Errorf("Multipleref should be a list")
					}

					// Return different types of errors for cref input
					if len(pvcr) != 3 {
						// Give an error if the cref is not filled with correct number of properties
						return fmt.Errorf(
							ErrorInvalidSingleRef,
							class.Class,
							pk,
						)
					} else if _, ok := pvcr["$cref"]; !ok {
						// Give an error if the cref is not filled with correct properties ($cref)
						return fmt.Errorf(
							ErrorMissingSingleRefCRef,
							class.Class,
							pk,
						)
					} else if _, ok := pvcr["locationUrl"]; !ok {
						// Give an error if the cref is not filled with correct properties (locationUrl)
						return fmt.Errorf(
							ErrorMissingSingleRefLocationURL,
							class.Class,
							pk,
						)
					} else if _, ok := pvcr["type"]; !ok {
						// Give an error if the cref is not filled with correct properties (type)
						return fmt.Errorf(
							ErrorMissingSingleRefType,
							class.Class,
							pk,
						)
					}

					// Return error if type is not right, when it is not one of the 3 possible types
					refType := pvcr["type"].(string)
					if !validateRefType(connutils.RefType(refType)) {
						return fmt.Errorf(
							ErrorInvalidClassType,
							class.Class, pk,
							connutils.RefTypeAction,
							connutils.RefTypeThing,
							connutils.RefTypeKey,
						)
					}

					// Validate whether reference exists based on given Type
					cref := &models.SingleRef{}
					cref.Type = refType
					locationURL := pvcr["locationUrl"].(string)
					cref.LocationURL = &locationURL
					cref.NrDollarCref = strfmt.UUID(pvcr["$cref"].(string))
					err = ValidateSingleRef(ctx, serverConfig, cref, dbConnector, fmt.Sprintf("'cref' %s %s:%s", cref.Type, class.Class, pk), keyToken)
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
