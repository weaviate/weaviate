/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package validation

import (
	"encoding/json"
	"fmt"
	"github.com/go-openapi/strfmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

// ValidateSchemaInBody Validate the schema in the given body
func ValidateSchemaInBody(weaviateSchema *models.SemanticSchema, bodySchema *models.Schema, className string, dbConnector dbconnector.DatabaseConnector) error {
	// Validate whether the class exists in the given schema
	// Get the class by its name
	class, err := schema.GetClassByName(weaviateSchema, className)

	// Return the error, in this case that the class is not found
	if err != nil {
		return err
	}

	// Validate whether the properties exist in the given schema
	// Get the input properties from the bodySchema in readable format
	isp := *bodySchema
	inputSchema := isp.(map[string]interface{})

	// For each property in the input schema
	for pk, pv := range inputSchema {
		// Get the property data type from the schema
		dt, err := schema.GetPropertyDataType(class, pk)

		// Return the error, in this case that the property is not found
		if err != nil {
			return err
		}

		// Check whether the datatypes are correct
		if *dt == schema.DataTypeCRef {
			// Cast it to a usable variable
			pvcr := pv.(map[string]interface{})

			// Return different types of errors for cref input
			if len(pvcr) != 3 {
				// Give an error if the cref is not filled with correct number of properties
				return fmt.Errorf(
					"class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. Check your input schema",
					class.Class,
					pk,
				)
			} else if _, ok := pvcr["$cref"]; !ok {
				// Give an error if the cref is not filled with correct properties ($cref)
				return fmt.Errorf(
					"class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. '$cref' is missing, check your input schema",
					class.Class,
					pk,
				)
			} else if _, ok := pvcr["locationUrl"]; !ok {
				// Give an error if the cref is not filled with correct properties (locationUrl)
				return fmt.Errorf(
					"class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'locationUrl' is missing, check your input schema",
					class.Class,
					pk,
				)
			} else if _, ok := pvcr["type"]; !ok {
				// Give an error if the cref is not filled with correct properties (type)
				return fmt.Errorf(
					"class '%s' with property '%s' requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'type' is missing, check your input schema",
					class.Class,
					pk,
				)
			}

			// Return error if type is not right, when it is not one of the 3 possible types
			refType := pvcr["type"].(string)
			if !validateRefType(refType) {
				return fmt.Errorf(
					"class '%s' with property '%s' requires one of the following values in 'type': '%s', '%s' or '%s'",
					class.Class, pk,
					connutils.RefTypeAction,
					connutils.RefTypeThing,
					connutils.RefTypeKey,
				)
			}

			// TODO: https://github.com/creativesoftwarefdn/weaviate/issues/237 Validate using / existing locationURL?
			// Validate whether reference exists based on given Type
			crefu := strfmt.UUID(pvcr["$cref"].(string))
			if refType == connutils.RefTypeAction {
				ar := &models.ActionGetResponse{}
				are := dbConnector.GetAction(crefu, ar)
				if are != nil {
					return fmt.Errorf("error finding the 'cref' to an Action in the database: %s", are)
				}
			} else if refType == connutils.RefTypeKey {
				kr := &models.KeyTokenGetResponse{}
				kre := dbConnector.GetKey(crefu, kr)
				if kre != nil {
					return fmt.Errorf("error finding the 'cref' to a Key in the database: %s", kre)
				}
			} else if refType == connutils.RefTypeThing {
				tr := &models.ThingGetResponse{}
				tre := dbConnector.GetThing(crefu, tr)
				if tre != nil {
					return fmt.Errorf("error finding the 'cref' to a Thing in the database: %s", tre)
				}
			}
		} else if *dt == schema.DataTypeString {
			// Return error when the input can not be casted to a string
			if _, ok := pv.(string); !ok {
				return fmt.Errorf(
					"class '%s' with property '%s' requires a string. The given value is '%v'",
					class.Class,
					pk,
					pv,
				)
			}
		} else if *dt == schema.DataTypeInt {
			// Return error when the input can not be casted to json.Number
			if _, ok := pv.(json.Number); !ok {
				// If value is not a json.Number, it could be an int, which is fine
				if _, ok := pv.(int64); !ok {
					// If value is not a json.Number, it could be an int, which is fine when the float does not contain a decimal
					if vFloat, ok := pv.(float64); ok {
						// Check whether the float is containing a decimal
						if vFloat != float64(int64(vFloat)) {
							return fmt.Errorf(
								"class '%s' with property '%s' requires an integer. The given value is '%v'",
								class.Class,
								pk,
								pv,
							)
						}
					} else {
						// If it is not a float, it is cerntainly not a integer, return the error
						return fmt.Errorf(
							"class '%s' with property '%s' requires an integer. The given value is '%v'",
							class.Class,
							pk,
							pv,
						)
					}
				}
			} else if _, err := pv.(json.Number).Int64(); err != nil {
				// Return error when the input can not be converted to an int
				return fmt.Errorf(
					"class '%s' with property '%s' requires an integer, the JSON number could not be converted to an int. The given value is '%v'",
					class.Class,
					pk,
					pv,
				)
			}

		} else if *dt == schema.DataTypeNumber {
			// Return error when the input can not be casted to json.Number
			if _, ok := pv.(json.Number); !ok {
				if _, ok := pv.(float64); !ok {
					return fmt.Errorf(
						"class '%s' with property '%s' requires a float. The given value is '%v'",
						class.Class,
						pk,
						pv,
					)
				}
			} else if _, err := pv.(json.Number).Float64(); err != nil {
				// Return error when the input can not be converted to a float
				return fmt.Errorf(
					"class '%s' with property '%s' requires a float, the JSON number could not be converted to a float. The given value is '%v'",
					class.Class,
					pk,
					pv,
				)
			}
		} else if *dt == schema.DataTypeBoolean {
			// Return error when the input can not be casted to a boolean
			if _, ok := pv.(bool); !ok {
				return fmt.Errorf(
					"class '%s' with property '%s' requires a bool. The given value is '%v'",
					class.Class,
					pk,
					pv,
				)
			}
		} else if *dt == schema.DataTypeDate {
			// Return error when the input can not be casted to a string
			if _, ok := pv.(string); !ok {
				return fmt.Errorf(
					"class '%s' with property '%s' requires a string with a RFC3339 formatted date. The given value is '%v'",
					class.Class,
					pk,
					pv,
				)
			}

			// Parse the time as this has to be correct
			_, err := time.Parse(time.RFC3339, pv.(string))

			// Return if there is an error while parsing
			if err != nil {
				return fmt.Errorf(
					"class '%s' with property '%s' requires a string with a RFC3339 formatted date. The given value is '%v'",
					class.Class,
					pk,
					pv,
				)
			}
		}
	}

	return nil
}
