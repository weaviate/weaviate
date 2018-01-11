/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @ CreativeSofwFdn / yourfriends@weaviate.com
 */

package schema

import (
	"encoding/json"
	errors_ "errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
)

type schemaProperties struct {
	localFile                string
	schemaLocationFromConfig string
	Schema                   *models.SemanticSchema
}

// WeaviateSchema represents the used schema's
type WeaviateSchema struct {
	ActionSchema schemaProperties
	ThingSchema  schemaProperties

	// The predicate dict to re-use for every schema (thing/action)
	predicateDict map[string]DataType

	messaging *messages.Messaging
}

// DataType is a representation of the predicate for queries
type DataType string

const (
	// DataTypeCRef The data type is a cross-reference, it is starting with a capital letter
	DataTypeCRef DataType = "cref"
	// DataTypeString The data type is a value of type string
	DataTypeString DataType = "string"
	// DataTypeInt The data type is a value of type int
	DataTypeInt DataType = "int"
	// DataTypeNumber The data type is a value of type number/float
	DataTypeNumber DataType = "number"
	// DataTypeBoolean The data type is a value of type boolean
	DataTypeBoolean DataType = "boolean"
	// DataTypeDate The data type is a value of type date
	DataTypeDate DataType = "date"
	// validationErrorMessage is a constant for returning the same message
	validationErrorMessage string = "All predicates with the same name across different classes should contain the same kind of data"
)

// GetClassByName returns the class by its name
func GetClassByName(s *models.SemanticSchema, className string) (*models.SemanticSchemaClass, error) {
	// For each class
	for _, class := range s.Classes {

		// Check if the name of the class is the given name, that's the class we need
		if class.Class == className {
			return class, nil
		}
	}

	return nil, fmt.Errorf("no such class with name '%s' found in the schema. Check your schema files for which classes are available", className)
}

// GetPropertyByName returns the class by its name
func GetPropertyByName(c *models.SemanticSchemaClass, propName string) (*models.SemanticSchemaClassProperty, error) {
	// For each class-property
	for _, prop := range c.Properties {

		// Check if the name of the property is the given name, that's the property we need
		if prop.Name == propName {
			return prop, nil
		}
	}

	return nil, fmt.Errorf("no such prop with name '%s' found in class '%s' in the schema. Check your schema files for which properties in this class are available", propName, c.Class)
}

// GetPropertyDataType checks whether the given string is a valid data type
func GetPropertyDataType(class *models.SemanticSchemaClass, propertyName string) (*DataType, error) {
	// Get the class-property
	prop, err := GetPropertyByName(class, propertyName)

	if err != nil {
		return nil, err
	}

	// Init the return value
	var returnDataType DataType

	// For each data type
	for _, dataType := range prop.AtDataType {
		// Get the first letter to see if it is a capital
		firstLetter := string(dataType[0])
		if strings.ToUpper(firstLetter) == firstLetter {
			returnDataType = DataTypeCRef
		} else {
			// Get the value-data type (non-cref), return error if there is one, otherwise assign it to return data type
			valueDataType, err := GetValueDataTypeFromString(dataType)
			if err != nil {
				return nil, err
			}
			returnDataType = *valueDataType
		}
	}
	return &returnDataType, nil
}

// GetValueDataTypeFromString checks whether the given string is a valid data type
func GetValueDataTypeFromString(dt string) (*DataType, error) {
	var returnDataType DataType

	if IsValidValueDataType(dt) {
		if dt == string(DataTypeBoolean) {
			returnDataType = DataTypeBoolean
		} else if dt == string(DataTypeInt) {
			returnDataType = DataTypeInt
		} else if dt == string(DataTypeDate) {
			returnDataType = DataTypeDate
		} else if dt == string(DataTypeNumber) {
			returnDataType = DataTypeNumber
		} else if dt == string(DataTypeString) {
			returnDataType = DataTypeString
		}
	} else {
		return nil, errors_.New("given value-DataType does not exist.")
	}

	return &returnDataType, nil
}

// IsValidValueDataType checks whether the given string is a valid data type
func IsValidValueDataType(dt string) bool {
	switch dt {
	case
		string(DataTypeString),
		string(DataTypeInt),
		string(DataTypeNumber),
		string(DataTypeBoolean),
		string(DataTypeDate):
		return true
	}
	return false
}

// LoadSchema from config locations
func (f *WeaviateSchema) LoadSchema(usedConfig *config.Environment, m *messages.Messaging) error {
	f.ThingSchema.schemaLocationFromConfig = usedConfig.Schemas.Thing
	f.ActionSchema.schemaLocationFromConfig = usedConfig.Schemas.Action
	f.predicateDict = map[string]DataType{}

	configFiles := map[string]*schemaProperties{
		"Action": &f.ActionSchema,
		"Thing":  &f.ThingSchema,
	}

	for cfk, cfv := range configFiles {
		// Continue loop if the file is not set in the config.
		if len(cfv.schemaLocationFromConfig) == 0 {
			return errors_.New("schema file for '" + cfk + "' not given in config (path: *env*/schemas/" + cfk + "')")
		}

		// Validate if given location is URL or local file
		_, err := url.ParseRequestURI(cfv.schemaLocationFromConfig)

		// With no error, it is an URL
		if err == nil {
			f.messaging.InfoMessage(cfk + ": Downloading schema file..")
			cfv.localFile = "temp/schema" + string(connutils.GenerateUUID()) + ".json"

			// Create local file
			schemaFile, _ := os.Create(cfv.localFile)
			defer schemaFile.Close()

			// Get the file from online
			resp, err := http.Get(cfv.schemaLocationFromConfig)
			if err != nil {
				f.messaging.ErrorMessage(cfk + ": Schema file '" + cfv.localFile + "' could not be downloaded")
				return err
			}
			defer resp.Body.Close()

			// Write file to local file
			b, _ := io.Copy(schemaFile, resp.Body)
			f.messaging.InfoMessage(fmt.Sprintf("%s: Download complete, file size: %d", cfk, b))
		} else {
			f.messaging.InfoMessage(cfk + ": Given schema location is not a valid URL, using local file")

			// Given schema location is not a valid URL, assume it is a local file
			cfv.localFile = cfv.schemaLocationFromConfig
		}

		// Read local file which is either just downloaded or given in config.
		f.messaging.InfoMessage(cfk + ": Read local file " + cfv.localFile)

		fileContents, err := ioutil.ReadFile(cfv.localFile)

		// Return error when error is given reading file.
		if err != nil {
			f.messaging.ErrorMessage(cfk + ": Schema file '" + cfv.localFile + "' could not be found")
			return err
		}

		// Merge JSON into Schema objects
		err = json.Unmarshal([]byte(fileContents), &cfv.Schema)
		f.messaging.InfoMessage(cfk + ": File is loaded")

		// Return error when error is given reading file.
		if err != nil {
			f.messaging.ErrorMessage(cfk + ": Can not parse schema")
			return err
		}

		// Validate schema
		err = f.validateSchema(cfv.Schema)

		// Return error when error is given validating the schema.
		if err != nil {
			f.messaging.ErrorMessage(cfk + ": Can not validate schema")
			return err
		}

		// Add info message about the schema validation.
		f.messaging.InfoMessage(cfk + ": Schema is validated and correct")
	}

	return nil
}

// validateSchema validates the given schema
func (f *WeaviateSchema) validateSchema(schema *models.SemanticSchema) error {
	// Initialize the dict to compare predicate data types

	// Loop through all classes
	for _, class := range schema.Classes {
		// Loop through all properties
		for _, prop := range class.Properties {
			// Init the inner loop variables
			// HasCref means that the value starts with a capital, so it is a cross-reference
			hasCRef := false

			// HasValue means that the value is not starting with a capital
			hasValue := false

			// Predicate init the predicate variable to contain the data type
			var pred DataType

			// Loop through all data types and set if a value or cross-reference is found
			for _, dataType := range prop.AtDataType {
				// Get the first letter to see if it is a capital
				firstLetter := string(dataType[0])
				if strings.ToUpper(firstLetter) == firstLetter {
					hasCRef = true
				} else {
					hasValue = true
				}
			}

			// See whether which of each combination of hasCRef and hasValue is found.
			if hasCRef && !hasValue {
				// It is a CRef, just make the pred a cross-reference in the dict
				pred = DataTypeCRef
			} else if hasValue && !hasCRef {
				// If it is a value, check whether it is a correct data type
				firstDataType := prop.AtDataType[0]

				// Check whether a class-property contains multiple data types
				if len(prop.AtDataType) > 1 {
					return errors_.New(fmt.Sprintf(
						"loaded schema has multiple data types in class '%s', at property '%s': '%s'. Just add one accepted data type to this property",
						class.Class,
						prop.Name,
						strings.Join(prop.AtDataType, ","),
					))
				}

				// Check if set the data type is correct
				if IsValidValueDataType(firstDataType) {
					// Cast the string to a data type
					pred = DataType(firstDataType)
				} else {
					// Check whether a class-property contains no data types
					return errors_.New(fmt.Sprintf(
						"unknown data type found in class '%s', at property '%s': '%s'",
						class.Class,
						prop.Name,
						strings.Join(prop.AtDataType, ","),
					))
				}
			} else if hasCRef && hasValue {
				// Check whether a class-property contains multiple data types (both cross-reference and value)
				return errors_.New(fmt.Sprintf(
					"loaded schema has an invalid combination of data type in class '%s', at property '%s': '%s'. Both references and values are mixed which is not correct",
					class.Class,
					prop.Name,
					strings.Join(prop.AtDataType, ","),
				))
			} else {
				// Check whether a class-property contains no data types
				return errors_.New(fmt.Sprintf(
					"no value given to the data type in class '%s', at property '%s'",
					class.Class,
					prop.Name,
				))
			}

			if val, ok := f.predicateDict[prop.Name]; ok {
				if val == DataTypeCRef && hasValue {
					// The value of the predicate in the dict is a Cref, but now its a value
					return errors_.New(fmt.Sprintf(
						"The value of the predicate '%s' is set as a cross-reference, but it is a value (%s) in class '%s', at property '%s'. %s",
						prop.Name,
						pred,
						class.Class,
						prop.Name,
						validationErrorMessage,
					))
				} else if pred != val {
					if pred == DataTypeCRef {
						// The value of the predicate in the dict is different
						return errors_.New(fmt.Sprintf(
							"The value of the predicate '%s' is set as '%s', but in class '%s', at property '%s' it is a cross-reference. %s",
							prop.Name,
							val,
							class.Class,
							prop.Name,
							validationErrorMessage,
						))
					}
					// The value of the predicate in the dict is different
					return errors_.New(fmt.Sprintf(
						"The value of the predicate '%s' is set as '%s', but in class '%s', at property '%s' it's value is a '%s'. %s",
						prop.Name,
						val,
						class.Class,
						prop.Name,
						pred,
						validationErrorMessage,
					))
				}
			} else if string(pred) != "" {
				// Add to predicate dict if it is not empty
				f.predicateDict[prop.Name] = pred
			}
		}
	}

	return nil
}

// UpdateObjectSchemaProperties updates all the edges of the Object in 'schema', used with a new Object or to update/patch a Object using a connector specified callback.
// This function is not part of connector utils because of the import cycle problem
func UpdateObjectSchemaProperties(refType string, object interface{}, nodeSchema models.Schema, schemas *WeaviateSchema, callback func(string, interface{}, *DataType) error) error {
	// Init error var
	var err error

	// Add Object properties
	for propKey, propValue := range nodeSchema.(map[string]interface{}) {
		var c *models.SemanticSchemaClass
		if refType == connutils.RefTypeAction {
			thing := object.(*models.Thing)
			c, err = GetClassByName(schemas.ActionSchema.Schema, thing.AtClass)
			if err != nil {
				return err
			}
		} else if refType == connutils.RefTypeThing {
			action := object.(*models.Thing)
			c, err = GetClassByName(schemas.ThingSchema.Schema, action.AtClass)
			if err != nil {
				return err
			}
		} else {
			return errors_.New("invalid refType given")
		}

		dataType, err := GetPropertyDataType(c, propKey)
		// If getting a property gives an error, return it
		if err != nil {
			return err
		}

		err = callback(propKey, propValue, dataType)

		// If adding a property gives an error, return it
		if err != nil {
			return err
		}
	}

	return err
}
