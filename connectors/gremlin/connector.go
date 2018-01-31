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
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

/*

- Gremlin Verticals are always a "thing", "action" or "key".
- The object itself is constructed in the same way the JSON object is.
- Private items are set prefixed with _
- All have the property _type and _id (which is a uuid)

*/

package gremlin

import (
	"encoding/json"
	errors_ "errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-gremlin/gremlin"
	"github.com/go-openapi/strfmt"
	"github.com/gorilla/websocket"
	"github.com/imdario/mergo"
	"github.com/mitchellh/mapstructure"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

// Gremlin has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type Gremlin struct {
	client *websocket.Conn
	kind   string

	config        Config
	serverAddress string
	schema        *schema.WeaviateSchema
	messaging     *messages.Messaging
}

// Config represents the config outline for Gremlin. The Database config shoud be of the following form:
// "database_config" : {
//     "host": "127.0.0.1",
//     "port": 9080
// }
// Notice that the port is the GRPC-port.
type Config struct {
	Host string
	Port int
}

// Gremlin Query result struct
type GremlinResult struct {
	AtType  string `json:"@type"`
	AtValue []struct {
		AtType  string `json:"@type"`
		AtValue struct {
			ID struct {
				AtType  string `json:"@type"`
				AtValue int    `json:"@value"`
			} `json:"id"`
			Label      string `json:"label"`
			Properties map[string][]struct {
				AtType  string `json:"@type"`
				AtValue struct {
					ID struct {
						AtType  string `json:"@type"`
						AtValue int    `json:"@value"`
					} `json:"id"`
					Label string      `json:"label"`
					Value interface{} `json:"value"`
				} `json:"@value"`
			} `json:"properties"`
		} `json:"@value"`
	} `json:"@value"`
}

// Finds the value in a Gremlin result
func (f *Gremlin) getGremlinResultValue(value string, result *GremlinResult) interface{} {

	// check if the map exists
	if _, ok := result.AtValue[0].AtValue.Properties[value]; ok {
		// send the right value back as a string
		return result.AtValue[0].AtValue.Properties[value][0].AtValue.Value
	}

	// not found, return empty string
	return ""
}

func (f *Gremlin) parseMap(aMap map[string]interface{}, prefix string, propertyString string) string {

	for key, val := range aMap {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			propertyString += f.parseMap(val.(map[string]interface{}), key+".", "")
		case []interface{}:
			f.parseArray(val.([]interface{}), key+".", propertyString)
		default:

			// set the correct type in the Gremlin query, note that ONLY the string has quotes!
			switch reflect.TypeOf(concreteVal).Name() {
			case "string":
				propertyString += `.property("` + prefix + key + `", "` + concreteVal.(string) + `")`
			case "float64":
				propertyString += `.property("` + prefix + key + `", ` + strconv.FormatFloat(concreteVal.(float64), 'E', -1, 64) + `)`
			case "float32":
				propertyString += `.property("` + prefix + key + `", ` + strconv.FormatFloat(concreteVal.(float64), 'E', -1, 32) + `)`
			case "bool":
				propertyString += `.property("` + prefix + key + `", ` + strconv.FormatBool(concreteVal.(bool)) + `)`
			case "int":
				propertyString += `.property("` + prefix + key + `", ` + strconv.FormatInt(concreteVal.(int64), 16) + `)`
			case "uint":
				propertyString += `.property("` + prefix + key + `", ` + strconv.FormatUint(concreteVal.(uint64), 16) + `)`
			}

		}
	}

	// escape the at sign, note that $ will become "Dollar__"
	return strings.Replace(propertyString, "$", "Dollar__", -1)

}

func (f *Gremlin) parseArray(anArray []interface{}, prefix string, propertyString string) {
	for _, val := range anArray {
		switch val.(type) {
		case map[string]interface{}:
			propertyString += f.parseMap(val.(map[string]interface{}), prefix, propertyString)
		case []interface{}:
			f.parseArray(val.([]interface{}), prefix, propertyString)
		}
	}
}

// Creates a string of properties like: .property("key", "value")
func (f *Gremlin) createGremlinProperties(input interface{}) string {

	inputAsJson, _ := json.Marshal(input)

	jsonMap := map[string]interface{}{}

	// Parsing/Unmarshalling JSON encoding/json
	err := json.Unmarshal([]byte(inputAsJson), &jsonMap)

	if err != nil {
		panic(err)
	}

	return f.parseMap(jsonMap, "", "")

}

// Creates a struct based on the input struct and result JSON
func (f *Gremlin) gremlinResultToStruct(inputJSON []byte, handleStruct interface{}) interface{} {

	inputJSONResultStruct := GremlinResult{}
	err := json.Unmarshal(inputJSON, &inputJSONResultStruct)
	if err != nil {
		// SEND ERROR UNMARSHALLING
	}

	t := reflect.TypeOf(handleStruct).Elem()
	v := reflect.ValueOf(handleStruct).Elem()

	for i := 0; i < t.NumField(); i++ {

		switch v.Field(i).Kind().String() {
		case "struct":
			// go through same function and merge interfaces
			if err := mergo.Merge(&handleStruct, f.gremlinResultToStruct(inputJSON, v.Field(i).Addr().Interface())); err != nil {
				// ERROR
			}
		case "string":
			v.Field(i).SetString(f.getGremlinResultValue(t.Field(i).Name, &inputJSONResultStruct).(string))
		case "int":
			// Set the integer value to int64
			v.Field(i).SetInt(f.getGremlinResultValue(t.Field(i).Name, &inputJSONResultStruct).(int64))
		case "int64":
			// Set the integer64 value to int64
			fmt.Println("NEED TO FIX - IM an INT: ", t.Field(i).Name)
			v.Field(i).SetInt(-1)
		case "bool":
			// Set a boolean value
			v.Field(i).SetBool(f.getGremlinResultValue(t.Field(i).Name, &inputJSONResultStruct).(bool))
		case "slice":
			// Set the slice
			fmt.Println("NEED TO FIX - IM a SLICE: ", t.Field(i).Name)
		default:
			// sets the string to the right value
			fmt.Println("NEED TO FIX - IM a UNKNOWN THING: ", v.Field(i).Kind().String())

		}
	}
	return handleStruct
}

// GetName returns a unique connector name, this name is used to define the connector in the weaviate config
func (f *Gremlin) GetName() string {
	return "gremlin"
}

// SetConfig sets variables, which can be placed in the config file section "database_config: {}"
// can be custom for any connector, in the example below there is only host and port available.
//
// Important to bear in mind;
// 1. You need to add these to the struct Config in this document.
// 2. They will become available via f.config.[variable-name]
//
// 	"database": {
// 		"name": "gremlin",
// 		"database_config" : {
// 			"host": "127.0.0.1",
// 			"port": 9080
// 		}
// 	},
func (f *Gremlin) SetConfig(configInput *config.Environment) error {

	// Mandatory: needed to add the JSON config represented as a map in f.config
	err := mapstructure.Decode(configInput.Database.DatabaseConfig, &f.config)

	// Example to: Validate if the essential  config is available, like host and port.
	if err != nil || len(f.config.Host) == 0 || f.config.Port == 0 {
		return errors_.New("could not get Gremlin host/port from config")
	}

	// If success return nil, otherwise return the error (see above)
	return nil
}

// SetSchema takes actionSchema and thingsSchema as an input and makes them available globally at f.schema
// In case you want to modify the schema, this is the place to do so.
// Note: When this function is called, the schemas (action + things) are already validated, so you don't have to build the validation.
func (f *Gremlin) SetSchema(schemaInput *schema.WeaviateSchema) error {
	f.schema = schemaInput

	// If success return nil, otherwise return the error
	return nil
}

// SetMessaging is used to send messages to the service.
// Available message types are: f.messaging.Infomessage ...DebugMessage ...ErrorMessage ...ExitError (also exits the service) ...InfoMessage
func (f *Gremlin) SetMessaging(m *messages.Messaging) error {

	// mandatory, adds the message functions to f.messaging to make them globally accessible.
	f.messaging = m

	// If success return nil, otherwise return the error
	return nil
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
// Does not return anything
func (f *Gremlin) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Connect creates a connection to the database and tables if not already available.
// The connections could not be closed because it is used more often.
func (f *Gremlin) Connect() error {

	/*
	 * NOTE: EXPLAIN WHAT HAPPENS HERE
	 */
	if err := gremlin.NewCluster(fmt.Sprintf("ws://%s:%d/gremlin", f.config.Host, f.config.Port)); err != nil {
		return err
	}

	// If success return nil, otherwise return the error (also see above)
	return nil
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Gremlin) Init() error {

	// Init message
	f.messaging.InfoMessage("Initializing Gremlin connector...")

	// REMOVE THIS WHEN DONE, IS JUST FOR DEV
	//_, err := gremlin.Query(`g.V().drop().iterate()`).Exec()
	//if err != nil {
	//	f.messaging.DebugMessage("Error while restoring.")
	//}

	// Check if a rootkey is present
	keyAvailable, err := gremlin.Query(`g.V().has("_type", "key").has("_isRoot", true)`).Exec()
	if err != nil {
		f.messaging.DebugMessage("Error while fetching the root key.")
	}

	// there is no root key, meaning that it is a clean database and all should be setup
	if keyAvailable == nil {
		f.messaging.InfoMessage("No root key found, setting up a clean database installation...")

		// Create new object and fill it
		keyObject := models.Key{}
		token := connutils.CreateRootKeyObject(&keyObject)

		// Create the Gremlin query to add a root key
		_, err := gremlin.Query(`g.addV("key").property("_id", "` + string(connutils.GenerateUUID()) + `").property("_type", "key").property("_isRoot", "true").property("Write", ` + strconv.FormatBool(keyObject.Write) + `).property("Read", ` + strconv.FormatBool(keyObject.Read) + `).property("Delete", ` + strconv.FormatBool(keyObject.Delete) + `).property("Execute", ` + strconv.FormatBool(keyObject.Execute) + `).property("KeyExpiresUnix", ` + strconv.FormatInt(keyObject.KeyExpiresUnix, 16) + `).property("KeyID", "` + string(token) + `").property("Token", "` + string(token) + `").next()`).Exec()

		if err != nil {
			f.messaging.ExitError(1, "Can not create the root key, maybe the DB is corrupt.")
		}

	}

	// Init message
	f.messaging.InfoMessage("Gremlin connected.")

	// Init message
	f.messaging.InfoMessage("Weaviate is running.")

	return nil
}

// AddThing adds a thing to the Gremlin database with the given UUID.
// Takes the thing and a UUID as input.
// Thing is already validated against the ontology
func (f *Gremlin) AddThing(thing *models.Thing, UUID strfmt.UUID) error {

	gremlinQueryProps := f.createGremlinProperties(thing)

	// Create the Gremlin query to add a root key
	_, err := gremlin.Query(`g.addV("thing").property("_id", "` + string(UUID) + `")` + gremlinQueryProps + `.next()`).Exec()

	if err != nil {
		f.messaging.ErrorMessage("Could not create thing with id: " + string(UUID))
	}

	// If success return nil, otherwise return the error
	return nil
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Gremlin) GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {

	// thingResponse should be populated with the response that comes from the DB.
	// thingResponse = based on the ontology

	// If success return nil, otherwise return the error

	// Check if the key is present
	queryResult, err := gremlin.Query(`g.V().has("_id", "` + string(UUID) + `")`).Exec()
	if err != nil {
		f.messaging.DebugMessage("Error while fetching a key.")
		return errors_.New("Key not found.")
	}

	// set Dollar__ to $
	queryResultAsString := strings.Replace(string(queryResult), "Dollar__", "$", -1)

	// Create struct of results
	queryResultAsStruct := GremlinResult{}

	// marshall to interface
	err = json.Unmarshal([]byte(queryResultAsString), &queryResultAsStruct)
	if err != nil {
		return errors_.New("Error when fetching results from Gremlin DB.")
	}

	// add values to key by merging them
	if err := mergo.MergeWithOverwrite(thingResponse, f.gremlinResultToStruct(queryResult, &models.ThingGetResponse{}).(*models.ThingGetResponse)); err != nil {
		return errors_.New("Could not merge the Gremlin results with the appropriate struct.")
	}

	fmt.Println("AND OUT!")

	return nil
}

// GetThings fills the given []ThingGetResponse with the values from the database, based on the given UUIDs.
func (f *Gremlin) GetThings(UUIDs []strfmt.UUID, thingResponse *models.ThingsListResponse) error {
	f.messaging.DebugMessage(fmt.Sprintf("GetThings: %s", UUIDs))

	// If success return nil, otherwise return the error
	return nil
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *Gremlin) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {

	// thingsResponse should be populated with the response that comes from the DB.
	// thingsResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *Gremlin) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {

	// Run the query to update the thing based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Gremlin) DeleteThing(thing *models.Thing, UUID strfmt.UUID) error {

	// Run the query to delete the thing based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// AddAction adds an action to the Gremlin database with the given UUID.
// Takes the action and a UUID as input.
// Action is already validated against the ontology
func (f *Gremlin) AddAction(action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetAction fills the given ActionGetResponse with the values from the database, based on the given UUID.
func (f *Gremlin) GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	// actionResponse should be populated with the response that comes from the DB.
	// actionResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// ListActions fills the given ActionListResponse with the values from the database, based on the given parameters.
func (f *Gremlin) ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	// actionsResponse should be populated with the response that comes from the DB.
	// actionsResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// UpdateAction updates the Thing in the DB at the given UUID.
func (f *Gremlin) UpdateAction(action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *Gremlin) DeleteAction(action *models.Action, UUID strfmt.UUID) error {

	// Run the query to delete the action based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// AddKey adds a key to the Gremlin database with the given UUID and token.
// UUID  = reference to the key
// token = is the actual access token used in the API's header
func (f *Gremlin) AddKey(key *models.Key, UUID strfmt.UUID, token strfmt.UUID) error {

	// Key struct should be stored

	// If success return nil, otherwise return the error
	return nil
}

// ValidateToken validates/gets a key to the Gremlin database with the given token (=UUID)
func (f *Gremlin) ValidateToken(token strfmt.UUID, key *models.KeyTokenGetResponse) error {

	// key (= models.KeyTokenGetResponse) should be populated with the response that comes from the DB.
	// key = based on the ontology

	// in case the key is not found, return an error like:
	// return errors_.New("Key not found in database.")

	// If success return nil, otherwise return the error

	// Check if the key is present
	queryResult, err := gremlin.Query(`g.V().has("KeyID", "` + string(token) + `")`).Exec()
	if err != nil {
		f.messaging.DebugMessage("Error while fetching a key.")
		return errors_.New("Key not found.")
	}

	// key is not found in results at all
	if queryResult == nil {
		f.messaging.DebugMessage("An unknown key is used.")
		return errors_.New("Key not valid.")
	}

	// add values to key by merging them
	if err := mergo.MergeWithOverwrite(key, f.gremlinResultToStruct(queryResult, &models.KeyTokenGetResponse{}).(*models.KeyTokenGetResponse)); err != nil {
		return errors_.New("Could not merge the Gremlin results with the appropriate struct.")
	}

	return nil
}

// GetKey fills the given KeyTokenGetResponse with the values from the database, based on the given UUID.
func (f *Gremlin) GetKey(UUID strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {

	connutils.Trace()

	return nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Gremlin) DeleteKey(key *models.Key, UUID strfmt.UUID) error {

	connutils.Trace()

	return nil
}

// GetKeyChildren fills the given KeyTokenGetResponse array with the values from the database, based on the given UUID.
func (f *Gremlin) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyTokenGetResponse) error {

	// for examle: `children = [OBJECT-A, OBJECT-B, OBJECT-C]`
	// Where an OBJECT = models.KeyTokenGetResponse

	return nil
}
