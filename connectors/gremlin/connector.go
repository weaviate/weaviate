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
When starting Weaviate, functions are called in the following order;
(find the function in this document to understand what it is that they do)
 - GetName
 - SetConfig
 - SetSchema
 - SetMessaging
 - SetServerAddress
 - Connect
 - Init

All other function are called on the API request

After creating the connector, make sure to add the name of the connector to: func GetAllConnectors() in configure_weaviate.go

*/

package gremlin

import (
	errors_ "errors"
	"fmt"
	"runtime"

	"github.com/go-openapi/strfmt"
	"github.com/gorilla/websocket"
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

func (f *Gremlin) trace() {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(2, pc)
	f2 := runtime.FuncForPC(pc[0])
	//file, line := f2.FileLine(pc[0])
	fmt.Printf("THIS FUNCTION RUNS: %s\n", f2.Name())
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

	gremlinWsAddress := fmt.Sprintf("ws://%s:%d/gremlin", f.config.Host, f.config.Port)

	var dialer *websocket.Dialer

	clientConn, _, err := dialer.Dial(gremlinWsAddress, nil)
	if err != nil {
		return err
	}

	f.client = clientConn

	// If success return nil, otherwise return the error (also see above)
	return nil
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Gremlin) Init() error {

	/*
	 * 1.  If a schema is needed, you need to add the schema to the DB here.
	 * 1.1 Create the (thing or action) classes first, classes that a node (subject or object) can have (for example: Building, Person, etcetera)
	 * 2.  Create a root key.
	 */

	// Example of creating rootkey
	//
	// Add ROOT-key if not exists
	// Search for Root key

	// SEARCH FOR ROOTKEY

	//if totalResult.Root.Count == 0 {
	//	f.messaging.InfoMessage("No root-key found.")
	//
	//	// Create new object and fill it
	//	keyObject := models.Key{}
	//	token := connutils.CreateRootKeyObject(&keyObject)
	//
	//	err = f.AddKey(&keyObject, connutils.GenerateUUID(), token)
	//
	//	if err != nil {
	//		return err
	//	}
	//}
	// END KEYS

	// If success return nil, otherwise return the error
	return nil
}

// AddThing adds a thing to the Gremlin database with the given UUID.
// Takes the thing and a UUID as input.
// Thing is already validated against the ontology
func (f *Gremlin) AddThing(thing *models.Thing, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Gremlin) GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {

	// thingResponse should be populated with the response that comes from the DB.
	// thingResponse = based on the ontology

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
func (f *Gremlin) DeleteThing(UUID strfmt.UUID) error {

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
func (f *Gremlin) DeleteAction(UUID strfmt.UUID) error {

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
	return nil
}

// GetKey fills the given KeyTokenGetResponse with the values from the database, based on the given UUID.
func (f *Gremlin) GetKey(UUID strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {

	f.trace()
	return nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Gremlin) DeleteKey(UUID strfmt.UUID) error {
	f.trace()
	return nil
}

// GetKeyChildren fills the given KeyTokenGetResponse array with the values from the database, based on the given UUID.
func (f *Gremlin) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyTokenGetResponse) error {

	// for examle: `children = [OBJECT-A, OBJECT-B, OBJECT-C]`
	// Where an OBJECT = models.KeyTokenGetResponse

	return nil
}
