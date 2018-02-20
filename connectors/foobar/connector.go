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

/*
 * THIS IS A DEMO CONNECTOR!
 * USE IT TO LEARN HOW TO CREATE YOUR OWN CONNECTOR.
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

package foobar

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

// Foobar has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type Foobar struct {
	client *websocket.Conn
	kind   string

	config        Config
	serverAddress string
	schema        *schema.WeaviateSchema
	messaging     *messages.Messaging
}

// Config represents the config outline for Foobar. The Database config shoud be of the following form:
// "database_config" : {
//     "host": "127.0.0.1",
//     "port": 9080
// }
// Notice that the port is the GRPC-port.
type Config struct {
	Host string
	Port int
}

func (f *Foobar) trace() {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(2, pc)
	f2 := runtime.FuncForPC(pc[0])
	//file, line := f2.FileLine(pc[0])
	fmt.Printf("THIS FUNCTION RUNS: %s\n", f2.Name())
}

// GetName returns a unique connector name, this name is used to define the connector in the weaviate config
func (f *Foobar) GetName() string {
	return "foobar"
}

// SetConfig sets variables, which can be placed in the config file section "database_config: {}"
// can be custom for any connector, in the example below there is only host and port available.
//
// Important to bear in mind;
// 1. You need to add these to the struct Config in this document.
// 2. They will become available via f.config.[variable-name]
//
// 	"database": {
// 		"name": "foobar",
// 		"database_config" : {
// 			"host": "127.0.0.1",
// 			"port": 9080
// 		}
// 	},
func (f *Foobar) SetConfig(configInput *config.Environment) error {

	// Mandatory: needed to add the JSON config represented as a map in f.config
	err := mapstructure.Decode(configInput.Database.DatabaseConfig, &f.config)

	// Example to: Validate if the essential  config is available, like host and port.
	if err != nil || len(f.config.Host) == 0 || f.config.Port == 0 {
		return errors_.New("could not get Foobar host/port from config")
	}

	// If success return nil, otherwise return the error (see above)
	return nil
}

// SetSchema takes actionSchema and thingsSchema as an input and makes them available globally at f.schema
// In case you want to modify the schema, this is the place to do so.
// Note: When this function is called, the schemas (action + things) are already validated, so you don't have to build the validation.
func (f *Foobar) SetSchema(schemaInput *schema.WeaviateSchema) error {
	f.schema = schemaInput

	// If success return nil, otherwise return the error
	return nil
}

// SetMessaging is used to send messages to the service.
// Available message types are: f.messaging.Infomessage ...DebugMessage ...ErrorMessage ...ExitError (also exits the service) ...InfoMessage
func (f *Foobar) SetMessaging(m *messages.Messaging) error {

	// mandatory, adds the message functions to f.messaging to make them globally accessible.
	f.messaging = m

	// If success return nil, otherwise return the error
	return nil
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
// Does not return anything
func (f *Foobar) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Connect creates a connection to the database and tables if not already available.
// The connections could not be closed because it is used more often.
func (f *Foobar) Connect() error {

	/*
	 * NOTE: EXPLAIN WHAT HAPPENS HERE
	 */

	foobarWsAddress := fmt.Sprintf("ws://%s:%d/foobar", f.config.Host, f.config.Port)

	var dialer *websocket.Dialer

	clientConn, _, err := dialer.Dial(foobarWsAddress, nil)
	if err != nil {
		return err
	}

	f.client = clientConn

	// If success return nil, otherwise return the error (also see above)
	return nil
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Foobar) Init() error {

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

// AddThing adds a thing to the Foobar database with the given UUID.
// Takes the thing and a UUID as input.
// Thing is already validated against the ontology
func (f *Foobar) AddThing(thing *models.Thing, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Foobar) GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {

	// thingResponse should be populated with the response that comes from the DB.
	// thingResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// GetThings fills the given []ThingGetResponse with the values from the database, based on the given UUIDs.
func (f *Foobar) GetThings(UUIDs []strfmt.UUID, thingResponse *models.ThingsListResponse) error {
	f.messaging.DebugMessage(fmt.Sprintf("GetThings: %s", UUIDs))

	// If success return nil, otherwise return the error
	return nil
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *Foobar) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {

	// thingsResponse should be populated with the response that comes from the DB.
	// thingsResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *Foobar) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {

	// Run the query to update the thing based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Foobar) DeleteThing(thing *models.Thing, UUID strfmt.UUID) error {

	// Run the query to delete the thing based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// AddAction adds an action to the Foobar database with the given UUID.
// Takes the action and a UUID as input.
// Action is already validated against the ontology
func (f *Foobar) AddAction(action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetAction fills the given ActionGetResponse with the values from the database, based on the given UUID.
func (f *Foobar) GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	// actionResponse should be populated with the response that comes from the DB.
	// actionResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// ListActions fills the given ActionListResponse with the values from the database, based on the given parameters.
func (f *Foobar) ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	// actionsResponse should be populated with the response that comes from the DB.
	// actionsResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// UpdateAction updates the Thing in the DB at the given UUID.
func (f *Foobar) UpdateAction(action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *Foobar) DeleteAction(action *models.Action, UUID strfmt.UUID) error {

	// Run the query to delete the action based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// AddKey adds a key to the Foobar database with the given UUID and token.
// UUID  = reference to the key
// token = is the actual access token used in the API's header
func (f *Foobar) AddKey(key *models.Key, UUID strfmt.UUID, token string) error {

	// Key struct should be stored

	// If success return nil, otherwise return the error
	return nil
}

// ValidateToken validates/gets a key to the Foobar database with the given token (=UUID)
func (f *Foobar) ValidateToken(token string, key *models.KeyGetResponse) error {

	// key (= models.KeyGetResponse) should be populated with the response that comes from the DB.

	// in case the key is not found, return an error like:
	// return errors_.New("Key not found in database.")

	// If success return nil, otherwise return the error
	return nil
}

// GetKey fills the given KeyGetResponse with the values from the database, based on the given UUID.
func (f *Foobar) GetKey(UUID strfmt.UUID, keyResponse *models.KeyGetResponse) error {

	f.trace()
	return nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Foobar) DeleteKey(key *models.Key, UUID strfmt.UUID) error {
	f.trace()
	return nil
}

// GetKeyChildren fills the given KeyGetResponse array with the values from the database, based on the given UUID.
func (f *Foobar) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyGetResponse) error {

	// for examle: `children = [OBJECT-A, OBJECT-B, OBJECT-C]`
	// Where an OBJECT = models.KeyGetResponse

	return nil
}
