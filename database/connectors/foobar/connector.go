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
	"context"
	"encoding/json"
	errors_ "errors"
	"fmt"
	"runtime"

	"github.com/go-openapi/strfmt"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"

	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	connutils "github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	graphql_local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	graphql_local_getmeta "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
)

// Foobar has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type Foobar struct {
	client *websocket.Conn
	kind   string

	config        Config
	serverAddress string
	schema        schema.Schema
	messaging     *messages.Messaging
}

func New(config interface{}) (error, dbconnector.DatabaseConnector) {
	f := &Foobar{}
	err := f.setConfig(config)

	if err != nil {
		return err, nil
	} else {
		return nil, f
	}
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

// setConfig sets variables, which can be placed in the config file section "database_config: {}"
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
func (f *Foobar) setConfig(config interface{}) error {

	// Mandatory: needed to add the JSON config represented as a map in f.config
	err := mapstructure.Decode(config, &f.config)

	// Example to: Validate if the essential  config is available, like host and port.
	if err != nil || len(f.config.Host) == 0 || f.config.Port == 0 {
		return errors_.New("could not get Foobar host/port from config")
	}

	// If success return nil, otherwise return the error (see above)
	return nil
}

// SetSchema takes actionSchema and thingsSchema as an input and makes them available globally at f.schema
func (f *Foobar) SetSchema(schemaInput schema.Schema) {
	f.schema = schemaInput
}

// SetMessaging is used to send messages to the service
// Available message types are: f.messaging.Infomessage ...DebugMessage ...ErrorMessage ...ExitError (also exits the service) ...InfoMessage
func (f *Foobar) SetMessaging(m *messages.Messaging) {

	// mandatory, adds the message functions to f.messaging to make them globally accessible.
	f.messaging = m
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
	 * NOTE: EXAMPLE FOR WEBSOCKETS
	 */

	// foobarWsAddress := fmt.Sprintf("ws://%s:%d/foobar", f.config.Host, f.config.Port)

	// var dialer *websocket.Dialer

	// clientConn, _, err := dialer.Dial(foobarWsAddress, nil)
	// if err != nil {
	// 	return err
	// }

	// f.client = clientConn

	// If success return nil, otherwise return the error (also see above)
	return nil
}

// Init 1st initializes the schema in the database.
func (f *Foobar) Init() error {

	/*
	 * 1.  If a schema is needed, you need to add the schema to the DB here.
	 * 1.1 Create the (thing or action) classes first, classes that a node (subject or object) can have (for example: Building, Person, etcetera)
	 */

	// If success return nil, otherwise return the error
	return nil
}

// Attach can attach something to the request-context
func (f *Foobar) Attach(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

// Add a class to the Thing or Action schema, depending on the kind parameter.
func (f *Foobar) AddClass(kind kind.Kind, class *models.SemanticSchemaClass) error {
	return errors_.New("Not supported")
}

// Drop a class from the schema.
func (f *Foobar) DropClass(kind kind.Kind, className string) error {
	return errors_.New("Not supported")
}

func (f *Foobar) UpdateClass(kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return errors_.New("Not supported")
}

func (f *Foobar) AddProperty(kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	return errors_.New("Not supported")
}

func (f *Foobar) UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return errors_.New("Not supported")
}

func (j *Foobar) UpdatePropertyAddDataType(kind kind.Kind, className string, propName string, newDataType string) error {
	return errors_.New("Not supported")
}

func (f *Foobar) DropProperty(kind kind.Kind, className string, propName string) error {
	return errors_.New("Not supported")
}

// AddThing adds a thing to the Foobar database with the given UUID.
// Takes the thing and a UUID as input.
// Thing is already validated against the ontology
func (f *Foobar) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Foobar) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {

	// thingResponse should be populated with the response that comes from the DB.
	// thingResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// GetThings fills the given ThingsListResponse with the values from the database, based on the given UUIDs.
func (f *Foobar) GetThings(ctx context.Context, UUIDs []strfmt.UUID, thingResponse *models.ThingsListResponse) error {
	f.messaging.DebugMessage(fmt.Sprintf("GetThings: %s", UUIDs))

	// If success return nil, otherwise return the error
	return nil
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *Foobar) ListThings(ctx context.Context, first int, offset int, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {

	// thingsResponse should be populated with the response that comes from the DB.
	// thingsResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *Foobar) UpdateThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {

	// Run the query to update the thing based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Foobar) DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {

	// Run the query to delete the thing based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// HistoryThing fills the history of a thing based on its UUID
func (f *Foobar) HistoryThing(ctx context.Context, UUID strfmt.UUID, history *models.ThingHistory) error {
	return nil
}

// MoveToHistoryThing moves a thing to history
func (f *Foobar) MoveToHistoryThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID, deleted bool) error {
	return nil
}

// AddAction adds an action to the Foobar database with the given UUID.
// Takes the action and a UUID as input.
// Action is already validated against the ontology
func (f *Foobar) AddAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetAction fills the given ActionGetResponse with the values from the database, based on the given UUID.
func (f *Foobar) GetAction(ctx context.Context, UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	// actionResponse should be populated with the response that comes from the DB.
	// actionResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// GetActions fills the given ActionsListResponse with the values from the database, based on the given UUIDs.
func (f *Foobar) GetActions(ctx context.Context, UUIDs []strfmt.UUID, actionsResponse *models.ActionsListResponse) error {
	// If success return nil, otherwise return the error
	return nil
}

// ListActions fills the ActionReponse  with a list of all actions
func (f *Foobar) ListActions(ctx context.Context, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	// If success return nil, otherwise return the error
	return nil
}

// UpdateAction updates the Thing in the DB at the given UUID.
func (f *Foobar) UpdateAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *Foobar) DeleteAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {

	// Run the query to delete the action based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// HistoryAction fills the history of a Action based on its UUID
func (f *Foobar) HistoryAction(ctx context.Context, UUID strfmt.UUID, history *models.ActionHistory) error {
	return nil
}

// MoveToHistoryAction moves an action to history
func (f *Foobar) MoveToHistoryAction(ctx context.Context, action *models.Action, UUID strfmt.UUID, deleted bool) error {
	return nil
}

// Called by a connector when it has updated it's internal state that needs to be shared across all connectors in other Weaviate instances.
func (f *Foobar) SetState(state json.RawMessage) {
}

// Link a connector to this state manager.
// When the internal state of some connector is updated, this state connector will call SetState on the provided conn.
func (f *Foobar) SetStateManager(manager connector_state.StateManager) {
}

func (f *Foobar) LocalGetClass(info *graphql_local_get.LocalGetClassParams) (interface{}, error) {
	return nil, nil
}

func (f *Foobar) LocalGetMeta(info *graphql_local_getmeta.Params) (interface{}, error) {
	return nil, nil
}
