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
 - SetLogger
 - SetServerAddress
 - Connect
 - Init

All other function are called on the API request

After creating the connector, make sure to add the name of the connector to:
func GetAllConnectors() in configure_weaviate.go

*/

package foobar

import (
	"context"
	"encoding/json"
	errors_ "errors"

	"github.com/go-openapi/strfmt"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"

	dbconnector "github.com/creativesoftwarefdn/weaviate/adapters/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

// Foobar has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type Foobar struct {
	client *websocket.Conn
	kind   string

	appConfig     config.Config
	config        Config
	serverAddress string
	schema        schema.Schema
	logger        logrus.FieldLogger
}

// New FoobarConnector from connector-specific config and application wide config
func New(config interface{}, appConfig config.Config) (dbconnector.DatabaseConnector, error) {
	f := &Foobar{
		appConfig: appConfig,
	}

	err := f.setConfig(config)
	if err != nil {
		return nil, err
	}

	return f, nil
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

// setConfig sets variables, which can be placed in the config file section
// "database_config: {}" can be custom for any connector, in the example below
// there is only host and port available.
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

// SetSchema takes actionSchema and thingsSchema as an input and makes them
// available globally at f.schema
func (f *Foobar) SetSchema(schemaInput schema.Schema) {
	f.schema = schemaInput
}

// SetLogger to make the connector use a logrus.FieldLogger
func (f *Foobar) SetLogger(l logrus.FieldLogger) {
	f.logger = l
}

// SetServerAddress is used to fill in a global variable with the server
// address, but can also be used to do some custom actions.  Does not return
// anything
func (f *Foobar) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Connect creates a connection to the database and tables if not already
// available. The connections could not be closed because it is used more
// often.
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

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Foobar) Init(ctx context.Context) error {

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
func (f *Foobar) AddClass(ctx context.Context, kind kind.Kind, class *models.SemanticSchemaClass) error {
	return errors_.New("Not supported")
}

// Drop a class from the schema.
func (f *Foobar) DropClass(ctx context.Context, kind kind.Kind, className string) error {
	return errors_.New("Not supported")
}

func (f *Foobar) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return errors_.New("Not supported")
}

func (f *Foobar) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	return errors_.New("Not supported")
}

func (f *Foobar) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return errors_.New("Not supported")
}

func (j *Foobar) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	return errors_.New("Not supported")
}

func (f *Foobar) DropProperty(ctx context.Context, kind kind.Kind, className string, propName string) error {
	return errors_.New("Not supported")
}

// AddThing adds a thing to the Foobar database with the given UUID. Takes the
// thing and a UUID as input. Thing is already validated against the ontology
func (f *Foobar) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// AddThingsBatch can be used for imports of large datasets. There is no fixed
// batch size, each connector can decide for themselves to split up the
// incoming batch into smaller chunks. For example the Janusgraph connector
// makes this cut at 50 items as this has proven to be a good threshold for
// that particular connector.
//
// Note that every thing in the kinds.BatchThings list also has an error
// field. The connector must check whether there already is an error in one of
// the items. This error could for example indicate a failed validation. Items
// that have failed prior to making it to the connector are not removed on
// purpose, so that the return result to the user matches their original
// request in both length and order.
//
// The connector can decide - based on the batching consistency promises it
// makes to the user - whether to fail (and not import) the entire batch or only
// to skip the ones which failed validation
func (f *Foobar) AddThingsBatch(ctx context.Context, things kinds.BatchThings) error {
	// If success return nil, otherwise return the error
	return nil
}

// GetThing fills the given Thing with the values from the database,
// based on the given UUID.
func (f *Foobar) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.Thing) error {

	// thingResponse should be populated with the response that comes from the DB.
	// thingResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// ListThings fills the given ThingsListResponse with the values from the
// database, based on the given parameters.
func (f *Foobar) ListThings(ctx context.Context, limit int, thingsResponse *models.ThingsListResponse) error {

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

// AddActionsBatch can be used for imports of large datasets. There is no fixed
// batch size, each connector can decide for themselves to split up the
// incoming batch into smaller chunks. For example the Janusgraph connector
// makes this cut at 50 items as this has proven to be a good threshold for
// that particular connector.
//
// Note that every action in the kinds.BatchActions list also has an error
// field. The connector must check whether there already is an error in one of
// the items. This error could for example indicate a failed validation. Items
// that have failed prior to making it to the connector are not removed on
// purpose, so that the return result to the user matches their original
// request in both length and order.
//
// The connector can decide - based on the batching consistency promises it
// makes to the user - whether to fail (and not import) the entire batch or
// only to skip the ones which failed validation
func (f *Foobar) AddActionsBatch(ctx context.Context, actions kinds.BatchActions) error {
	// If success return nil, otherwise return the error
	return nil
}

// AddAction adds an action to the Foobar database with the given UUID. Takes
// the action and a UUID as input. Action is already validated against the
// ontology
func (f *Foobar) AddAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetAction fills the given Action with the values from the
// database, based on the given UUID.
func (f *Foobar) GetAction(ctx context.Context, UUID strfmt.UUID, actionResponse *models.Action) error {
	// actionResponse should be populated with the response that comes from the DB.
	// actionResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// ListActions fills the ActionReponse  with a list of all actions
func (f *Foobar) ListActions(ctx context.Context, limit int, actionsResponse *models.ActionsListResponse) error {
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

// AddBatchReferences can be used for imports of a large number of references
// in bulk. There is no fixed batch size, each connector can decide for
// themselves to split up the incoming batch into smaller chunks. For example
// the Janusgraph connector makes this cut at 50 items as this has proven to be
// a good threshold for that particular connector.
//
// Note that every thing in the kinds.BatchReferences list also has an error
// field. The connector must check whether there already is an error in one of
// the items. This error could for example indicate a failed validation. Items
// that have failed prior to making it to the connector are not removed on
// purpose, so that the return result to the user matches their original
// request in both length and order.
//
// The connector can decide - based on the batching consistency promises it
// makes to the user - whether to fail (and not import) the entire batch or only
// to skip the ones which failed validation
//
// WARNING: The validation that occurs prior to calling this is absolutely
// minimal. This is to avoid "read before write" queries as this batch
// importing is meant for maximum speed and thus avoids potentially slow
// patterns, such as "read before write". This means that we have no guarantuee
// that the source uuid exists and/or matches the specified class name and
// property.
func (f *Foobar) AddBatchReferences(ctx context.Context, refs kinds.BatchReferences) error {
	// If success return nil, otherwise return the error
	return nil
}

// SetState is called by a connector when it has updated it's internal state that needs to
// be shared across all connectors in other Weaviate instances.
func (f *Foobar) SetState(ctx context.Context, state json.RawMessage) {
}

// SetStateManager links a connector to this state manager. When the internal
// state of some connector is updated, this state connector will call SetState
// on the provided conn.
func (f *Foobar) SetStateManager(manager connector_state.StateManager) {
}

// LocalGetClass resolves a GraphQL request about a single Class like so
//
//	`{ Local { Get { Things { City { population } } } } }`
//
// Where "City" is the particular className of kind "Thing". In the example
// above the user asked to resolve one property named "population". This
// information is contained in the Params, together with pagination and filter
// information.  Based on this info, the foobar connector can resolve the
// request.  It should resolve to a []map[string]interface{} that can be consumed
// by the respective resolver in graphqlapi/local/get.
//
// An example matching the return value to the query above could look like:
//
//	[]interface{}{
//	 map[string]interface {}{
//	  "population": 1800000,
//	 },
//	 map[string]interface {}{
//	  "population": 600000,
//	 },
//	}
func (f *Foobar) LocalGetClass(info *kinds.LocalGetParams) (interface{}, error) {
	return nil, nil
}

// LocalGetMeta resolves a GraphQL request to retrieve meta info about a single
// Class like so:
//
//	`{ Local { GetMeta { Things { City { population { count } } } } } }`
//
// Where "City" is the particular className of kind "Thing". In the example
// above the user asked to resolve one property named "population". On this
// particular property the meta info "count" is requested, i.e. how many "City"
// classes have the property "count" set? This information is contained in the
// Params, together with pagination and filter information.  Based on this
// info, the foobar connector can resolve the request.  It should resolve to a
// map[string]interface{} that can be consumed by the respective resolver in
// graphqlapi/local/getmeta.
//
// An example for a return value matching the above query could look like:
//
//	map[string]interface{}{
//		"population": map[string]interface{}{
//			"count": 4,
//		},
//	}
func (f *Foobar) LocalGetMeta(info *kinds.GetMetaParams) (interface{}, error) {
	return nil, nil
}

// LocalAggregate resolves a GraphQL request to aggregate info about a single
// Class grouped by a property like so:
//
//	`{ Local { Aggregate { Things { City(groupBy: ["isCapital"]) { population { mean } } } } } }`
//
// Where "City" is the particular className of kind "Thing". In the example
// above the user asked to resolve one property named "population". On this
// particular property the aggregation info "mean" is requested. In addition
// the "City" class is grouped by the "isCapital" property. So overall the user
// wants to known what is the mean population of cities which are capitals and
// what is the mean population of cities which are not capitals? This
// information is contained in the Params, together with pagination and filter
// information. Based on this info, the foobar connector can resolve the
// request. It should resolve to a []map[string]interface{} that can be consumed
// by the respective resolver in graphqlapi/local/aggregate
//
// An example for a return value matching the above query could look like:
//
//	[]interface{}{
//		map[string]interface{}{
//			"population": map[string]interface{}{
//				"mean": 1.2e+06,
//			},
//			"groupedBy": map[string]interface{}{
//				"value": "false",
//				"path": []interface{}{
//					"isCapital",
//				},
//			},
//		},
//		map[string]interface{}{
//			"population": map[string]interface{}{
//				"mean": 2.635e+06,
//			},
//			"groupedBy": map[string]interface{}{
//				"value": "true",
//				"path": []interface{}{
//					"isCapital",
//				},
//			},
//		},
//	}
func (f *Foobar) LocalAggregate(info *kinds.AggregateParams) (interface{}, error) {
	return nil, nil
}

// LocalFetchKindClass allows for a contextionary-aided search and will find
// any classes that match the params outlined in the users request. By the time
// the connector is called, the contextionary-related work has already been
// completed and the connector is presented with a list of possible class
// names, as well as a list of possible property names. In additional a filter
// criterium (to be applied on those classes/properties) is present. For
// details on the input parameteres, see kinds.FetchParams
//
// The connector must respond with a list of short-form beacon and certainty
// tuples as maps. Note that the concept of how to calculate certainity has not
// been finalized yet:
// https://github.com/creativesoftwarefdn/weaviate/issues/710
//
// An example return value could look like this:
//	[]interface{}{
//		map[string]interface{}{
//			"certainty": 0.5,
//			"beacon": "weaviate://localhost/things/4f2aa50a-82c4-40f9-998c-55957cdc4b74",
//			},
//		},
//	}
func (f *Foobar) LocalFetchKindClass(info *kinds.FetchParams) (interface{}, error) {
	return nil, nil
}

// LocalFetchFuzzy allows for a contextionary-aided search and will find
// any classes that contain the outlined words. If the connector supports
// searching by levensthein-edit distance it should perform such as a search,
// otherwise it can go for exact match of the tokenized words in the string
// properties.
//
// The connector must respond with a list of short-form beacon and certainty
// tuples as maps. Note that the concept of how to calculate certainity has not
// been finalized yet:
// https://github.com/creativesoftwarefdn/weaviate/issues/710
//
// An example return value could look like this:
//	[]interface{}{
//		map[string]interface{}{
//			"certainty": 0.5,
//			"beacon": "weaviate://localhost/things/4f2aa50a-82c4-40f9-998c-55957cdc4b74",
//			},
//		},
//	}
func (f *Foobar) LocalFetchFuzzy(words []string) (interface{}, error) {
	return nil, nil
}
