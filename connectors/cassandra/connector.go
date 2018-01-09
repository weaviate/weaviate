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

package cassandra

import (
	errors_ "errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

const objectTableName = "object_data"

// Cassandra has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type Cassandra struct {
	client *gocql.Session
	kind   string

	config        Config
	serverAddress string
	schema        *schema.WeaviateSchema
	messaging     *messages.Messaging
}

// Config represents the config outline for Cassandra. The Database config shoud be of the following form:
// "database_config" : {
//     "host": "127.0.0.1",
//     "port": 9080
// }
// Notice that the port is the GRPC-port.
type Config struct {
	Host string
	Port int
}

func (f *Cassandra) trace() {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(2, pc)
	f2 := runtime.FuncForPC(pc[0])
	//file, line := f2.FileLine(pc[0])
	fmt.Printf("THIS FUNCTION RUNS: %s\n", f2.Name())
}

// GetName returns a unique connector name, this name is used to define the connector in the weaviate config
func (f *Cassandra) GetName() string {
	return "cassandra"
}

// SetConfig sets variables, which can be placed in the config file section "database_config: {}"
// can be custom for any connector, in the example below there is only host and port available.
//
// Important to bear in mind;
// 1. You need to add these to the struct Config in this document.
// 2. They will become available via f.config.[variable-name]
//
// 	"database": {
// 		"name": "cassandra",
// 		"database_config" : {
// 			"host": "127.0.0.1",
// 			"port": 9080
// 		}
// 	},
func (f *Cassandra) SetConfig(configInput *config.Environment) error {

	// Mandatory: needed to add the JSON config represented as a map in f.config
	err := mapstructure.Decode(configInput.Database.DatabaseConfig, &f.config)

	// Example to: Validate if the essential  config is available, like host and port.
	if err != nil || len(f.config.Host) == 0 || f.config.Port == 0 {
		return errors_.New("could not get Cassandra host/port from config")
	}

	// If success return nil, otherwise return the error (see above)
	return nil
}

// SetSchema takes actionSchema and thingsSchema as an input and makes them available globally at f.schema
// In case you want to modify the schema, this is the place to do so.
// Note: When this function is called, the schemas (action + things) are already validated, so you don't have to build the validation.
func (f *Cassandra) SetSchema(schemaInput *schema.WeaviateSchema) error {
	f.schema = schemaInput

	// If success return nil, otherwise return the error
	return nil
}

// SetMessaging is used to send messages to the service.
// Available message types are: f.messaging.Infomessage ...DebugMessage ...ErrorMessage ...ExitError (also exits the service) ...InfoMessage
func (f *Cassandra) SetMessaging(m *messages.Messaging) error {

	// mandatory, adds the message functions to f.messaging to make them globally accessible.
	f.messaging = m

	// If success return nil, otherwise return the error
	return nil
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
// Does not return anything
func (f *Cassandra) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Connect creates a connection to the database and tables if not already available.
// The connections could not be closed because it is used more often.
func (f *Cassandra) Connect() error {
	/*
	 * NOTE: EXPLAIN WHAT HAPPENS HERE
	 */

	cluster := gocql.NewCluster("127.0.0.1") // TODO variable

	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	if err := session.Query(`CREATE KEYSPACE IF NOT EXISTS weaviate 
		WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }`).Exec(); err != nil {
		return err
	} // TODO variable

	session.Close()

	cluster.Keyspace = "weaviate" // TODO variable
	session, err = cluster.CreateSession()

	if err != nil {
		return err
	}

	f.client = session

	// If success return nil, otherwise return the error (also see above)
	return nil
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Cassandra) Init() error {
	// Add table 'object_data'
	err := f.client.Query(`
		CREATE TABLE IF NOT EXISTS weaviate.object_data (
			id UUID PRIMARY KEY,
			uuid UUID,
			type text,
			class text,
			property_key text,
			property_val_string text,
			property_val_bool boolean,
			property_val_timestamp timestamp,
			property_val_int int,
			property_val_float float,
			property_ref text,
			timestamp timestamp,
			deleted boolean
		);`).Exec()

	if err != nil {
		return err
	}

	// Create all indexes
	indexes := []string{"uuid", "type", "class", "property_key", "property_val_string", "property_val_bool", "property_val_timestamp", "property_val_int", "property_val_float", "property_ref", "timestamp", "deleted"}
	for _, prop := range indexes {
		if err := f.client.Query(fmt.Sprintf(`CREATE INDEX IF NOT EXISTS object_%s ON weaviate.object_data (%s);`, prop, prop)).Exec(); err != nil {
			return err
		}
	}

	// Add ROOT-key if not exists
	// Search for Root key
	var rootCount int

	if err := f.client.Query(`
		SELECT COUNT(id) AS rootCount FROM object_data WHERE property_key = ? AND property_val_bool = ? ALLOW FILTERING
	`, "root", true).Scan(&rootCount); err != nil {
		return err
	}

	if rootCount == 0 {
		f.messaging.InfoMessage("No root-key found.")

		// Create new object and fill it
		keyObject := models.Key{}
		token := connutils.CreateRootKeyObject(&keyObject)

		err = f.AddKey(&keyObject, connutils.GenerateUUID(), token)

		if err != nil {
			return err
		}
	}

	// If success return nil, otherwise return the error
	return nil
}

// AddThing adds a thing to the Cassandra database with the given UUID.
// Takes the thing and a UUID as input.
// Thing is already validated against the ontology
func (f *Cassandra) AddThing(thing *models.Thing, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {

	// thingResponse should be populated with the response that comes from the DB.
	// thingResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *Cassandra) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {

	// thingsResponse should be populated with the response that comes from the DB.
	// thingsResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *Cassandra) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {

	// Run the query to update the thing based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Cassandra) DeleteThing(UUID strfmt.UUID) error {

	// Run the query to delete the thing based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// AddAction adds an action to the Cassandra database with the given UUID.
// Takes the action and a UUID as input.
// Action is already validated against the ontology
func (f *Cassandra) AddAction(action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// GetAction fills the given ActionGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	// actionResponse should be populated with the response that comes from the DB.
	// actionResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// ListActions fills the given ActionListResponse with the values from the database, based on the given parameters.
func (f *Cassandra) ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	// actionsResponse should be populated with the response that comes from the DB.
	// actionsResponse = based on the ontology

	// If success return nil, otherwise return the error
	return nil
}

// UpdateAction updates the Thing in the DB at the given UUID.
func (f *Cassandra) UpdateAction(action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	return nil
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *Cassandra) DeleteAction(UUID strfmt.UUID) error {

	// Run the query to delete the action based on its UUID.

	// If success return nil, otherwise return the error
	return nil
}

// AddKey adds a key to the Cassandra database with the given UUID and token.
// UUID  = reference to the key
// token = is the actual access token used in the API's header
func (f *Cassandra) AddKey(key *models.Key, UUID strfmt.UUID, token strfmt.UUID) error {
	insertStmt := `
		INSERT INTO %v (id, uuid, type, class, property_key, %s, property_ref, timestamp, deleted) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	batch := f.client.NewBatch(gocql.LoggedBatch)

	keyUUID, _ := gocql.ParseUUID(string(UUID))

	batch.Query(fmt.Sprintf(insertStmt, objectTableName, "property_val_bool"), gocql.TimeUUID(), keyUUID, connutils.RefTypeKey, "", "delete", key.Delete, "", connutils.NowUnix(), false)
	batch.Query(fmt.Sprintf(insertStmt, objectTableName, "property_val_string"), gocql.TimeUUID(), keyUUID, connutils.RefTypeKey, "", "email", key.Email, "", connutils.NowUnix(), false)
	batch.Query(fmt.Sprintf(insertStmt, objectTableName, "property_val_bool"), gocql.TimeUUID(), keyUUID, connutils.RefTypeKey, "", "execute", key.Execute, "", connutils.NowUnix(), false)
	batch.Query(fmt.Sprintf(insertStmt, objectTableName, "property_val_string"), gocql.TimeUUID(), keyUUID, connutils.RefTypeKey, "", "ipOrigin", strings.Join(key.IPOrigin, "|"), "", connutils.NowUnix(), false)
	batch.Query(fmt.Sprintf(insertStmt, objectTableName, "property_val_timestamp"), gocql.TimeUUID(), keyUUID, connutils.RefTypeKey, "", "keyExpiresUnix", key.KeyExpiresUnix, "", connutils.NowUnix(), false)
	batch.Query(fmt.Sprintf(insertStmt, objectTableName, "property_val_bool"), gocql.TimeUUID(), keyUUID, connutils.RefTypeKey, "", "read", key.Read, "", connutils.NowUnix(), false)
	batch.Query(fmt.Sprintf(insertStmt, objectTableName, "property_val_bool"), gocql.TimeUUID(), keyUUID, connutils.RefTypeKey, "", "write", key.Write, "", connutils.NowUnix(), false)
	batch.Query(fmt.Sprintf(insertStmt, objectTableName, "property_val_bool"), gocql.TimeUUID(), keyUUID, connutils.RefTypeKey, "", "root", true, "", connutils.NowUnix(), false)

	if err := f.client.ExecuteBatch(batch); err != nil {
		return err
	}

	// If success return nil, otherwise return the error
	return nil
}

// ValidateToken validates/gets a key to the Cassandra database with the given token (=UUID)
func (f *Cassandra) ValidateToken(token strfmt.UUID, key *models.KeyTokenGetResponse) error {

	// key (= models.KeyTokenGetResponse) should be populated with the response that comes from the DB.
	// key = based on the ontology

	// in case the key is not found, return an error like:
	// return errors_.New("Key not found in database.")

	// If success return nil, otherwise return the error
	return nil
}

// GetKey fills the given KeyTokenGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetKey(UUID strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {

	f.trace()
	return nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Cassandra) DeleteKey(UUID strfmt.UUID) error {
	f.trace()
	return nil
}

// GetKeyChildren fills the given KeyTokenGetResponse array with the values from the database, based on the given UUID.
func (f *Cassandra) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyTokenGetResponse) error {

	// for examle: `children = [OBJECT-A, OBJECT-B, OBJECT-C]`
	// Where an OBJECT = models.KeyTokenGetResponse

	return nil
}
