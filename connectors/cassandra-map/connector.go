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

package cassandramap

import (
	"math/rand"
	"strings"
	// "encoding/json"
	errors_ "errors"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

const objectTableName = "object_data_map"
const sep = "||"

// IDColumn constant column name
// const IDColumn string = "id"

// UUIDColumn constant column name
const UUIDColumn string = "uuid"

// TypeColumn constant column name
const TypeColumn string = "type"

// ClassColumn constant column name
const ClassColumn string = "class"

// CreationTimeColumn constant column name
const CreationTimeColumn string = "creation_time"

// LastUpdatedTimeColumn constant column name
const LastUpdatedTimeColumn string = "last_updated_time"

// OwnerColumn constant column name
const OwnerColumn string = "owner"

// PropertiesColumn const column name
// const PropertiesColumn string = "properties"

// // PropertyKeyMapKey constant column name
// const PropertyKeyMapKey string = "property_key"

// // PropertyValueMapKey constant column name
// const PropertyValueMapKey string = "property_value"

// RootColumn constant column name
const RootColumn string = "root"

// TokenColumn constant column name
const TokenColumn string = "keytoken"

// DeletedColumn constant column name
const DeletedColumn string = "deleted"

// Global insert statement
const insertStatement = `
	INSERT INTO %v (
		` + UUIDColumn + `, 
		` + TypeColumn + `, 
		` + ClassColumn + `, 
		` + CreationTimeColumn + `,
		` + LastUpdatedTimeColumn + `, 
		` + OwnerColumn + `,
		` + DeletedColumn + `
		%s) 
	VALUES (?, ?, ?, ?, ?, ?, ? %s)
`

const selectKeyByTokenStatement = `
	SELECT * 
	FROM %s 
	WHERE %s = ? AND '%s' = ? 
	LIMIT 1 
	ALLOW FILTERING
`

const selectRootKeyStatement = `
	SELECT COUNT(uuid) AS rootCount 
	FROM %s WHERE %s = ? 
	AND %s = ? 
	ALLOW FILTERING
`

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

// GetName returns a unique connector name, this name is used to define the connector in the weaviate config
func (f *Cassandra) GetName() string {
	return "cassandra-map"
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
	// Create a Cassandra cluster
	cluster := gocql.NewCluster("127.0.0.1") // TODO variable

	// Create a session on the cluster for just creating/checking the Keyspace
	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	if err := session.Query(`CREATE KEYSPACE IF NOT EXISTS weaviate 
		WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }`).Exec(); err != nil {
		return err
	} // TODO variable

	// Close session for checking Keyspace
	session.Close()

	// Settings for createing the new Session
	cluster.Keyspace = "weaviate" // TODO variable
	cluster.ConnectTimeout = time.Minute
	cluster.Timeout = time.Hour
	session, err = cluster.CreateSession()

	if err != nil {
		return err
	}

	// Put the session into the client-variable to make is usable everywhere else
	f.client = session

	// If success return nil, otherwise return the error (also see above)
	return nil
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Cassandra) Init() error {
	var columns string
	propertiesDone := map[string]bool{}
	for _, class := range f.schema.ThingSchema.Schema.Classes {
		for _, prop := range class.Properties {
			if propertiesDone[prop.Name] {
				continue
			}
			columns = fmt.Sprintf(`%s %s map<timestamp, text>,`, columns, prop.Name)
			propertiesDone[prop.Name] = true
		}
	}

	// Add table 'object_data'
	err := f.client.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS `+objectTableName+` (
			`+UUIDColumn+` UUID, 
			`+TypeColumn+` text, 
			`+ClassColumn+` text,
			`+CreationTimeColumn+` timestamp, 
			`+LastUpdatedTimeColumn+` timestamp, 
			`+OwnerColumn+` UUID, 
			%s
			`+DeletedColumn+` boolean,
			`+RootColumn+` boolean,
			`+TokenColumn+` text,
			PRIMARY KEY (`+UUIDColumn+`)
		)`, columns)).Exec()

	if err != nil {
		return err
	}

	// Create all indexes
	indexes := []string{ClassColumn}
	for _, prop := range indexes {
		if err := f.client.Query(fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS object_%s ON `+objectTableName+` (%s);
			`, prop, prop)).Exec(); err != nil {

			return err
		}
	}

	// Add ROOT-key if not exists
	// Search for Root key
	var rootCount int

	if err := f.client.Query(
		fmt.Sprintf(selectRootKeyStatement, objectTableName, TypeColumn, RootColumn),
		connutils.RefTypeKey,
		true,
	).Scan(&rootCount); err != nil {
		return err
	}

	// If root-key is not found
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
	defer f.messaging.TimeTrack(time.Now(), "AddThing")

	c, _ := schema.GetClassByName(f.schema.ThingSchema.Schema, thing.AtClass)

	propNameList := ""
	propQM := ""

	data := []map[int64]string{}
	j := 0
	for _, prop := range c.Properties {
		d := map[int64]string{}
		dataCount := 10000

		propNameList = fmt.Sprintf(`%s ,%s `, propNameList, prop.Name)
		propQM = fmt.Sprintf(`%s ,? `, propQM)

		i := 0
		for i < dataCount {
			val := rand.Intn(100000000)
			d[connutils.NowUnix()+int64(val)] = fmt.Sprintf("Value %d", val)
			i = i + 1
		}
		data = append(data, d)
		if j >= 1 {
			break
		}
		j = j + 1
	}

	query := f.client.Query(
		fmt.Sprintf(insertStatement,
			objectTableName,
			propNameList,
			propQM,
		),
		f.convUUIDtoCQLUUID(UUID),
		connutils.RefTypeThing,
		thing.AtClass,
		connutils.NowUnix(),
		thing.CreationTimeUnix,
		f.convUUIDtoCQLUUID(thing.Key.NrDollarCref),
		false,
		data[0],
		data[1],
	)

	return query.Exec()
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	return nil
}

// GetThings fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetThings(UUIDs []strfmt.UUID, thingsResponse *models.ThingsListResponse) error {
	return nil
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *Cassandra) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {
	return nil
}

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *Cassandra) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {
	return nil
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Cassandra) DeleteThing(thing *models.Thing, UUID strfmt.UUID) error {
	return nil
}

// AddAction adds an action to the Cassandra database with the given UUID.
// Takes the action and a UUID as input.
// Action is already validated against the ontology
func (f *Cassandra) AddAction(action *models.Action, UUID strfmt.UUID) error {
	return nil
}

// GetAction fills the given ActionGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	return nil
}

// ListActions fills the given ActionListResponse with the values from the database, based on the given parameters.
func (f *Cassandra) ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	return nil
}

// UpdateAction updates the Action in the DB at the given UUID.
func (f *Cassandra) UpdateAction(action *models.Action, UUID strfmt.UUID) error {
	return nil
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *Cassandra) DeleteAction(action *models.Action, UUID strfmt.UUID) error {
	return nil
}

// AddKey adds a key to the Cassandra database with the given UUID and token.
// UUID  = reference to the key
// token = is the actual access token used in the API's header
func (f *Cassandra) AddKey(key *models.Key, UUID strfmt.UUID, token strfmt.UUID) error {
	keyUUID, _ := gocql.ParseUUID(string(UUID))

	isRoot := key.Parent == nil

	fmt.Println(fmt.Sprintf(insertStatement,
		objectTableName,
		`, `+RootColumn+`, `+TokenColumn,
		`,?,?`,
	))

	query := f.client.Query(
		fmt.Sprintf(insertStatement,
			objectTableName,
			`,`+RootColumn+`, `+TokenColumn,
			`,?,?`,
		),
		keyUUID,
		connutils.RefTypeKey,
		nil,
		connutils.NowUnix(),
		nil,
		nil,
		false,
		isRoot,
		string(token),
	)

	// If success return nil, otherwise return the error
	return query.Exec()
}

// ValidateToken validates/gets a key to the Cassandra database with the given token (=UUID)
func (f *Cassandra) ValidateToken(token strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {
	keyResponse.KeyID = connutils.GenerateUUID()
	keyResponse.Delete = true
	keyResponse.Email = "john@test.nl"
	keyResponse.Execute = true
	keyResponse.IPOrigin = strings.Split("hoi||doei", sep)
	keyResponse.KeyExpiresUnix = -1
	keyResponse.Read = true
	keyResponse.Write = true
	keyResponse.Token = token

	// If success return nil, otherwise return the error
	return nil
}

// GetKey fills the given KeyTokenGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetKey(UUID strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {
	keyResponse.KeyID = UUID
	keyResponse.Delete = true
	keyResponse.Email = "john@test.nl"
	keyResponse.Execute = true
	keyResponse.IPOrigin = strings.Split("hoi||doei", sep)
	keyResponse.KeyExpiresUnix = -1
	keyResponse.Read = true
	keyResponse.Write = true

	return nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Cassandra) DeleteKey(key *models.Key, UUID strfmt.UUID) error {
	return nil
}

// GetKeyChildren fills the given KeyTokenGetResponse array with the values from the database, based on the given UUID.
func (f *Cassandra) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyTokenGetResponse) error {
	return nil
}

func (f *Cassandra) convUUIDtoCQLUUID(UUID strfmt.UUID) gocql.UUID {
	cqlUUID, _ := gocql.ParseUUID(string(UUID))
	return cqlUUID
}

func (f *Cassandra) convCQLUUIDtoUUID(cqlUUID gocql.UUID) strfmt.UUID {
	UUID := strfmt.UUID(cqlUUID.String())
	return UUID
}
