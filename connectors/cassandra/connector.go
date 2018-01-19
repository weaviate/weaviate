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
	"strconv"
	"strings"
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

const objectTableName = "object_data"
const sep = "||"

// IDColumn constant column name
const IDColumn string = "id"

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
const PropertiesColumn string = "properties"

// // PropertyKeyMapKey constant column name
// const PropertyKeyMapKey string = "property_key"

// // PropertyValueMapKey constant column name
// const PropertyValueMapKey string = "property_value"

// DeletedColumn constant column name
const DeletedColumn string = "deleted"

// TimeStampColumn constant column name
const TimeStampColumn string = "timestamp"

// Global insert statement
const insertStatement = `
	INSERT INTO %v (
		` + IDColumn + `, 
		` + UUIDColumn + `, 
		` + TypeColumn + `, 
		` + ClassColumn + `, 
		` + CreationTimeColumn + `,
		` + LastUpdatedTimeColumn + `, 
		` + OwnerColumn + `, 
		` + PropertiesColumn + `,
		` + DeletedColumn + `,
		` + TimeStampColumn + `) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

const selectStatement = `
	SELECT *
	FROM ` + objectTableName + ` WHERE uuid = ?
`

const listSelectStatement = `
	SELECT uuid 
	FROM ` + objectTableName + ` 
	WHERE property_ref = ? AND property_key = 'key' AND type = '` + connutils.RefTypeThing + `' ALLOW FILTERING
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
	// Add table 'object_data'

	// TODO make const
	// TODO property_ref = UUID type?

	err := f.client.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS object_data (
			` + IDColumn + ` UUID PRIMARY KEY, 
			` + UUIDColumn + ` UUID, 
			` + TypeColumn + ` text, 
			` + ClassColumn + ` text,
			` + CreationTimeColumn + ` timestamp, 
			` + LastUpdatedTimeColumn + ` timestamp, 
			` + OwnerColumn + ` UUID, 
			` + PropertiesColumn + ` map<text, text>,
			` + DeletedColumn + ` boolean, 
			` + TimeStampColumn + ` timestamp
		);`)).Exec()

	if err != nil {
		return err
	}

	// Create all indexes
	indexes := []string{UUIDColumn, ClassColumn}
	for _, prop := range indexes {
		if err := f.client.Query(fmt.Sprintf(`CREATE INDEX IF NOT EXISTS object_%s ON object_data (%s);`, prop, prop)).Exec(); err != nil {
			return err
		}
	}

	// Add ROOT-key if not exists
	// Search for Root key
	var rootCount int

	if err := f.client.Query(fmt.Sprintf(`
		SELECT COUNT(id) AS rootCount FROM %s WHERE %s = ? AND %s['%s'] = ? ALLOW FILTERING
	`, objectTableName, TypeColumn, PropertiesColumn, `root`), connutils.RefTypeKey, strconv.FormatBool(true)).Scan(&rootCount); err != nil {
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
	// Parse UUID in Cassandra type
	cqlUUID := f.convUUIDtoCQLUUID(UUID)

	properties := map[string]string{}
	properties["context"] = thing.AtContext

	// Add Object properties using a callback
	callback := f.createPropertyCallback(&properties, cqlUUID, thing.AtClass)
	err := schema.UpdateObjectSchemaProperties(connutils.RefTypeThing, thing, thing.Schema, f.schema, callback)

	if err != nil {
		return err
	}

	query := f.client.Query(
		fmt.Sprintf(insertStatement, objectTableName),
		gocql.TimeUUID(),
		cqlUUID,
		connutils.RefTypeThing,
		thing.AtClass,
		thing.CreationTimeUnix,
		nil,
		f.convUUIDtoCQLUUID(thing.Key.NrDollarCref),
		properties,
		false,
		connutils.NowUnix(),
	)

	if err := query.Exec(); err != nil {
		return err
	}

	// If success return nil, otherwise return the error
	return nil
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	// Get the iterator
	iter := f.getSelectIteratorByUUID(UUID)

	err := f.fillResponseWithIter(iter, thingResponse, connutils.RefTypeThing)

	// If success return nil, otherwise return the error
	return err
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *Cassandra) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {

	iterUUIDs := f.client.Query(listSelectStatement, string(keyID)).Iter()

	var sUUID gocql.UUID
	for iterUUIDs.Scan(&sUUID) {
		fmt.Println(sUUID)
		fmt.Println("hoi")
	}

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
	keyUUID, _ := gocql.ParseUUID(string(UUID))

	isRoot := key.Parent == nil

	properties := map[string]string{
		"delete":           strconv.FormatBool(key.Delete),
		"email":            key.Email,
		"execute":          strconv.FormatBool(key.Execute),
		"ip_origin":        strings.Join(key.IPOrigin, sep), // TODO
		"key_expires_unix": strconv.FormatInt(key.KeyExpiresUnix, 10),
		"read":             strconv.FormatBool(key.Read),
		"write":            strconv.FormatBool(key.Write),
		"root":             strconv.FormatBool(isRoot),
		"token":            string(token),
	}

	if !isRoot {
		properties["parent.location_url"] = *key.Parent.LocationURL
		properties["parent.cref"] = string(key.Parent.NrDollarCref)
		properties["parent.type"] = key.Parent.Type
	}

	query := f.client.Query(
		fmt.Sprintf(insertStatement, objectTableName),
		gocql.TimeUUID(),
		keyUUID,
		connutils.RefTypeKey,
		nil,
		connutils.NowUnix(),
		nil,
		nil,
		properties,
		false,
		connutils.NowUnix(),
	)

	if err := query.Exec(); err != nil {
		return err
	}

	// If success return nil, otherwise return the error
	return nil
}

// ValidateToken validates/gets a key to the Cassandra database with the given token (=UUID)
func (f *Cassandra) ValidateToken(token strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {

	// key (= models.KeyTokenGetResponse) should be populated with the response that comes from the DB.
	// key = based on the ontology

	// in case the key is not found, return an error like:
	// return errors_.New("Key not found in database.")

	iter := f.client.Query(fmt.Sprintf(`
		SELECT * 
		FROM %s 
		WHERE %s = ? AND %s['%s'] = ? 
		LIMIT 1 ALLOW FILTERING
	`, objectTableName, TypeColumn, PropertiesColumn, `token`), connutils.RefTypeKey, string(token)).Consistency(gocql.One).Iter()

	err := f.fillResponseWithIter(iter, keyResponse, connutils.RefTypeKey)

	if err != nil {
		return errors_.New("Key not found in database.")
	}

	// If success return nil, otherwise return the error
	return nil
}

// GetKey fills the given KeyTokenGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetKey(UUID strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {
	// Get row for the key
	iter := f.getSelectIteratorByUUID(UUID)

	err := f.fillResponseWithIter(iter, keyResponse, connutils.RefTypeKey)

	return err
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Cassandra) DeleteKey(UUID strfmt.UUID) error {

	return nil
}

// GetKeyChildren fills the given KeyTokenGetResponse array with the values from the database, based on the given UUID.
func (f *Cassandra) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyTokenGetResponse) error {

	// for examle: `children = [OBJECT-A, OBJECT-B, OBJECT-C]`
	// Where an OBJECT = models.KeyTokenGetResponse

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

func (f *Cassandra) createPropertyCallback(properties *map[string]string, cqlUUID gocql.UUID, className string) func(string, interface{}, *schema.DataType, string) error {
	return func(propKey string, propValue interface{}, dataType *schema.DataType, edgeName string) error {
		var dataValue string

		if *dataType == schema.DataTypeBoolean {
			dataValue = strconv.FormatBool(propValue.(bool))
		} else if *dataType == schema.DataTypeDate {
			dataValue = propValue.(string)
		} else if *dataType == schema.DataTypeInt {
			dataValue = strconv.FormatInt(int64(propValue.(float64)), 10)
		} else if *dataType == schema.DataTypeNumber {
			dataValue = strconv.FormatFloat(propValue.(float64), 'f', -1, 64)
		} else if *dataType == schema.DataTypeString {
			dataValue = propValue.(string)
		} else if *dataType == schema.DataTypeCRef {
			// TODO
		}

		if dataValue != "" {
			(*properties)[schema.SchemaPrefix+propKey] = dataValue
		}

		return nil
	}
}

func (f *Cassandra) getSelectIteratorByUUID(UUID strfmt.UUID) *gocql.Iter {
	return f.client.Query(selectStatement, string(UUID)).Iter()
}

func (f *Cassandra) fillResponseWithIter(iter *gocql.Iter, response interface{}, refType string) error {
	m := map[string]interface{}{}

	found := false

	responseSchema := make(map[string]interface{})
	for iter.MapScan(m) {
		p := m[PropertiesColumn].(map[string]string)

		if connutils.RefTypeKey == refType {
			keyResponse := response.(*models.KeyTokenGetResponse)
			keyResponse.KeyID = f.convCQLUUIDtoUUID(m[UUIDColumn].(gocql.UUID))
			keyResponse.Delete = connutils.Must(strconv.ParseBool(p["delete"])).(bool)
			keyResponse.Email = p["email"]
			keyResponse.Execute = connutils.Must(strconv.ParseBool(p["execute"])).(bool)
			keyResponse.IPOrigin = strings.Split(p["ip_origin"], sep)
			keyResponse.KeyExpiresUnix = connutils.Must(strconv.ParseInt(p["key_expires_unix"], 20, 64)).(int64)
			keyResponse.Read = connutils.Must(strconv.ParseBool(p["read"])).(bool)
			keyResponse.Write = connutils.Must(strconv.ParseBool(p["write"])).(bool)
			keyResponse.Token = strfmt.UUID(p["token"])

			// TODO Add parent
			// if !isRoot {
			// 	properties["parent.location_url"] = *key.Parent.LocationURL
			// 	properties["parent.cref"] = string(key.Parent.NrDollarCref)
			// 	properties["parent.type"] = key.Parent.Type
			// }
		} else if connutils.RefTypeThing == refType {
			class := m[ClassColumn].(string)

			thingResponse := response.(*models.ThingGetResponse)
			thingResponse.ThingID = f.convCQLUUIDtoUUID(m[UUIDColumn].(gocql.UUID))
			thingResponse.AtClass = class
			thingResponse.AtContext = p["context"]
			thingResponse.CreationTimeUnix = m[CreationTimeColumn].(time.Time).Unix()

			url := ""
			ownerObj := models.SingleRef{
				LocationURL:  &url,
				NrDollarCref: f.convCQLUUIDtoUUID(m[OwnerColumn].(gocql.UUID)),
				Type:         connutils.RefTypeKey,
			}
			thingResponse.Key = &ownerObj
			thingResponse.LastUpdateTimeUnix = m[LastUpdatedTimeColumn].(time.Time).Unix()

			for key, value := range p {
				if isSchema, propKey, dataType, err := schema.TranslateSchemaPropertiesFromDataBase(key, class, f.schema.ThingSchema.Schema); isSchema {
					if err != nil {
						return err
					}

					if *dataType == schema.DataTypeBoolean {
						responseSchema[propKey] = connutils.Must(strconv.ParseBool(value)).(bool)
					} else if *dataType == schema.DataTypeDate {
						t, err := time.Parse(time.RFC3339, value)

						// Return if there is an error while parsing
						if err != nil {
							return err
						}
						responseSchema[propKey] = t
					} else if *dataType == schema.DataTypeInt {
						responseSchema[propKey] = connutils.Must(strconv.ParseInt(value, 20, 64)).(int64)
					} else if *dataType == schema.DataTypeNumber {
						responseSchema[propKey] = connutils.Must(strconv.ParseFloat(value, 64)).(float64)
					} else if *dataType == schema.DataTypeString {
						responseSchema[propKey] = value
					} else if *dataType == schema.DataTypeCRef {
						// TODO
					}
				}
			}

			thingResponse.Schema = responseSchema
		}

		found = true
	}

	if !found {
		return errors_.New("Nothing found")
	}

	if err := iter.Close(); err != nil {
		return err
	}

	return nil
}
