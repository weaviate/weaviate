/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package cassandra

import (
	"context"
	errors_ "errors"
	"fmt"
	"strconv"
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

// Set constants for keys in the database, table names and columns
const tableKeys string = "keys"

// const tableKeysToken string = "keys_by_token"
const tableKeysParent string = "keys_by_parent"
const colKeyToken string = "key_token"
const colKeyUUID string = "key_uuid"
const colKeyParent string = "parent"
const colKeyRoot string = "root"
const colKeyAllowR string = "allow_read"
const colKeyAllowW string = "allow_write"
const colKeyAllowD string = "allow_delete"
const colKeyAllowX string = "allow_execute"
const colKeyEmail string = "email"
const colKeyIPOrigin string = "ip_origin"
const colKeyExpiryTime string = "key_expiry_time"

// Set constants for things and actions in the database, the tablenames have a similar form
const tableThings string = "things"
const tableActions string = "actions"
const tableListSuffix string = "_list"
const tableHistorySuffix string = "_property_history"
const tableClassSearchSuffix string = "_class_search"
const tableValueSearchSuffix string = "_key_value_search"
const tableThingsList string = tableThings + tableListSuffix
const tableThingsHistory string = tableThings + tableHistorySuffix
const tableThingsClassSearch string = tableThings + tableClassSearchSuffix
const tableThingsValueSearch string = tableThings + tableValueSearchSuffix
const tableActionsList string = tableActions + tableListSuffix
const tableActionsHistory string = tableActions + tableHistorySuffix
const tableActionsClassSearch string = tableActions + tableClassSearchSuffix
const tableActionsValueSearch string = tableActions + tableValueSearchSuffix

// Set constants for things and actions in the database, the column have a similar form
const colNodeTypePrefixThing string = "thing"
const colNodeTypePrefixAction string = "action"
const colNodeUUIDSuffix string = "_uuid"
const colThingUUID string = colNodeTypePrefixThing + colNodeUUIDSuffix
const colActionUUID string = colNodeTypePrefixAction + colNodeUUIDSuffix
const colActionSubjectUUID string = "action_subject_uuid"
const colActionSubjectLocation string = "action_subject_location"
const colActionObjectUUID string = "action_object_uuid"
const colActionObjectLocation string = "action_object_location"
const colNodeOwner string = "owner_uuid"
const colNodeDeleted string = "deleted"
const colNodeCreationTime string = "creation_time"
const colNodeLastUpdateTime string = "last_update_time"
const colNodeClass string = "class"
const colNodeContext string = "context"
const colNodeProperties string = "properties"
const colNodePropKey string = "property_key"
const colNodePropValue string = "property_value"

// All the database statements for selecting, inserting etc. data in the tables

// Key statements
// selectKeyStatement is used to select a single key from the database
const selectKeyStatement = `
	SELECT *
	FROM ` + tableKeys + `
	WHERE ` + colKeyUUID + ` = ?
`

// insertKeyStatement is used to insert a single key into the database
const insertKeyStatement = `
	INSERT INTO ` + tableKeys + ` (
		` + colKeyToken + `, 
		` + colKeyUUID + `, 
		` + colKeyParent + `, 
		` + colKeyRoot + `,
		` + colKeyAllowR + `, 
		` + colKeyAllowW + `, 
		` + colKeyAllowD + `,
		` + colKeyAllowX + `,
		` + colKeyEmail + `,
		` + colKeyIPOrigin + `,
		` + colKeyExpiryTime + ` ) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

// updateKeyStatement is used to update a single key into the database
const updateKeyStatement = `
	UPDATE ` + tableKeys + ` 
	SET
		` + colKeyToken + ` = ?, 
		` + colKeyParent + ` = ?, 		
		` + colKeyAllowR + ` = ?, 
		` + colKeyAllowW + ` = ?, 
		` + colKeyAllowD + ` = ?,
		` + colKeyAllowX + ` = ?,
		` + colKeyEmail + ` = ?,
		` + colKeyIPOrigin + ` = ?,
		` + colKeyExpiryTime + ` = ? 
	WHERE ` + colKeyUUID + ` = ? 
	AND ` + colKeyRoot + ` = ?
`

// selectRootKeyStatement is used to see whether a root key is present in the database
const selectRootKeyStatement = `
	SELECT COUNT(` + colKeyUUID + `) AS rootCount 
	FROM ` + tableKeys + ` 
	WHERE ` + colKeyRoot + ` = true
`

// selectKeyChildrenStatement is used get all children of a given parent from the database
const selectKeyChildrenStatement = `
	SELECT *
	FROM ` + tableKeysParent + `
	WHERE ` + colKeyParent + ` = ?
`

// deleteKeyStatement is used to delete a single key from the database
const deleteKeyStatement = `
	DELETE FROM ` + tableKeys + ` 
	WHERE ` + colKeyUUID + ` = ? 
	AND ` + colKeyRoot + ` = false
	IF EXISTS;
`

// Thing statements
// selectThingStatement is used to get a single thing form the database
const selectThingStatement = `
	SELECT *
	FROM ` + tableThings + ` 
	WHERE ` + colThingUUID + ` = ?
`

// selectThingInStatement is used to get a things form the database
const selectThingInStatement = `
	SELECT *
	FROM ` + tableThings + ` 
	WHERE ` + colThingUUID + ` IN ?
`

// listThingsSelectStatement is used to get a list of things form the database, based on owner and ordered by creationtime
const listThingsSelectStatement = `
	SELECT *
	FROM %s  
	WHERE ` + colNodeOwner + ` = ?
	%s
	ORDER BY ` + colNodeCreationTime + ` DESC
	LIMIT ?
`

// listThingsCountStatement is used to count the total amount of things for a certain owner
const listThingsCountStatement = `
	SELECT COUNT(` + colThingUUID + `) AS thingsCount 
	FROM %s  
	WHERE ` + colNodeOwner + ` = ?
	%s
`

// insertThingStatement is used to insert a single thing into the database
const insertThingStatement = `
	INSERT INTO ` + tableThings + ` (
		` + colThingUUID + `,
		` + colNodeOwner + `,
		` + colNodeCreationTime + `,
		` + colNodeLastUpdateTime + `, 
		` + colNodeClass + `, 
		` + colNodeContext + `,
		` + colNodeProperties + `) 
	VALUES (?, ?, ?, ?, ?, ?, ?)
`

// insertThingHistoryStatement is used to insert a single thing's properties into the history-database
const insertThingHistoryStatement = `
	INSERT INTO ` + tableThingsHistory + ` (
		` + colThingUUID + `, 
		` + colNodeOwner + `, 
		` + colNodeCreationTime + `, 
		` + colNodeDeleted + `, 
		` + colNodeClass + `,
		` + colNodeContext + `,
		` + colNodeProperties + ` ) 
	VALUES (?, ?, ?, ?, ?, ?, ?)
`

// deleteThingStatement is used to delete a single thing from the database
const deleteThingStatement = `
	DELETE FROM ` + tableThings + `
	WHERE ` + colThingUUID + ` = ? 
	AND ` + colNodeOwner + ` = ? 
	AND ` + colNodeCreationTime + ` = ? 
`

// selectThingHistoryStatement is used to get the a thing history from the database
const selectThingHistoryStatement = `
	SELECT *
	FROM ` + tableThingsHistory + ` 
	WHERE ` + colThingUUID + ` = ?
`

// Action statements
// selectActionStatement is used to get a single action form the database
const selectActionStatement = `
	SELECT *
	FROM ` + tableActions + ` 
	WHERE ` + colActionUUID + ` = ?
`

// listActionsSelectStatement is used to get a list of actions form the database, based on object-thing-id and ordered by creationtime
const listActionsSelectStatement = `
	SELECT *
	FROM %s 
	WHERE ` + colActionObjectUUID + ` = ?
	%s
	ORDER BY ` + colNodeCreationTime + ` DESC 
	LIMIT ?
`

// listActionsCountStatement is used to count the total amount of actions for a certain object-thing-id
const listActionsCountStatement = `
	SELECT COUNT(` + colActionUUID + `) AS actionsCount 
	FROM %s 
	WHERE ` + colActionObjectUUID + ` = ?
	%s 
`

// insertActionStatement is used to insert a single action into the database
const insertActionStatement = `
	INSERT INTO ` + tableActions + ` (
		` + colActionUUID + `,
		` + colNodeOwner + `,
		` + colNodeCreationTime + `,
		` + colNodeLastUpdateTime + `, 
		` + colNodeClass + `, 
		` + colNodeContext + `,
		` + colNodeProperties + `,
		` + colActionSubjectUUID + `,
		` + colActionSubjectLocation + `,
		` + colActionObjectUUID + `,
		` + colActionObjectLocation + `) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

// insertActionHistoryStatement is used to insert a single action's properties into the history-database
const insertActionHistoryStatement = `
	INSERT INTO ` + tableActionsHistory + ` (
		` + colActionUUID + `, 
		` + colNodeOwner + `, 
		` + colNodeCreationTime + `, 
		` + colNodeDeleted + `,
		` + colNodeClass + `,
		` + colNodeContext + `,
		` + colNodeProperties + `,
		` + colActionSubjectUUID + `,
		` + colActionSubjectLocation + `,
		` + colActionObjectUUID + `,
		` + colActionObjectLocation + ` ) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

// deleteActionStatement is used to delete a single action from the database
const deleteActionStatement = `
	DELETE FROM ` + tableActions + `
	WHERE ` + colActionUUID + ` = ? 
	AND ` + colNodeOwner + ` = ? 
	AND ` + colNodeCreationTime + ` = ? 
	AND ` + colActionObjectUUID + ` = ?;
`

// selectActionHistoryStatement is used to get the an action history from the database
const selectActionHistoryStatement = `
	SELECT *
	FROM ` + tableActionsHistory + ` 
	WHERE ` + colActionUUID + ` = ?
`

// Cassandra has some basic variables.
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
	Host     string
	Port     int
	Keyspace string
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
func (f *Cassandra) SetMessaging(m *messages.Messaging) error {

	// mandatory, adds the message functions to f.messaging to make them globally accessible.
	f.messaging = m

	// If success return nil, otherwise return the error
	return nil
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
func (f *Cassandra) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Connect creates a connection to the database and tables if not already available.
// The connections could not be closed because it is used more often.
func (f *Cassandra) Connect() error {
	// Create a Cassandra cluster
	cluster := gocql.NewCluster(f.config.Host)

	// Create a session on the cluster for just creating/checking the Keyspace
	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	// Create the keyspace with certain replication
	if err := session.Query(`CREATE KEYSPACE IF NOT EXISTS ` + f.config.Keyspace + ` 
		WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }`).Exec(); err != nil {
		return err
	}

	// Close session for checking Keyspace
	session.Close()

	// Settings for createing the new Session
	// TODO: Determine what to add to config and what not (https://github.com/creativesoftwarefdn/weaviate/issues/308)
	cluster.Keyspace = f.config.Keyspace
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
func (f *Cassandra) Init(ctx context.Context) (context.Context, error) {
	// Add table for 'keys', based on querying it by UUID
	// TODO: ADD SOMETHING LIKE:
	// has_read_access_to set<uuid>,
	// has_write_access_to set<uuid>,
	// has_delete_access_to set<uuid>,
	// has_execute_access_to set<uuid>,
	err := f.client.Query(`
		CREATE TABLE IF NOT EXISTS ` + tableKeys + ` (
			` + colKeyToken + ` text,
			` + colKeyUUID + ` uuid,
			` + colKeyParent + ` uuid,
			` + colKeyRoot + ` boolean,
			` + colKeyAllowR + ` boolean,
			` + colKeyAllowW + ` boolean,
			` + colKeyAllowD + ` boolean,
			` + colKeyAllowX + ` boolean,
			` + colKeyEmail + ` text,
			` + colKeyIPOrigin + ` list<text>,
			` + colKeyExpiryTime + ` timestamp,
			PRIMARY KEY ((` + colKeyUUID + `), ` + colKeyRoot + `)
		);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Add index for the clustering part 'root' to enable fast filtering
	err = f.client.Query(`CREATE INDEX IF NOT EXISTS i_root ON ` + tableKeys + ` (` + colKeyRoot + `);`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create a materialized view for querying for children of a certain node
	err = f.client.Query(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS ` + tableKeysParent + `
		AS SELECT *
		FROM ` + tableKeys + ` 
		WHERE ` + colKeyParent + ` IS NOT NULL AND ` + colKeyUUID + ` IS NOT NULL AND ` + colKeyRoot + ` IS NOT NULL
		PRIMARY KEY ((` + colKeyParent + `), ` + colKeyUUID + `, ` + colKeyRoot + `);`).Exec()

	if err != nil {
		return ctx, err
	}

	// Add table for 'things', based on selects it by UUID, bear in mind:
	// 1. Sorting on owner first to enable list searches
	// 2. Order by creation time for querying the most recent things
	err = f.client.Query(`
		CREATE TABLE IF NOT EXISTS ` + tableThings + ` (
			` + colNodeOwner + ` uuid,
			` + colThingUUID + ` uuid,
			` + colNodeCreationTime + ` timestamp,
			` + colNodeLastUpdateTime + ` timestamp,
			` + colNodeClass + ` text,
			` + colNodeContext + ` text,
			` + colNodeProperties + ` map<text, text>,
			PRIMARY KEY ((` + colThingUUID + `), ` + colNodeOwner + `, ` + colNodeCreationTime + `)
		) WITH CLUSTERING ORDER BY (` + colNodeOwner + ` ASC);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Add index on owner_uuid column to enable filtering on owner_uuid
	err = f.client.Query(`CREATE INDEX IF NOT EXISTS i_owner_uuid ON ` + tableThings + ` (` + colNodeOwner + `);`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create a table for the history of properties with uuid as primary key and ordering/clustering on creation time
	err = f.client.Query(`
		CREATE TABLE IF NOT EXISTS ` + tableThingsHistory + ` (
			` + colThingUUID + ` uuid,
			` + colNodeOwner + ` uuid,
			` + colNodeCreationTime + ` timestamp,
			` + colNodeDeleted + ` boolean,
			` + colNodeClass + ` text,
			` + colNodeContext + ` text,
			` + colNodeProperties + ` map<text, text>,
			PRIMARY KEY ((` + colThingUUID + `), ` + colNodeCreationTime + `)
		) WITH CLUSTERING ORDER BY (` + colNodeCreationTime + ` DESC);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create a view for list queries based on onwer-UUID and ordered by creation time
	err = f.client.Query(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS ` + tableThingsList + `
		AS SELECT *
		FROM ` + tableThings + ` 
		WHERE ` + colNodeOwner + ` IS NOT NULL AND ` + colNodeCreationTime + ` IS NOT NULL AND ` + colThingUUID + ` IS NOT NULL 
		PRIMARY KEY ((` + colNodeOwner + `), ` + colNodeCreationTime + `, ` + colThingUUID + `);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create view for search on thing-class
	err = f.client.Query(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS ` + tableThingsClassSearch + `
		AS SELECT *
		FROM ` + tableThings + ` 
		WHERE ` + colNodeClass + ` IS NOT NULL AND ` + colNodeOwner + ` IS NOT NULL AND ` + colThingUUID + ` IS NOT NULL AND ` + colNodeCreationTime + ` IS NOT NULL
		PRIMARY KEY ((` + colNodeClass + `, ` + colNodeOwner + `), ` + colNodeCreationTime + `, ` + colThingUUID + `);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create view to search on property key and value
	err = f.client.Query(`
		CREATE TABLE IF NOT EXISTS ` + tableThingsValueSearch + ` (
			` + colNodePropKey + ` text,
			` + colNodePropValue + ` text,
			` + colThingUUID + ` uuid,
			` + colNodeOwner + ` uuid,
			PRIMARY KEY ((` + colNodePropKey + `), ` + colNodePropValue + `)
		);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Add index to filter on property value
	err = f.client.Query(`CREATE INDEX IF NOT EXISTS i_property_value ON ` + tableThingsValueSearch + ` (` + colNodePropValue + `);`).Exec()

	if err != nil {
		return ctx, err
	}

	// Add table for 'actions', based on selects it by UUID, bear in mind:
	// 1. Sorting on owner first to enable list searches
	// 2. Order by creation time for querying the most recent things
	err = f.client.Query(`
		CREATE TABLE IF NOT EXISTS ` + tableActions + ` (
			` + colNodeOwner + ` uuid,
			` + colActionUUID + ` uuid,
			` + colNodeCreationTime + ` timestamp,
			` + colNodeLastUpdateTime + ` timestamp,
			` + colNodeClass + ` text,
			` + colNodeContext + ` text,
			` + colNodeProperties + ` map<text, text>,
			` + colActionSubjectUUID + ` uuid,
			` + colActionSubjectLocation + ` text,
			` + colActionObjectUUID + ` uuid,
			` + colActionObjectLocation + ` text,
			PRIMARY KEY ((` + colActionUUID + `), ` + colActionObjectUUID + `, ` + colNodeOwner + `, ` + colNodeCreationTime + `)
		) WITH CLUSTERING ORDER BY (` + colActionObjectUUID + ` ASC);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Add index on owner_uuid column to enable filtering on owner_uuid
	err = f.client.Query(`CREATE INDEX IF NOT EXISTS i_owner_uuid_actions ON ` + tableActions + ` (` + colNodeOwner + `);`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create a table for the history of properties with uuid as primary key and ordering/clustering on creation time
	err = f.client.Query(`
		CREATE TABLE IF NOT EXISTS ` + tableActionsHistory + ` (
			` + colActionUUID + ` uuid,
			` + colNodeOwner + ` uuid,
			` + colNodeCreationTime + ` timestamp,
			` + colNodeDeleted + ` boolean,
			` + colNodeClass + ` text,
			` + colNodeContext + ` text,
			` + colNodeProperties + ` map<text, text>,
			` + colActionSubjectUUID + ` uuid,
			` + colActionSubjectLocation + ` text,
			` + colActionObjectUUID + ` uuid,
			` + colActionObjectLocation + ` text,
			PRIMARY KEY ((` + colActionUUID + `), ` + colNodeCreationTime + `)
		) WITH CLUSTERING ORDER BY (` + colNodeCreationTime + ` DESC);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create a view for list queries based on object-thing-UUID and ordered by creation time
	err = f.client.Query(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS ` + tableActionsList + `
		AS SELECT *
		FROM ` + tableActions + ` 
		WHERE ` + colActionObjectUUID + ` IS NOT NULL AND ` + colNodeCreationTime + ` IS NOT NULL AND ` + colNodeOwner + ` IS NOT NULL AND ` + colActionUUID + ` IS NOT NULL 
		PRIMARY KEY ((` + colActionObjectUUID + `), ` + colNodeCreationTime + `, ` + colNodeOwner + `, ` + colActionUUID + `);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create view for search on action-class
	err = f.client.Query(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS ` + tableActionsClassSearch + `
		AS SELECT *
		FROM ` + tableActions + ` 
		WHERE ` + colNodeClass + ` IS NOT NULL AND ` + colActionObjectUUID + ` IS NOT NULL AND ` + colNodeOwner + ` IS NOT NULL AND ` + colNodeCreationTime + ` IS NOT NULL AND ` + colActionUUID + ` IS NOT NULL 
		PRIMARY KEY ((` + colNodeClass + `, ` + colActionObjectUUID + `), ` + colNodeCreationTime + `, ` + colNodeOwner + `, ` + colActionUUID + `);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Create view to search on property key and value
	err = f.client.Query(`
		CREATE TABLE IF NOT EXISTS ` + tableActionsValueSearch + ` (
			` + colNodePropKey + ` text,
			` + colNodePropValue + ` text,
			` + colActionUUID + ` uuid,
			` + colNodeOwner + ` uuid,
			PRIMARY KEY ((` + colNodePropKey + `), ` + colNodePropValue + `)
		);
	`).Exec()

	if err != nil {
		return ctx, err
	}

	// Add index to filter on property value
	err = f.client.Query(`CREATE INDEX IF NOT EXISTS i_property_value_actions ON ` + tableActionsValueSearch + ` (` + colNodePropValue + `);`).Exec()

	if err != nil {
		return ctx, err
	}

	// Add ROOT-key if not exists
	var rootCount int

	// Search for Root key
	if err := f.client.Query(selectRootKeyStatement).Scan(&rootCount); err != nil {
		return ctx, err
	}

	// If root-key is not found
	if rootCount == 0 {
		f.messaging.InfoMessage(connutils.StaticNoRootKey)

		// Create new object and fill it
		keyObject := models.Key{}
		token, UUID := connutils.CreateRootKeyObject(&keyObject)

		// Add the root-key to the database
		err = f.AddKey(&keyObject, UUID, token)

		if err != nil {
			return ctx, err
		}
	}

	// If success return nil, otherwise return the error
	return ctx, nil
}

// AddThing adds a thing to the Cassandra database with the given UUID.
// Takes the thing and a UUID as input.
// Thing is already validated against the ontology
func (f *Cassandra) AddThing(thing *models.Thing, UUID strfmt.UUID) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("AddThing: %s", UUID))

	// Run the query to add the thing based on its UUID.
	err := f.addThingRow(thing, UUID)

	// Also log the error message
	if err != nil {
		f.messaging.ErrorMessage(err)
	}

	// If success return nil, otherwise return the error
	return err
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("GetThing: %s", UUID))

	// Do the query to get the thing from the database and get the iterator
	iter := f.client.Query(selectThingStatement, f.convUUIDtoCQLUUID(UUID)).Iter()

	// Initialize the 'found' variable
	found := false

	// Put everything in a for loop, although there is only one result in this case
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Fill the response with the row
		f.fillThingResponseWithRow(m, thingResponse)

		// Update the 'found' variable
		found = true
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return err
	}

	// If there is no thing found, return an error
	if !found {
		return errors_.New(connutils.StaticThingNotFound)
	}

	// No errors, return nil
	return nil
}

// GetThings fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetThings(ctx context.Context, UUIDs []strfmt.UUID, thingsResponse *models.ThingsListResponse) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("GetThings '%s'", UUIDs))

	cqlUUIDs := []gocql.UUID{}

	for _, UUID := range UUIDs {
		cqlUUIDs = append(cqlUUIDs, f.convUUIDtoCQLUUID(UUID))
	}

	// Build the query
	query := fmt.Sprintf(selectThingInStatement)

	// Do the query to get the thing from the database and get the iterator
	iter := f.client.Query(query, cqlUUIDs).Iter()

	// Put everything in a for loop
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Fill the response with the row
		thingResponse := models.ThingGetResponse{}
		f.fillThingResponseWithRow(m, &thingResponse)

		// Add the thing to the list in the response
		thingsResponse.Things = append(thingsResponse.Things, &thingResponse)
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return err
	}

	// No errors, return nil
	return nil
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *Cassandra) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now())

	// Get the filter on class
	whereFilter, err := f.parseWhereFilters(wheres, false)

	if err != nil {
		return err
	}

	// Build the query
	query := ""
	countQuery := ""
	if whereFilter != "" {
		query = fmt.Sprintf(listThingsSelectStatement, tableThingsClassSearch, whereFilter)
		countQuery = fmt.Sprintf(listThingsCountStatement, tableThingsClassSearch, whereFilter)
	} else {
		query = fmt.Sprintf(listThingsSelectStatement, tableThingsList, "")
		countQuery = fmt.Sprintf(listThingsCountStatement, tableThingsList, "")
	}

	// Do the query to get the thing from the database and get the iterator
	iter := f.client.Query(query, f.convUUIDtoCQLUUID(keyID), first+offset).Iter()

	// Counter for offset
	offsetCount := 0

	// Put everything in a for loop
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Update offset count
		offsetCount = offsetCount + 1

		// Continue when the offset isnt reached yet
		if offset >= offsetCount {
			continue
		}

		// Fill the response with the row
		thingResponse := models.ThingGetResponse{}
		f.fillThingResponseWithRow(m, &thingResponse)

		// Add the thing to the list in the response
		thingsResponse.Things = append(thingsResponse.Things, &thingResponse)

	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return err
	}

	// Query for the total count of things
	var thingsCount int64
	if err := f.client.Query(countQuery, f.convUUIDtoCQLUUID(keyID)).Scan(&thingsCount); err != nil {
		return err
	}

	// Fill the total results with this specs
	thingsResponse.TotalResults = thingsCount

	// No errors, return nil
	return nil
}

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *Cassandra) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("UpdateThing: %s", UUID))

	// Run the query to update the thing based on its UUID.
	err := f.addThingRow(thing, UUID)

	// If there is an error, add an error message
	if err != nil {
		f.messaging.ErrorMessage(err)
	}

	// If success return nil, otherwise return the error
	return err
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Cassandra) DeleteThing(thing *models.Thing, UUID strfmt.UUID) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("DeleteThing: %s", UUID))

	// Delete the row in the database
	err := f.client.Query(
		deleteThingStatement,
		f.convUUIDtoCQLUUID(UUID),
		f.convUUIDtoCQLUUID(thing.Key.NrDollarCref),
		thing.CreationTimeUnix,
	).Exec()

	// If there is an error, add an error message and return
	if err != nil {
		f.messaging.ErrorMessage(err)
		return err
	}

	return nil
}

// HistoryThing fills the history of a thing based on its UUID
func (f *Cassandra) HistoryThing(UUID strfmt.UUID, history *models.ThingHistory) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("HistoryThing: %s", UUID))

	// Run the query to delete the thing based on its UUID.
	iter := f.client.Query(selectThingHistoryStatement, f.convUUIDtoCQLUUID(UUID)).Iter()

	// Initialize the 'found' variable
	found := false

	// Put everything in a for loop, although there is only one result in this case
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Update the 'found' variable
		found = true

		// Get the class from the map
		class := m[colNodeClass].(string)

		// Set the key in the response
		history.Key = f.createCrefObject(f.convCQLUUIDtoUUID(m[colNodeOwner].(gocql.UUID)), f.serverAddress, connutils.RefTypeKey)

		// Fill the response with the row
		historyObject := &models.ThingHistoryObject{}
		historyObject.AtClass = class
		historyObject.AtContext = m[colNodeContext].(string)
		historyObject.CreationTimeUnix = connutils.MakeUnixMillisecond(m[colNodeCreationTime].(time.Time))

		// Initialize the schema in the response
		responseSchema := make(map[string]interface{})

		// Fill the response schema
		err := f.fillResponseSchema(&responseSchema, m[colNodeProperties].(map[string]string), class, f.schema.ThingSchema.Schema)

		// Return error if there is one
		if err != nil {
			return err
		}

		// Fill the schema in the response object
		historyObject.Schema = responseSchema

		history.PropertyHistory = append(history.PropertyHistory, historyObject)
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return err
	}

	// If there is no history found, return an error
	if !found {
		return errors_.New(connutils.StaticNoHistoryFound)
	}

	return nil
}

// MoveToHistoryThing moves the thing-properties to the history table based on a given UUID
// Note that the thing related to the UUID is not updated yet.
func (f *Cassandra) MoveToHistoryThing(thing *models.Thing, UUID strfmt.UUID, deleted bool) error {
	// Convert UUID
	cqlUUID := f.convUUIDtoCQLUUID(UUID)

	// Initialize the properties map to add into the database
	properties := map[string]string{}

	// Add Object properties using a callback.
	// The callback is needed because it may differ for every connector. Similar code is put into
	// the schema#UpdateObjectSchemaProperties function.
	callback := f.createPropertyCallback(&properties, cqlUUID, thing.AtClass)
	err := schema.UpdateObjectSchemaProperties(connutils.RefTypeThing, thing, thing.Schema, f.schema, callback)

	if err != nil {
		return err
	}

	// Add the thing properties to the history
	err = f.client.Query(
		insertThingHistoryStatement,
		cqlUUID,
		f.convUUIDtoCQLUUID(thing.Key.NrDollarCref),
		time.Now(),
		deleted,
		thing.AtClass,
		thing.AtContext,
		properties,
	).Exec()

	if err != nil {
		return err
	}

	// No errors, return nil
	return nil
}

// AddAction adds an action to the Cassandra database with the given UUID.
// Takes the action and a UUID as input.
// Action is already validated against the ontology
func (f *Cassandra) AddAction(action *models.Action, UUID strfmt.UUID) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("AddAction: %s", UUID))

	// Run the query to add the thing based on its UUID.
	err := f.addActionRow(action, UUID)

	// Also log the error message
	if err != nil {
		f.messaging.ErrorMessage(err)
	}

	// If success return nil, otherwise return the error
	return err
}

// GetAction fills the given ActionGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("GetAction: %s", UUID))

	// Do the query to get the action from the database and get the iterator
	iter := f.client.Query(selectActionStatement, f.convUUIDtoCQLUUID(UUID)).Iter()

	// Initialize the 'found' variable
	found := false

	// Put everything in a for loop, although there is only one result in this case
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Fill the response with the row
		f.fillActionResponseWithRow(m, actionResponse)

		// Update the 'found' variable
		found = true
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return err
	}

	// If there is no action found, return an error
	if !found {
		return errors_.New(connutils.StaticActionNotFound)
	}

	// No errors, return nil
	return nil
}

// ListActions fills the given ActionListResponse with the values from the database, based on the given parameters.
func (f *Cassandra) ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now())

	// Get the filter on class
	whereFilter, err := f.parseWhereFilters(wheres, false)

	if err != nil {
		return err
	}

	// Build the query
	query := ""
	countQuery := ""
	if whereFilter != "" {
		query = fmt.Sprintf(listActionsSelectStatement, tableActionsClassSearch, whereFilter)
		countQuery = fmt.Sprintf(listActionsCountStatement, tableActionsClassSearch, whereFilter)
	} else {
		query = fmt.Sprintf(listActionsSelectStatement, tableActionsList, "")
		countQuery = fmt.Sprintf(listActionsCountStatement, tableActionsList, "")
	}

	// Do the query to get the action from the database and get the iterator
	iter := f.client.Query(query, f.convUUIDtoCQLUUID(UUID), first+offset).Iter()

	// Counter for offset
	offsetCount := 0

	// Put everything in a for loop
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Update offset count
		offsetCount = offsetCount + 1

		// Continue when the offset isnt reached yet
		if offset >= offsetCount {
			continue
		}

		// Fill the response with the row
		actionResponse := models.ActionGetResponse{}
		f.fillActionResponseWithRow(m, &actionResponse)

		// Add the action to the list in the response
		actionsResponse.Actions = append(actionsResponse.Actions, &actionResponse)
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return err
	}

	// Query for the total count of actions
	var actionsCount int64
	if err := f.client.Query(countQuery, f.convUUIDtoCQLUUID(UUID)).Scan(&actionsCount); err != nil {
		return err
	}

	// Fill the total results with this specs
	actionsResponse.TotalResults = actionsCount

	// No errors, return nil
	return nil
}

// UpdateAction updates the Action in the DB at the given UUID.
func (f *Cassandra) UpdateAction(action *models.Action, UUID strfmt.UUID) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("UpdateAction: %s", UUID))

	// Run the query to update the action based on its UUID.
	err := f.addActionRow(action, UUID)

	// If there is an error, add an error message
	if err != nil {
		f.messaging.ErrorMessage(err)
	}

	// If success return nil, otherwise return the error
	return err
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *Cassandra) DeleteAction(action *models.Action, UUID strfmt.UUID) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("DeleteAction: %s", UUID))

	// Delete the row in the database
	err := f.client.Query(
		deleteActionStatement,
		f.convUUIDtoCQLUUID(UUID),
		f.convUUIDtoCQLUUID(action.Key.NrDollarCref),
		action.CreationTimeUnix,
		f.convUUIDtoCQLUUID(action.Things.Object.NrDollarCref),
	).Exec()

	// If there is an error, add an error message and return
	if err != nil {
		f.messaging.ErrorMessage(err)
		return err
	}

	// If success return nil, otherwise return the error
	return nil
}

// HistoryAction fills the history of a action based on its UUID
func (f *Cassandra) HistoryAction(UUID strfmt.UUID, history *models.ActionHistory) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("HistoryAction: %s", UUID))

	// Run the query to delete the action based on its UUID.
	iter := f.client.Query(selectActionHistoryStatement, f.convUUIDtoCQLUUID(UUID)).Iter()

	// Initialize the 'found' variable
	found := false

	// Put everything in a for loop, although there is only one result in this case
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Update the 'found' variable
		found = true

		// Get the class from the map
		class := m[colNodeClass].(string)

		// Set the key in the response
		history.Key = f.createCrefObject(f.convCQLUUIDtoUUID(m[colNodeOwner].(gocql.UUID)), f.serverAddress, connutils.RefTypeKey)

		// Fill the response with the row
		historyObject := &models.ActionHistoryObject{}
		historyObject.AtClass = class
		historyObject.AtContext = m[colNodeContext].(string)
		historyObject.CreationTimeUnix = connutils.MakeUnixMillisecond(m[colNodeCreationTime].(time.Time))

		historyObject.Things = &models.ObjectSubject{}

		// Fill the things-object part of the response with a c-ref
		historyObject.Things.Object = f.createCrefObject(
			f.convCQLUUIDtoUUID(m[colActionObjectUUID].(gocql.UUID)),
			m[colActionObjectLocation].(string),
			connutils.RefTypeThing,
		)

		// Fill the things-object part of the response with a c-ref
		historyObject.Things.Subject = f.createCrefObject(
			f.convCQLUUIDtoUUID(m[colActionSubjectUUID].(gocql.UUID)),
			m[colActionSubjectLocation].(string),
			connutils.RefTypeThing,
		)

		// Initialize the schema in the response
		responseSchema := make(map[string]interface{})

		// Fill the response schema
		err := f.fillResponseSchema(&responseSchema, m[colNodeProperties].(map[string]string), class, f.schema.ActionSchema.Schema)

		// Return error if there is one
		if err != nil {
			return err
		}

		// Fill the schema in the response object
		historyObject.Schema = responseSchema

		history.PropertyHistory = append(history.PropertyHistory, historyObject)
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return err
	}

	// If there is no history found, return an error
	if !found {
		return errors_.New(connutils.StaticNoHistoryFound)
	}

	return nil
}

// MoveToHistoryAction moves the action-properties to the history table based on a given UUID
// Note that the action related to the UUID is not updated yet.
func (f *Cassandra) MoveToHistoryAction(action *models.Action, UUID strfmt.UUID, deleted bool) error {
	// Convert UUID
	cqlUUID := f.convUUIDtoCQLUUID(UUID)

	// Initialize the properties map to add into the database
	properties := map[string]string{}

	// Add Object properties using a callback.
	// The callback is needed because it may differ for every connector. Similar code is put into
	// the schema#UpdateObjectSchemaProperties function.
	callback := f.createPropertyCallback(&properties, cqlUUID, action.AtClass)
	err := schema.UpdateObjectSchemaProperties(connutils.RefTypeAction, action, action.Schema, f.schema, callback)

	if err != nil {
		return err
	}

	// Add the action properties to the history
	err = f.client.Query(
		insertActionHistoryStatement,
		cqlUUID,
		f.convUUIDtoCQLUUID(action.Key.NrDollarCref),
		time.Now(),
		deleted,
		action.AtClass,
		action.AtContext,
		properties,
		f.convUUIDtoCQLUUID(action.Things.Subject.NrDollarCref),
		action.Things.Subject.LocationURL,
		f.convUUIDtoCQLUUID(action.Things.Object.NrDollarCref),
		action.Things.Object.LocationURL,
	).Exec()

	if err != nil {
		return err
	}

	// No errors, return nil
	return nil
}

// AddKey adds a key to the Cassandra database with the given UUID and token.
// UUID  = reference to the key
// token = is the actual access token used in the API's header
func (f *Cassandra) AddKey(key *models.Key, UUID strfmt.UUID, token string) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("AddKey: %s", UUID))

	// Determine whether the key is the root-key
	isRoot := key.Parent == nil

	// Determine the parent UUID
	var parent interface{}
	if !isRoot {
		parent = f.convUUIDtoCQLUUID(key.Parent.NrDollarCref)
	} else {
		parent = nil
	}

	// Add the key to the database
	query := f.client.Query(
		insertKeyStatement,
		token,
		f.convUUIDtoCQLUUID(UUID),
		parent,
		isRoot,
		key.Read,
		key.Write,
		key.Delete,
		key.Execute,
		key.Email,
		key.IPOrigin,
		key.KeyExpiresUnix,
	)

	// If success return nil, otherwise return the error
	return query.Exec()
}

// ValidateToken validates/gets a key to the Cassandra database with the given token (=UUID)
func (f *Cassandra) ValidateToken(UUID strfmt.UUID, keyResponse *models.KeyGetResponse) (token string, err error) {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("ValidateToken: %s", token))

	// Do the query to get the key from the database based on access-token and get the iterator
	iter := f.client.Query(selectKeyStatement, f.convUUIDtoCQLUUID(UUID)).Consistency(gocql.One).Iter()

	// Initialize the 'found' variable
	found := false

	// Put everything in a for loop, although there is only one result in this case
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		token = m[colKeyToken].(string)

		// Fill the response with the row
		f.fillKeyResponseWithRow(m, keyResponse)

		// Update the 'found' variable
		found = true
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return token, err
	}

	// If there is no key found, return an error
	if !found {
		return token, errors_.New(connutils.StaticKeyNotFound)
	}

	// No errors, return nil
	return token, nil
}

// GetKey fills the given KeyGetResponse with the values from the database, based on the given UUID.
func (f *Cassandra) GetKey(UUID strfmt.UUID, keyResponse *models.KeyGetResponse) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("GetKey: %s", UUID))

	// Do the query to get the key from the database and get the iterator
	iter := f.client.Query(selectKeyStatement, f.convUUIDtoCQLUUID(UUID)).Iter()

	// Initialize the 'found' variable
	found := false

	// Put everything in a for loop, although there is only one result in this case
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Fill the response with the row
		f.fillKeyResponseWithRow(m, keyResponse)

		// Update the 'found' variable
		found = true
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		return err
	}

	// If there is no key found, return an error
	if !found {
		return errors_.New(connutils.StaticKeyNotFound)
	}

	// No errors, return nil
	return nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Cassandra) DeleteKey(key *models.Key, UUID strfmt.UUID) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("DeleteKey: %s", UUID))

	// Run the query to delete the key based on its UUID.
	err := f.client.Query(
		deleteKeyStatement,
		f.convUUIDtoCQLUUID(UUID),
	).Exec()

	// If there is an error, add an error message and return
	if err != nil {
		f.messaging.ErrorMessage(err)
		return err
	}

	// If success return nil, otherwise return the error
	return nil
}

// GetKeyChildren fills the given KeyGetResponse array with the values from the database, based on the given UUID.
func (f *Cassandra) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyGetResponse) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("GetKeyChildren: %s", UUID))

	// Do the query to get the thing from the database and get the iterator
	iter := f.client.Query(selectKeyChildrenStatement, f.convUUIDtoCQLUUID(UUID)).Iter()

	// Put everything in a for loop
	for {
		// Init the map for each row
		m := map[string]interface{}{}

		// Fill the map with the current row in the iterator
		if !iter.MapScan(m) {
			break
		}

		// Fill the response with the row
		keyResponse := models.KeyGetResponse{}
		f.fillKeyResponseWithRow(m, &keyResponse)

		// Add the thing to the list in the response
		*children = append(*children, &keyResponse)
	}

	// Close the iterator to get errors
	if err := iter.Close(); err != nil {
		f.messaging.ErrorMessage(err)
		return err
	}

	// No errors, return nil
	return nil
}

// UpdateKey updates the Key in the DB at the given UUID.
func (f *Cassandra) UpdateKey(key *models.Key, UUID strfmt.UUID, token string) error {
	// Track time of this function for debug reasons
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("UpdateKey: %s", UUID))

	// Determine whether the key is the root-key
	isRoot := key.Parent == nil

	// Determine the parent UUID
	var parent interface{}
	if !isRoot {
		parent = f.convUUIDtoCQLUUID(key.Parent.NrDollarCref)
	} else {
		parent = nil
	}

	// Add the key to the database
	query := f.client.Query(
		updateKeyStatement,
		token,
		parent,
		key.Read,
		key.Write,
		key.Delete,
		key.Execute,
		key.Email,
		key.IPOrigin,
		key.KeyExpiresUnix,
		f.convUUIDtoCQLUUID(UUID),
		isRoot,
	)

	// If success return nil, otherwise return the error
	return query.Exec()
}

// convUUIDtoCQLUUID converts an UUID of type strfmt.UUID to an UUID of type gocql.UUID
func (f *Cassandra) convUUIDtoCQLUUID(UUID strfmt.UUID) gocql.UUID {
	cqlUUID, _ := gocql.ParseUUID(string(UUID))
	return cqlUUID
}

// convCQLUUIDtoUUID converts an UUID of type gocql.UUID to an UUID of type strfmt.UUID
func (f *Cassandra) convCQLUUIDtoUUID(cqlUUID gocql.UUID) strfmt.UUID {
	UUID := strfmt.UUID(cqlUUID.String())
	return UUID
}

// createPropertyCallback is an function-callback which fills the properties based on datatype
func (f *Cassandra) createPropertyCallback(properties *map[string]string, cqlUUID gocql.UUID, className string) func(string, interface{}, *schema.DataType, string) error {
	// Return a function that fills in the properties
	return func(propKey string, propValue interface{}, dataType *schema.DataType, edgeName string) error {
		// Initialize the data value
		var dataValue string

		// For all data types, convert the value into the right type
		if *dataType == schema.DataTypeBoolean {
			// Parse the boolean value as string
			dataValue = strconv.FormatBool(propValue.(bool))
		} else if *dataType == schema.DataTypeDate {
			// Parse the time value as string
			dataValue = propValue.(time.Time).Format(time.RFC3339)
		} else if *dataType == schema.DataTypeInt {
			// Parse the integer value as string
			switch propValue.(type) {
			case int64:
				dataValue = strconv.FormatInt(propValue.(int64), 10)
			case float64:
				dataValue = strconv.FormatInt(int64(propValue.(float64)), 10)
			}
		} else if *dataType == schema.DataTypeNumber {
			// Parse the float value as string
			dataValue = strconv.FormatFloat(propValue.(float64), 'f', -1, 64)
		} else if *dataType == schema.DataTypeString {
			// Parse the string value as string
			dataValue = propValue.(string)
		} else if *dataType == schema.DataTypeCRef {
			// Parse the c-ref value, based on three values in the raw-properties
			// Just use the first found propkey to fill the c-ref.
			cref := propValue.(*models.SingleRef)
			(*properties)[schema.SchemaPrefix+propKey+".location_url"] = *cref.LocationURL
			(*properties)[schema.SchemaPrefix+propKey+".type"] = cref.Type
			(*properties)[schema.SchemaPrefix+propKey+".cref"] = string(cref.NrDollarCref)
		}

		// Set the value parsed above
		if dataValue != "" {
			(*properties)[schema.SchemaPrefix+propKey] = dataValue
		}

		return nil
	}
}

// fillKeyResponseWithRow fills a keyResponse object with data from a single map-row from the database
func (f *Cassandra) fillKeyResponseWithRow(row map[string]interface{}, keyResponse *models.KeyGetResponse) error {
	// Fill all values of the response
	keyResponse.KeyID = f.convCQLUUIDtoUUID(row[colKeyUUID].(gocql.UUID))
	keyResponse.Delete = row[colKeyAllowD].(bool)
	keyResponse.Email = row[colKeyEmail].(string)
	keyResponse.Execute = row[colKeyAllowX].(bool)
	keyResponse.IPOrigin = row[colKeyIPOrigin].([]string)
	keyResponse.KeyExpiresUnix = connutils.MakeUnixMillisecond(row[colKeyExpiryTime].(time.Time))
	keyResponse.Read = row[colKeyAllowR].(bool)
	keyResponse.Write = row[colKeyAllowW].(bool)
	isRoot := row[colKeyRoot].(bool)
	keyResponse.IsRoot = &isRoot

	// Determine the parent and add it as an cref
	if !isRoot {
		keyResponse.Parent = f.createCrefObject(f.convCQLUUIDtoUUID(row[colKeyParent].(gocql.UUID)), f.serverAddress, connutils.RefTypeKey)
	}

	// Return nill as there is no error generated
	return nil
}

// fillThingResponseWithRow fills a thingResponse object with data from a single map-row from the database
func (f *Cassandra) fillThingResponseWithRow(row map[string]interface{}, thingResponse *models.ThingGetResponse) error {
	// Initialize the schema in the response
	responseSchema := make(map[string]interface{})

	// Set the class variable for further use
	class := row[colNodeClass].(string)

	// Fill all values of the response
	thingResponse.ThingID = f.convCQLUUIDtoUUID(row[colThingUUID].(gocql.UUID))
	thingResponse.AtClass = class
	thingResponse.AtContext = row[colNodeContext].(string)
	thingResponse.CreationTimeUnix = connutils.MakeUnixMillisecond(row[colNodeCreationTime].(time.Time))

	// Fill in the owner object
	thingResponse.Key = f.createCrefObject(f.convCQLUUIDtoUUID(row[colNodeOwner].(gocql.UUID)), f.serverAddress, connutils.RefTypeKey)

	// Determine the last update time and don't set it (let it be nil) when the value is invalid
	lut := connutils.MakeUnixMillisecond(row[colNodeLastUpdateTime].(time.Time))
	if lut > 0 {
		thingResponse.LastUpdateTimeUnix = lut
	}

	// Fill the response schema
	err := f.fillResponseSchema(&responseSchema, row[colNodeProperties].(map[string]string), class, f.schema.ThingSchema.Schema)

	// Return error if there is one
	if err != nil {
		return err
	}

	// Fill the schema in the response object
	thingResponse.Schema = responseSchema

	// No error, return nil
	return nil
}

// fillActionResponseWithRow fills a actionResponse object with data from a single map-row from the database
func (f *Cassandra) fillActionResponseWithRow(row map[string]interface{}, actionResponse *models.ActionGetResponse) error {
	// Initialize the schema in the response
	responseSchema := make(map[string]interface{})

	// Set the class variable for further use
	class := row[colNodeClass].(string)

	// Fill all values of the response
	actionResponse.ActionID = f.convCQLUUIDtoUUID(row[colActionUUID].(gocql.UUID))
	actionResponse.AtClass = class
	actionResponse.AtContext = row[colNodeContext].(string)
	actionResponse.CreationTimeUnix = connutils.MakeUnixMillisecond(row[colNodeCreationTime].(time.Time))
	actionResponse.Things = &models.ObjectSubject{}

	// Fill the things-object part of the response with a c-ref
	actionResponse.Things.Object = f.createCrefObject(
		f.convCQLUUIDtoUUID(row[colActionObjectUUID].(gocql.UUID)),
		row[colActionObjectLocation].(string),
		connutils.RefTypeThing,
	)

	// Fill the things-object part of the response with a c-ref
	actionResponse.Things.Subject = f.createCrefObject(
		f.convCQLUUIDtoUUID(row[colActionSubjectUUID].(gocql.UUID)),
		row[colActionSubjectLocation].(string),
		connutils.RefTypeThing,
	)

	// Fill in the owner object
	actionResponse.Key = f.createCrefObject(f.convCQLUUIDtoUUID(row[colNodeOwner].(gocql.UUID)), f.serverAddress, connutils.RefTypeKey)

	// Determine the last update time and don't set it (let it be nil) when the value is invalid
	lut := connutils.MakeUnixMillisecond(row[colNodeLastUpdateTime].(time.Time))
	if lut > 0 {
		actionResponse.LastUpdateTimeUnix = lut
	}

	// Fill the response schema
	err := f.fillResponseSchema(&responseSchema, row[colNodeProperties].(map[string]string), class, f.schema.ActionSchema.Schema)

	// Return error if there is one
	if err != nil {
		return err
	}

	// Fill the schema in the response object
	actionResponse.Schema = responseSchema

	// No error, return nil
	return nil
}

// fillResponseSchema is a function to fill the schema part of a response with raw data from the database
func (f *Cassandra) fillResponseSchema(responseSchema *map[string]interface{}, p map[string]string, class string, schemaType *models.SemanticSchema) error {
	// For each raw value in the database, get the key and the value
	for key, value := range p {
		// Based on key and class, get the datatype
		if isSchema, propKey, dataType, err := schema.TranslateSchemaPropertiesFromDataBase(key, class, schemaType); isSchema {
			// If there is an error, caused by for instance an non-existing class or key, return the error
			if err != nil {
				return err
			}

			// For all data types, convert the value into the right type
			if *dataType == schema.DataTypeBoolean {
				// Parse the boolean value
				(*responseSchema)[propKey] = connutils.Must(strconv.ParseBool(value)).(bool)
			} else if *dataType == schema.DataTypeDate {
				// Parse the time value
				t, err := time.Parse(time.RFC3339, value)

				// Return if there is an error while parsing
				if err != nil {
					return err
				}
				(*responseSchema)[propKey] = t
			} else if *dataType == schema.DataTypeInt {
				// Parse the integer value
				(*responseSchema)[propKey] = connutils.Must(strconv.ParseInt(value, 10, 64)).(int64)
			} else if *dataType == schema.DataTypeNumber {
				// Parse the float value
				(*responseSchema)[propKey] = connutils.Must(strconv.ParseFloat(value, 64)).(float64)
			} else if *dataType == schema.DataTypeString {
				// Parse the string value
				(*responseSchema)[propKey] = value
			} else if *dataType == schema.DataTypeCRef {
				// Parse the c-ref value, based on three values in the raw-properties
				// Just use the first found propkey to fill the c-ref.
				if (*responseSchema)[propKey] == nil {
					prefix := schema.SchemaPrefix + propKey + "."
					(*responseSchema)[propKey] = f.createCrefObject(
						strfmt.UUID(p[prefix+"cref"]),
						p[prefix+"location_url"],
						connutils.RefType(p[prefix+"type"]),
					)
				}
			}
		}
	}

	return nil
}

// createCrefObject is a helper function to create a cref-object. This function is used for Cassandra only.
func (f *Cassandra) createCrefObject(UUID strfmt.UUID, location string, refType connutils.RefType) *models.SingleRef {
	// Create the 'cref'-node for the response.
	crefObj := models.SingleRef{}

	// Get the given node properties to generate response object
	crefObj.NrDollarCref = UUID
	crefObj.Type = string(refType)
	url := location
	crefObj.LocationURL = &url

	return &crefObj
}

func (f *Cassandra) parseWhereFilters(wheres []*connutils.WhereQuery, useWhere bool) (string, error) {
	filterWheres := ""

	// Create filter query
	var op string
	var prop string
	var value string
	for _, vWhere := range wheres {
		// Set the operator
		if vWhere.Value.Operator == connutils.Equal || vWhere.Value.Operator == connutils.NotEqual {
			// Set the value from the object (now only string)
			// TODO: https://github.com/creativesoftwarefdn/weaviate/issues/202
			value = vWhere.Value.Value.(string)

			if vWhere.Value.Contains {
				continue // TODO
			} else {
				op = "="
				value = fmt.Sprintf(`'%s'`, value)
			}

			if vWhere.Value.Operator == connutils.NotEqual {
				op = "="
				return "", errors_.New("Searching on not-equal is not supported in Cassandra.")
			}
		}

		// Set the property
		prop = vWhere.Property
		if prop == "atClass" {
			prop = "class"
		}
		// else if strings.HasPrefix(prop, schema.SchemaPrefix) {
		// 	prop = fmt.Sprintf("%s['%s']", PropertiesColumn, prop)
		// }

		// Filter on wheres variable which is used later in the query
		andOp := ""
		if useWhere {
			andOp = "WHERE"
		} else {
			andOp = "AND"
		}

		// Parse the filter 'wheres'. Note that the 'value' may need block-quotes.
		filterWheres = fmt.Sprintf(`%s %s %s %s %s`, filterWheres, andOp, prop, op, value)
	}

	return filterWheres, nil
}

// addThingRow adds a single thing row into the database for the given thing based on the given UUID
func (f *Cassandra) addThingRow(thing *models.Thing, UUID strfmt.UUID) error {
	// Parse UUID in Cassandra type
	cqlUUID := f.convUUIDtoCQLUUID(UUID)

	// Initialize the properties map to add into the database
	properties := map[string]string{}

	// Add Object properties using a callback.
	// The callback is needed because it may differ for every connector. Similar code is put into
	// the schema#UpdateObjectSchemaProperties function.
	callback := f.createPropertyCallback(&properties, cqlUUID, thing.AtClass)
	err := schema.UpdateObjectSchemaProperties(connutils.RefTypeThing, thing, thing.Schema, f.schema, callback)

	if err != nil {
		return err
	}

	// Check whether last updated time is set, otherwise 'nil'
	var lut interface{}
	lut = thing.LastUpdateTimeUnix
	if lut.(int64) <= 0 {
		lut = nil
	}

	// Insert data into the database
	query := f.client.Query(
		insertThingStatement,
		cqlUUID,
		f.convUUIDtoCQLUUID(thing.Key.NrDollarCref),
		thing.CreationTimeUnix,
		lut,
		thing.AtClass,
		thing.AtContext,
		properties,
	)

	// Run the query, return the error
	return query.Exec()
}

// addActionRow adds a single action row into the database for the given action based on the given UUID
func (f *Cassandra) addActionRow(action *models.Action, UUID strfmt.UUID) error {
	// Parse UUID in Cassandra type
	cqlUUID := f.convUUIDtoCQLUUID(UUID)

	// Initialize the properties map to add into the database
	properties := map[string]string{}

	// Add Object properties using a callback.
	// The callback is needed because it may differ for every connector. Similar code is put into
	// the schema#UpdateObjectSchemaProperties function.
	callback := f.createPropertyCallback(&properties, cqlUUID, action.AtClass)
	err := schema.UpdateObjectSchemaProperties(connutils.RefTypeAction, action, action.Schema, f.schema, callback)

	if err != nil {
		return err
	}

	// Check whether last updated time is set, otherwise 'nil'
	var lut interface{}
	lut = action.LastUpdateTimeUnix
	if lut.(int64) <= 0 {
		lut = nil
	}

	// Insert data into the database
	query := f.client.Query(
		insertActionStatement,
		cqlUUID,
		f.convUUIDtoCQLUUID(action.Key.NrDollarCref),
		action.CreationTimeUnix,
		lut,
		action.AtClass,
		action.AtContext,
		properties,
		f.convUUIDtoCQLUUID(action.Things.Subject.NrDollarCref),
		action.Things.Subject.LocationURL,
		f.convUUIDtoCQLUUID(action.Things.Object.NrDollarCref),
		action.Things.Object.LocationURL,
	)

	// Run the query, return the error
	return query.Exec()
}
