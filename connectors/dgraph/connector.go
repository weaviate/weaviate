/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package dgraph

import (
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"

	dgraphClient "github.com/dgraph-io/dgraph/client"
	"github.com/weaviate/weaviate/connectors/config"
	"github.com/weaviate/weaviate/connectors/utils"
)

// Datastore has some basic variables.
type Dgraph struct {
	client              *dgraphClient.Dgraph
	kind                string
	thingSchemaLocation string
}

// GetName returns a unique connector name
func (f *Dgraph) GetName() string {
	return "dgraph"
}

// SetConfig is used to fill in a struct with config variables
func (f *Dgraph) SetConfig(configInput connectorConfig.Environment) {
	// println(configInput.Schemas.Action)
	f.thingSchemaLocation = configInput.Schemas.Thing
}

// Creates connection and tables if not already available
func (f *Dgraph) Connect() error {
	dgraphGrpcAddress := "http://localhost:9081"

	conn, err := grpc.Dial(dgraphGrpcAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	connections := []*grpc.ClientConn{
		conn,
	}

	f.client = dgraphClient.NewDgraphClient(connections, dgraphClient.DefaultOptions, "weaviate_dir_")
	defer f.client.Close()

	return nil
}

// Creates a root key, normally this should be validaded, but because it is an indgraph DB it is created always
func (f *Dgraph) Init() error {
	// Generate a basic DB object and print it's key.
	// dbObject := connector_utils.CreateFirstUserObject()

	// Init var for local file location
	var localThingSchemaFile string

	// Validate if given location is URL or local file
	_, err := url.ParseRequestURI(f.thingSchemaLocation)

	// With no error, it is an URL
	if err == nil {
		log.Println("Downloading Thing schema file...")
		localThingSchemaFile = "temp/thing-schema.json"

		thingSchema, _ := os.Create(localThingSchemaFile)
		defer thingSchema.Close()

		resp, _ := http.Get(f.thingSchemaLocation)
		defer resp.Body.Close()

		b, _ := io.Copy(thingSchema, resp.Body)
		log.Println("Download complete, file size: ", b)
	} else {
		log.Println("Given Thing schema location is not a valid URL, looking for local file...")

		localThingSchemaFile = f.thingSchemaLocation
	}

	// Read local file which is either just downloaded or given in config.
	log.Println("Read local file...")
	fileContents, err := ioutil.ReadFile(localThingSchemaFile)

	// Return error when error is given reading file.
	if err != nil {
		return err
	}

	fileContentJSON := string(fileContents)
	log.Println(fileContentJSON[0:100])

	return nil
}

func (f *Dgraph) Add(dbObject connector_utils.DatabaseObject) (string, error) {

	// Return the ID that is used to create.
	return "hoi", nil
}

func (f *Dgraph) Get(Uuid string) (connector_utils.DatabaseObject, error) {

	return connector_utils.DatabaseObject{}, nil
}

// return a list
func (f *Dgraph) List(refType string, ownerUUID string, limit int, page int, referenceFilter *connector_utils.ObjectReferences) (connector_utils.DatabaseObjects, int64, error) {
	dataObjs := connector_utils.DatabaseObjects{}

	return dataObjs, 0, nil
}

// Validate if a user has access, returns permissions object
func (f *Dgraph) ValidateKey(token string) ([]connector_utils.DatabaseUsersObject, error) {
	dbUsersObjects := []connector_utils.DatabaseUsersObject{}

	// keys are found, return true
	return dbUsersObjects, nil
}

// GetKey returns user object by ID
func (f *Dgraph) GetKey(Uuid string) (connector_utils.DatabaseUsersObject, error) {
	// Return found object
	return connector_utils.DatabaseUsersObject{}, nil

}

// AddUser to DB
func (f *Dgraph) AddKey(parentUuid string, dbObject connector_utils.DatabaseUsersObject) (connector_utils.DatabaseUsersObject, error) {

	// Return the ID that is used to create.
	return connector_utils.DatabaseUsersObject{}, nil

}

// DeleteKey removes a key from the database
func (f *Dgraph) DeleteKey(UUID string) error {

	return nil
}

// GetChildKeys returns all the child keys
func (f *Dgraph) GetChildObjects(UUID string, filterOutDeleted bool) ([]connector_utils.DatabaseUsersObject, error) {
	// Fill children array
	childUserObjects := []connector_utils.DatabaseUsersObject{}

	return childUserObjects, nil
}
