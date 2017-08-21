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
	"context"
	"encoding/json"
	errors_ "errors"
	"fmt"
	"github.com/go-openapi/strfmt"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"

	dgraphClient "github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"github.com/dgraph-io/dgraph/types"
	gouuid "github.com/satori/go.uuid"

	"github.com/weaviate/weaviate/connectors/config"
	"github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/models"
	"github.com/weaviate/weaviate/schema"
)

// Dgraph has some basic variables.
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

// Connect creates connection and tables if not already available
func (f *Dgraph) Connect() error {
	// Connect with Dgraph host, create connection dail
	// TODO: Put hostname in config using the SetConfig function (configInput.Database.DatabaseConfig and making custom struct)
	dgraphGrpcAddress := "127.0.0.1:9080"

	conn, err := grpc.Dial(dgraphGrpcAddress, grpc.WithInsecure())
	if err != nil {
		log.Println("dail error", err)
	}
	// defer conn.Close()

	// Create temp-folder for caching
	dir, err := ioutil.TempDir("", "weaviate_dgraph")

	if err != nil {
		return err
	}
	// defer os.RemoveAll(dir)

	// Set custom options
	var options = dgraphClient.BatchMutationOptions{
		Size:          100,
		Pending:       100,
		PrintCounters: true,
		MaxRetries:    math.MaxUint32,
		Ctx:           f.getContext(),
	}

	f.client = dgraphClient.NewDgraphClient([]*grpc.ClientConn{conn}, options, dir)
	// defer f.client.Close()

	return nil
}

// Init creates a root key, normally this should be validaded, but because it is an indgraph DB it is created always
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

		// Create local file
		thingSchema, _ := os.Create(localThingSchemaFile)
		defer thingSchema.Close()

		// Get the file from online
		resp, err := http.Get(f.thingSchemaLocation)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// Write file to local file
		b, _ := io.Copy(thingSchema, resp.Body)
		log.Println("Download complete, file size: ", b)
	} else {
		log.Println("Given Thing schema location is not a valid URL, looking for local file...")

		// Given Thing schema location is not a valid URL, assume it is a local file
		localThingSchemaFile = f.thingSchemaLocation
	}

	// Read local file which is either just downloaded or given in config.
	log.Println("Read local file...")
	fileContents, err := ioutil.ReadFile(localThingSchemaFile)

	// Return error when error is given reading file.
	if err != nil {
		return err
	}

	// Merge JSON into Schema objects
	thingSchema := &schema.Schema{}
	err = json.Unmarshal([]byte(fileContents), &thingSchema)
	log.Println("File is loaded.")

	// Return error when error is given reading file.
	if err != nil {
		log.Println("Can not parse schema.")
		return err
	}

	// Add class schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "class",
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add type schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "type",
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add ID schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "id",
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
	}); err != nil {
		return err
	}

	// Add UUID schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "uuid",
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add search possibilities for every "timing"
	thingTimings := []string{
		"creationTimeMs",
		"lastSeenTimeMs",
		"lastUpdateTimeMs",
		"lastUseTimeMs",
	}

	// For every timing, add them in the DB
	for _, ms := range thingTimings {
		if err := f.client.AddSchema(protos.SchemaUpdate{
			Predicate: ms,
			ValueType: uint32(types.IntID),
			Tokenizer: []string{"int"},
			Directive: protos.SchemaUpdate_INDEX,
			Count:     true,
		}); err != nil {
			return err
		}
	}

	// Get all classes to verify we do not create duplicates
	allClasses, err := f.getAllClasses()

	// Init flush variable
	flushIt := false

	// Add schema to database
	for _, class := range thingSchema.Classes {
		// for _, prop := range class.Properties {
		for _ = range class.Properties {
			// Add Dgraph-schema for every property of individual nodes
			// err = f.client.AddSchema(protos.SchemaUpdate{
			// 	Predicate: "schema." + prop.Name,
			// 	ValueType: uint32(types.UidID),
			// 	Directive: protos.SchemaUpdate_REVERSE,
			// })

			// if err != nil {
			// 	return err
			// }

			// TODO: Add specific schema for datatypes
			// http://schema.org/DataType
		}

		// Create the class name by concatinating context with class
		// TODO: Do not concatinate, split and add classname and context seperatly to node?
		className := thingSchema.Context + "/" + class.Class

		if _, err := f.getClassFromResult(className, allClasses); err != nil {
			// Add node and edge for every class
			// TODO: Only add if not exists
			node, err := f.client.NodeBlank(class.Class)

			if err != nil {
				return err
			}

			// Add class edge
			edge := node.Edge("class")
			err = edge.SetValueString(className)

			if err != nil {
				return err
			}

			// Add class edge to batch
			err = f.client.BatchSet(edge)

			if err != nil {
				return err
			}

			flushIt = true
		}
	}

	// Call flush to flush buffers after all mutations are added
	if flushIt {
		err = f.client.BatchFlush()

		if err != nil {
			return err
		}
	}

	return nil
}

// AddThing adds a thing to the Dgraph database with the given UUID
func (f *Dgraph) AddThing(thing *models.ThingCreate, UUID strfmt.UUID) error {
	// TODO: make type interactive
	// thingType := thing.AtContext + "/" + models.ThingCreate.type
	thingType := thing.AtContext + "/Event"

	// Search for the class to make the connection, create variables
	variables := make(map[string]string)
	variables["$a"] = thingType

	// Create the query for existing class
	req := dgraphClient.Req{}
	req.SetQueryWithVariables(`{
		class(func: eq(class, $a)) {
			_uid_
			class
		}
	}`, variables)

	// Run the query
	resp, err := f.client.Run(f.getContext(), &req)

	if err != nil {
		return err
	}

	// Unmarshal the result
	var dClass ClassResult
	err = dgraphClient.Unmarshal(resp.N, &dClass)

	if err != nil {
		return err
	}

	// Create the classNode from the result
	classNode := f.client.NodeUid(dClass.Root.ID)

	// Node has been found, create new one and connect it
	newThingNode, err := f.client.NodeBlank(fmt.Sprintf("%v", gouuid.NewV4()))

	if err != nil {
		return err
	}

	// Add edge between New Thing and Class Node
	typeEdge := newThingNode.ConnectTo("type", classNode)

	// Add class edge to batch
	req = dgraphClient.Req{}
	err = req.Set(typeEdge)

	if err != nil {
		return err
	}

	// Add UUID node
	uuidNode, err := f.client.NodeBlank(string(UUID))

	// Add UUID edge
	uuidEdge := newThingNode.ConnectTo("id", uuidNode)
	if err = req.Set(uuidEdge); err != nil {
		return err
	}

	// Add UUID to UUID node
	// TODO: Search for uuid edge/node before making new??
	edge := uuidNode.Edge("uuid")
	if err = edge.SetValueString(string(UUID)); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add timing nodes
	edge = newThingNode.Edge("creationTimeMs")
	if err = edge.SetValueInt(thing.CreationTimeMs); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	edge = newThingNode.Edge("lastSeenTimeMs")
	if err = edge.SetValueInt(thing.LastSeenTimeMs); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	edge = newThingNode.Edge("lastUpdateTimeMs")
	if err = edge.SetValueInt(thing.LastUpdateTimeMs); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	edge = newThingNode.Edge("lastUseTimeMs")
	if err = edge.SetValueInt(thing.LastUseTimeMs); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add Thing properties
	for propKey, propValue := range thing.Schema {
		// TODO: add property: string/int/connection other object, now everything is string
		// TODO: Add 'schema.' to a global var, if this is the nicest way to fix
		edgeName := "schema." + propKey
		if val, ok := propValue["value"]; ok {
			edge = newThingNode.Edge(edgeName)
			if err = edge.SetValueString(val.(string)); err != nil {
				return err
			}
			if err = req.Set(edge); err != nil {
				return err
			}
		} else if val, ok = propValue["ref"]; ok {
			refThingNode, err := f.getThingNodeByUUID(strfmt.UUID(val.(string)))
			if err != nil {
				return err
			}
			relatedEdge := newThingNode.ConnectTo(edgeName, refThingNode)
			if err = req.Set(relatedEdge); err != nil {
				return err
			}
		}

	}

	// Call flush to flush buffers after all mutations are added
	resp, err = f.client.Run(f.getContext(), &req)

	if err != nil {
		return err
	}

	return nil

	// TODO: Reset batch before and flush after every function??
}

// GetThing returns the thing in the ThingGetResponse format
func (f *Dgraph) GetThing(UUID strfmt.UUID) (models.ThingGetResponse, error) {
	// Initialize response
	thingResponse := models.ThingGetResponse{}
	thingResponse.Schema = map[string]models.JSONObject{}

	// Do a query to get all node-information based on the given UUID
	variables := make(map[string]string)
	variables["$uuid"] = string(UUID)

	req := dgraphClient.Req{}
	req.SetQueryWithVariables(`{ 
		get(func: eq(uuid, $uuid)) {
			uuid
			~id {
				expand(_all_) {
					expand(_all_)
				}
			}
		}
	}`, variables)

	// Run query created above
	resp, err := f.client.Run(f.getContext(), &req)
	if err != nil {
		return thingResponse, err
	}

	// Merge the results into the model to return
	nodes := resp.GetN()
	for _, node := range nodes {
		mergeNodeInResponse(node, &thingResponse)
	}

	return thingResponse, nil
}

// ListThings returns the thing in the ThingGetResponse format
func (f *Dgraph) ListThings(limit int, page int) (models.ThingsListResponse, error) {
	// Initialize response
	thingsResponse := models.ThingsListResponse{}
	thingsResponse.Things = make([]*models.ThingGetResponse, limit)

	// Do a query to get all node-information
	req := dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{ 
		things(func: has(type), orderdesc: creationTimeMs, first: %d, offset: %d)  {
			expand(_all_) {
				expand(_all_)
			}
		}
	}	
	`, limit, (page-1)*limit))

	// Run query created above
	resp, err := f.client.Run(f.getContext(), &req)
	if err != nil {
		return thingsResponse, err
	}

	// Merge the results into the model to return
	nodes := resp.GetN()
	for i, node := range nodes[0].Children {
		thingResponse := &models.ThingGetResponse{}
		thingResponse.Schema = map[string]models.JSONObject{}
		mergeNodeInResponse(node, thingResponse)
		thingsResponse.Things[i] = thingResponse
	}

	// Create query to count total results
	req = dgraphClient.Req{}
	req.SetQuery(`{ 
  		totalResults(func: has(type))  {
    		count()
  		}
	}`)

	// Run query created above
	resp, err = f.client.Run(f.getContext(), &req)
	if err != nil {
		return thingsResponse, err
	}

	// Unmarshal the dgraph response into a struct
	var totalResult TotalResultsResult
	err = dgraphClient.Unmarshal(resp.N, &totalResult)
	if err != nil {
		return thingsResponse, nil
	}

	// Set the total results
	thingsResponse.TotalResults = totalResult.Root.Count

	return thingsResponse, nil
}

// mergeNodeInResponse based on https://github.com/dgraph-io/dgraph/blob/release/v0.8.0/wiki/resources/examples/goclient/crawlerRDF/crawler.go#L250-L264
func mergeNodeInResponse(node *protos.Node, thingResponse *models.ThingGetResponse) {
	attribute := node.Attribute

	for _, prop := range node.GetProperties() {
		if attribute == "~id" || attribute == "things" {
			if prop.Prop == "creationTimeMs" {
				thingResponse.CreationTimeMs = prop.GetValue().GetIntVal()
			} else if prop.Prop == "lastSeenTimeMs" {
				thingResponse.LastSeenTimeMs = prop.GetValue().GetIntVal()
			} else if prop.Prop == "lastUpdateTimeMs" {
				thingResponse.LastUpdateTimeMs = prop.GetValue().GetIntVal()
			} else if prop.Prop == "lastUseTimeMs" {
				thingResponse.LastUseTimeMs = prop.GetValue().GetIntVal()
			} else {
				thingResponse.Schema[prop.Prop] = map[string]models.JSONValue{
					"value": prop.GetValue().GetStrVal(),
				}
			}
		} else if attribute == "type" {
			if prop.Prop == "context" {
				thingResponse.AtContext = prop.GetValue().GetStrVal()
			} else if prop.Prop == "class" {
				// thingResponse.AtType = "Person" TODO: FIX?
			}
		} else if attribute == "id" {
			if prop.Prop == "uuid" {
				thingResponse.ThingID = strfmt.UUID(prop.GetValue().GetStrVal())
			}
		}
	}

	for _, child := range node.Children {
		mergeNodeInResponse(child, thingResponse)
	}

}

func (f *Dgraph) getThingNodeByUUID(UUID strfmt.UUID) (dgraphClient.Node, error) {
	// Search for the class to make the connection, create variables
	variables := make(map[string]string)
	variables["$uuid"] = string(UUID)

	// Create the query for existing class
	req := dgraphClient.Req{}
	req.SetQueryWithVariables(`{ 
		thing(func: eq(uuid, $uuid)) {
			uuid
			~id {
				_uid_
			}
		}
	}`, variables)

	// Run the query
	resp, err := f.client.Run(f.getContext(), &req)

	if err != nil {
		return dgraphClient.Node{}, err
	}

	// Unmarshal the result
	var idResult ThingIDResult
	err = dgraphClient.Unmarshal(resp.N, &idResult)

	if err != nil {
		return dgraphClient.Node{}, err
	}

	// Create the classNode from the result
	node := f.client.NodeUid(idResult.Root.Node.ID)

	return node, err
}

// Add deprecated?
func (f *Dgraph) Add(dbObject connector_utils.DatabaseObject) (string, error) {

	// Return the ID that is used to create.
	return "hoi", nil
}

// Get deprecated?
func (f *Dgraph) Get(UUID string) (connector_utils.DatabaseObject, error) {

	return connector_utils.DatabaseObject{}, nil
}

// List deprecated?
func (f *Dgraph) List(refType string, ownerUUID string, limit int, page int, referenceFilter *connector_utils.ObjectReferences) (connector_utils.DatabaseObjects, int64, error) {
	dataObjs := connector_utils.DatabaseObjects{}

	return dataObjs, 0, nil
}

// ValidateKey Validate if a user has access, returns permissions object
func (f *Dgraph) ValidateKey(token string) ([]connector_utils.DatabaseUsersObject, error) {
	dbUsersObjects := []connector_utils.DatabaseUsersObject{{
		Deleted:        false,
		KeyExpiresUnix: -1,
		KeyToken:       "be9d1928-d004-4b0b-a8c3-2aad06cc67e1",
		Object:         "{}",
		Parent:         "",
		Uuid:           "ce9d1928-d004-4b0b-a8c3-2aad06cc67e1",
	}}

	// keys are found, return true
	return dbUsersObjects, nil
}

// GetKey returns user object by ID
func (f *Dgraph) GetKey(UUID string) (connector_utils.DatabaseUsersObject, error) {
	// Return found object
	return connector_utils.DatabaseUsersObject{}, nil

}

// AddKey to DB
func (f *Dgraph) AddKey(parentUUID string, dbObject connector_utils.DatabaseUsersObject) (connector_utils.DatabaseUsersObject, error) {

	// Return the ID that is used to create.
	return connector_utils.DatabaseUsersObject{}, nil

}

// DeleteKey removes a key from the database
func (f *Dgraph) DeleteKey(UUID string) error {

	return nil
}

// GetChildObjects returns all the child objects
func (f *Dgraph) GetChildObjects(UUID string, filterOutDeleted bool) ([]connector_utils.DatabaseUsersObject, error) {
	// Fill children array
	childUserObjects := []connector_utils.DatabaseUsersObject{}

	return childUserObjects, nil
}

func (f *Dgraph) getAllClasses() (AllClassesResult, error) {
	// Search for all existing classes
	req := dgraphClient.Req{}

	req.SetQuery(`{
		classes(func: has(class)) {
			_uid_
			class
		}
	}`)

	resp, err := f.client.Run(f.getContext(), &req)
	if err != nil {
		return AllClassesResult{}, err
	}

	var allClasses AllClassesResult
	err = dgraphClient.Unmarshal(resp.N, &allClasses)

	if err != nil {
		return AllClassesResult{}, err
	}

	return allClasses, nil
}

func (f *Dgraph) getClassFromResult(className string, allClasses AllClassesResult) (*DgraphClass, error) {
	for _, class := range allClasses.Root {
		if class.Class == className {
			return class, nil
		}
	}
	return &DgraphClass{}, errors_.New("class not found")
}

func (f *Dgraph) getContext() context.Context {
	return context.Background()
}
