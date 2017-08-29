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
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/go-openapi/strfmt"
	"google.golang.org/grpc"

	dgraphClient "github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"github.com/dgraph-io/dgraph/types"
	gouuid "github.com/satori/go.uuid"

	"github.com/weaviate/weaviate/connectors/config"
	"github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/error"
	"github.com/weaviate/weaviate/models"
	"github.com/weaviate/weaviate/schema"
)

// Dgraph has some basic variables.
type Dgraph struct {
	client *dgraphClient.Dgraph
	kind   string

	actionSchema schemaProperties
	thingSchema  schemaProperties
}

const refTypePointer string = "_type_"
const schemaPrefix string = "schema."

type schemaProperties struct {
	localFile      string
	configLocation string
	schema         schema.Schema
}

// GetName returns a unique connector name
func (f *Dgraph) GetName() string {
	return "dgraph"
}

// SetConfig is used to fill in a struct with config variables
func (f *Dgraph) SetConfig(configInput connectorConfig.Environment) {
	f.thingSchema.configLocation = configInput.Schemas.Thing
	f.actionSchema.configLocation = configInput.Schemas.Action
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
	dir, err := ioutil.TempDir("temp", "weaviate_dgraph")

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

	configFiles := map[string]*schemaProperties{
		"Action": &f.actionSchema,
		"Thing":  &f.thingSchema,
	}

	// Err var
	var err error

	// Init flush variable
	flushIt := false

	for cfk, cfv := range configFiles {
		// Continue loop if the file is not set in the config.
		if len(cfv.configLocation) == 0 {
			errorHandler.ExitError(78, "schema file for '"+cfk+"' not given in config (path: *env*/schemas/"+cfk+"')")
			continue
		}

		// Validate if given location is URL or local file
		_, err := url.ParseRequestURI(cfv.configLocation)

		// With no error, it is an URL
		if err == nil {
			log.Println(cfk + ": Downloading schema file...")
			cfv.localFile = "temp/schema" + string(connector_utils.GenerateUUID()) + ".json"

			// Create local file
			schemaFile, _ := os.Create(cfv.localFile)
			defer schemaFile.Close()

			// Get the file from online
			resp, err := http.Get(cfv.configLocation)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			// Write file to local file
			b, _ := io.Copy(schemaFile, resp.Body)
			log.Println(cfk+": Download complete, file size: ", b)
		} else {
			log.Println(cfk + ": Given schema location is not a valid URL, using local file.")

			// Given schema location is not a valid URL, assume it is a local file
			cfv.localFile = cfv.configLocation
		}

		// Read local file which is either just downloaded or given in config.
		log.Println(cfk + ": Read local file " + cfv.localFile)

		fileContents, err := ioutil.ReadFile(cfv.localFile)

		// Return error when error is given reading file.
		if err != nil {
			log.Println(cfk + ": Schema file '" + cfv.localFile + "' could not be found.")
			return err
		}

		// Merge JSON into Schema objects
		err = json.Unmarshal([]byte(fileContents), &cfv.schema)
		log.Println(cfk + ": File is loaded.")

		// Return error when error is given reading file.
		if err != nil {
			log.Println(cfk + ": Can not parse schema.")
			return err
		}

		// Add schema to database
		for _, class := range cfv.schema.Classes {
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
		}
	}

	// Add class schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "atClass",
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add context schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "atContext",
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
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

	// Add refTypePointer schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: refTypePointer,
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add 'action.of' schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "action.of",
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add 'action.target' schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "action.target",
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add search possibilities for every "timing"
	thingTimings := []string{
		"creationTimeUnix",
		"lastUpdateTimeUnix",
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

	// KEYS
	// Add index for keys
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "key.child",
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add key for searching root
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "key.root",
		ValueType: uint32(types.BoolID),
		Tokenizer: []string{"bool"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return err
	}

	// Add ROOT-key if not exists
	// Search for Root key
	// req := dgraphClient.Req{}
	// req.SetQuery(`{
	// 	totalResults(func: eq(key.root, 1))  {
	// 		count()
	// 	}
	// }`)

	// // Run query created above
	// var resp *protos.Response
	// if resp, err = f.client.Run(f.getContext(), &req); err != nil {
	// 	return err
	// }

	// // Unmarshal the dgraph response into a struct
	// var totalResult TotalResultsResult
	// if err = dgraphClient.Unmarshal(resp.N, &totalResult); err != nil {
	// 	return err
	// }

	// // Set the total results
	// if totalResult.Root.Count == 0 {
	// 	log.Println("NO ROOTKEY YET")
	// 	userObject := connector_utils.CreateFirstUserObject()

	// 	log.Println(userObject)
	// }

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
func (f *Dgraph) AddThing(thing *models.Thing, UUID strfmt.UUID) error {
	// Create new node with base vars
	newNode, err := f.addNewNode(
		connector_utils.RefTypeThing,
		thing.AtContext,
		thing.AtClass,
		thing.CreationTimeUnix,
		thing.LastUpdateTimeUnix,
		UUID,
	)

	if err != nil {
		return err
	}

	// Add all given information to the new node
	err = f.updateThingNodeEdges(newNode, thing)

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
		f.mergeThingNodeInResponse(node, &thingResponse)
	}

	return thingResponse, nil
}

// ListThings returns the thing in the ThingGetResponse format
func (f *Dgraph) ListThings(limit int, page int) (models.ThingsListResponse, error) {
	// Initialize response
	thingsResponse := models.ThingsListResponse{}
	thingsResponse.Things = make([]*models.ThingGetResponse, limit)

	// Do a query to get all node-information
	// TODO: Only return Things and no actions
	req := dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{ 
		things(func: has(atContext), orderdesc: creationTimeUnix, first: %d, offset: %d)  {
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
	resultItems := nodes[0].Children

	// Set the return array length
	thingsResponse.Things = make([]*models.ThingGetResponse, len(resultItems))

	for i, node := range resultItems {
		thingResponse := &models.ThingGetResponse{}
		thingResponse.Schema = map[string]interface{}{}
		f.mergeThingNodeInResponse(node, thingResponse)
		thingsResponse.Things[i] = thingResponse
	}

	// Create query to count total results
	req = dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{ 
  		totalResults(func: eq(_type_, "%s"))  {
    		count()
  		}
	}`, connector_utils.RefTypeThing))

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

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *Dgraph) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {
	refThingNode, err := f.getNodeByUUID(UUID)

	if err != nil {
		return err
	}

	err = f.updateThingNodeEdges(refThingNode, thing)

	return err
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Dgraph) DeleteThing(UUID strfmt.UUID) error {
	// Create the query for removing query
	variables := make(map[string]string)
	variables["$uuid"] = string(UUID)

	req := dgraphClient.Req{}
	req.SetQueryWithVariables(`{
		var(func: eq(uuid, $uuid)) {
			node_to_delete as ~id
		}
		
		id_node_to_delete as var(func: eq(uuid, $uuid))
	}
	mutation {
		delete {
			uid(node_to_delete) * * .
			uid(id_node_to_delete) * * .
		}
	}`, variables)

	_, err := f.client.Run(f.getContext(), &req)

	if err != nil {
		return err
	}

	return nil
}

// AddAction adds an Action to the Dgraph database with the given UUID
func (f *Dgraph) AddAction(action *models.Action, UUID strfmt.UUID) error {
	// TODO: make type interactive
	newNode, err := f.addNewNode(
		connector_utils.RefTypeAction,
		action.AtContext,
		action.AtClass,
		action.CreationTimeUnix,
		action.LastUpdateTimeUnix,
		UUID,
	)

	if err != nil {
		return err
	}

	// Add all given information to the new node
	err = f.updateActionNodeEdges(newNode, action)

	if err != nil {
		return err
	}

	return nil
}

// GetAction returns an action from the database
func (f *Dgraph) GetAction(UUID strfmt.UUID) (models.ActionGetResponse, error) {
	// Initialize response
	actionResponse := models.ActionGetResponse{}
	actionResponse.Schema = map[string]models.JSONObject{}

	// Do a query to get all node-information based on the given UUID
	variables := make(map[string]string)
	variables["$uuid"] = string(UUID)

	req := dgraphClient.Req{}
	req.SetQueryWithVariables(`{
		get(func: eq(uuid, $uuid)) { 
			uuid
			~id {
				expand(_all_) {
					expand(_all_) {
						expand(_all_) 
					}
				}
				~action.of {
					id {
						uuid
					}
					type {
						class
					}
				}
			}
		}
	}`, variables)

	// Run query created above
	resp, err := f.client.Run(f.getContext(), &req)
	if err != nil {
		return actionResponse, err
	}

	// Merge the results into the model to return
	nodes := resp.GetN()
	for _, node := range nodes {
		f.mergeActionNodeInResponse(node, &actionResponse, "")
	}

	return actionResponse, nil
}

// ListActions lists actions for a specific thing
func (f *Dgraph) ListActions(UUID strfmt.UUID, limit int, page int) (models.ActionsListResponse, error) {
	// Initialize response
	actionsResponse := models.ActionsListResponse{}

	// Do a query to get all node-information
	req := dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{
		actions(func: eq(uuid, "%s")) {
			uuid
			~id {
				actions: ~action.target (orderdesc: creationTimeUnix) (first: %d, offset: %d) {
					expand(_all_) {
						expand(_all_)
					}
				}
			}
		}
	}`, UUID, limit, (page-1)*limit))

	// Run query created above
	resp, err := f.client.Run(f.getContext(), &req)
	if err != nil {
		return actionsResponse, err
	}

	// Merge the results into the model to return
	nodes := resp.GetN()
	resultItems := nodes[0].Children[0].Children[0].Children

	// Set the return array length
	actionsResponse.Actions = make([]*models.ActionGetResponse, len(resultItems))

	// Loop to add all items in the return object
	for i, node := range resultItems {
		actionResponse := &models.ActionGetResponse{}
		actionResponse.Schema = map[string]models.JSONObject{}
		// TODO: Add object and subject
		f.mergeActionNodeInResponse(node, actionResponse, "")
		actionsResponse.Actions[i] = actionResponse
	}

	// Create query to count total results
	// TODO: Combine the total results code with the code of the 'things
	req = dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{
		totalResults(func: eq(uuid, "%s")) {
			~id {
				~action.target {
					count()
				}
			}
		}
	}`, UUID))

	// Run query created above
	resp, err = f.client.Run(f.getContext(), &req)
	if err != nil {
		return actionsResponse, err
	}

	// Unmarshal the dgraph response into a struct
	var totalResult TotalResultsResult
	err = dgraphClient.Unmarshal(resp.N, &totalResult)
	if err != nil {
		return actionsResponse, nil
	}

	// Set the total results
	actionsResponse.TotalResults = totalResult.Root.Count // TODO: NOT WORKING, MISSING 'totalResults' IN RETURN OBJ, DGRAPH bug?

	return actionsResponse, nil
}

// UpdateAction updates a specific action
func (f *Dgraph) UpdateAction(action *models.Action, UUID strfmt.UUID) error {
	refActionNode, err := f.getNodeByUUID(UUID)

	if err != nil {
		return err
	}

	err = f.updateActionNodeEdges(refActionNode, action)

	return err
}

func (f *Dgraph) addNewNode(nType string, nodeContext string, nodeClass string, creationTimeUnix int64, lastUpdateTimeUnix int64, UUID strfmt.UUID) (dgraphClient.Node, error) {
	// TODO: Search for uuid edge/node before making new??

	// Create new one and connect it
	newNode, err := f.client.NodeBlank(fmt.Sprintf("%v", gouuid.NewV4()))

	if err != nil {
		return dgraphClient.Node{}, err
	}

	// Init the request
	req := dgraphClient.Req{}

	// Add UUID to node
	edge := newNode.Edge("uuid")
	if err = edge.SetValueString(string(UUID)); err != nil {
		return dgraphClient.Node{}, err
	}
	if err = req.Set(edge); err != nil {
		return dgraphClient.Node{}, err
	}

	// Add type (thing/key/action)
	edge = newNode.Edge(refTypePointer)
	if err = edge.SetValueString(nType); err != nil {
		return dgraphClient.Node{}, err
	}
	if err = req.Set(edge); err != nil {
		return dgraphClient.Node{}, err
	}

	// Add context and class to node
	edge = newNode.Edge("atContext")
	if err = edge.SetValueString(nodeContext); err != nil {
		return dgraphClient.Node{}, err
	}
	if err = req.Set(edge); err != nil {
		return dgraphClient.Node{}, err
	}

	edge = newNode.Edge("atClass")
	if err = edge.SetValueString(nodeClass); err != nil {
		return dgraphClient.Node{}, err
	}
	if err = req.Set(edge); err != nil {
		return dgraphClient.Node{}, err
	}

	// Add timing nodes
	edge = newNode.Edge("creationTimeUnix")
	if err = edge.SetValueInt(creationTimeUnix); err != nil {
		return dgraphClient.Node{}, err
	}
	if err = req.Set(edge); err != nil {
		return dgraphClient.Node{}, err
	}

	edge = newNode.Edge("lastUpdateTimeUnix")
	if err = edge.SetValueInt(lastUpdateTimeUnix); err != nil {
		return dgraphClient.Node{}, err
	}
	if err = req.Set(edge); err != nil {
		return dgraphClient.Node{}, err
	}

	// Call run after all mutations are added
	_, err = f.client.Run(f.getContext(), &req)

	return newNode, nil
}

// updateThingNodeEdges updates all the edges of the node, used with a new node or to update/patch a node
func (f *Dgraph) updateThingNodeEdges(node dgraphClient.Node, thing *models.Thing) error {
	// Create update request
	req := dgraphClient.Req{}

	// Init error var
	var err error

	// Add Thing properties
	for propKey, propValue := range thing.Schema.(map[string]interface{}) {
		err = f.addPropertyEdge(&req, node, propKey, propValue)
	}

	// Call run after all mutations are added
	_, err = f.client.Run(f.getContext(), &req)

	return err
}

// updateActionNodeEdges updates all the edges of the node, used with a new node or to update/patch a node
func (f *Dgraph) updateActionNodeEdges(node dgraphClient.Node, action *models.Action) error {
	// Create update request
	req := dgraphClient.Req{}

	// Init error var
	var err error

	// Add Action properties
	for propKey, propValue := range action.Schema.(map[string]interface{}) {
		err = f.addPropertyEdge(&req, node, propKey, propValue)
	}

	// Add Thing that gets the action
	// TODO: Use 'locationUrl' and 'type'
	objectNode, err := f.getNodeByUUID(strfmt.UUID(action.Things.Object.NrDollarCref))
	if err != nil {
		return err
	}
	err = f.connectRef(&req, node, "action.target", objectNode)
	if err != nil {
		return err
	}

	// Add subject Thing by $ref TODO: make interactive
	// TODO: Use 'locationUrl' and 'type'
	subjectNode, err := f.getNodeByUUID(strfmt.UUID(action.Things.Subject.NrDollarCref))
	if err != nil {
		return err
	}
	err = f.connectRef(&req, subjectNode, "action.of", node)
	if err != nil {
		return err
	}

	// Call run after all mutations are added
	_, err = f.client.Run(f.getContext(), &req)

	return err
}

func (f *Dgraph) addPropertyEdge(req *dgraphClient.Req, node dgraphClient.Node, propKey string, propValue interface{}) error {
	// Add prefix to the schema properties
	edgeName := schemaPrefix + propKey

	// Get the type of the given value
	typeVar := fmt.Sprintf("%T", propValue)

	// If it is an interface, then it should contain a "cref" reference to another object. Use it to connect nodes.
	if typeVar == "map[string]interface {}" {
		refProperties := propValue.(map[string]interface{})
		refThingNode, err := f.getNodeByUUID(strfmt.UUID(refProperties["$cref"].(string)))
		if err != nil {
			return err
		}
		err = f.connectRef(req, node, edgeName, refThingNode)
	} else {
		// Otherwise, the data should be added by type.
		edge := node.Edge(edgeName)
		if strings.Contains(typeVar, "bool") {
			if err := edge.SetValueBool(propValue.(bool)); err != nil {
				return err
			}
		} else if strings.Contains(typeVar, "int") {
			if err := edge.SetValueInt(propValue.(int64)); err != nil {
				return err
			}
		} else if strings.Contains(typeVar, "float") {
			if err := edge.SetValueFloat(propValue.(float64)); err != nil {
				return err
			}
		} else if strings.Contains(typeVar, "string") {
			if err := edge.SetValueString(propValue.(string)); err != nil {
				return err
			}
		} else {
			return errors_.New("given type can not be saved to the database")
		}

		// Set 'edge' specified above.
		if err := req.Set(edge); err != nil {
			return err
		}
	}

	return nil
}

// connectRef function to connect two nodes.
func (f *Dgraph) connectRef(req *dgraphClient.Req, nodeFrom dgraphClient.Node, edgeName string, nodeTo dgraphClient.Node) error {
	relatedEdge := nodeFrom.ConnectTo(edgeName, nodeTo)
	if err := req.Set(relatedEdge); err != nil {
		return err
	}
	return nil
}

// mergeThingNodeInResponse based on https://github.com/dgraph-io/dgraph/blob/release/v0.8.0/wiki/resources/examples/goclient/crawlerRDF/crawler.go#L250-L264
func (f *Dgraph) mergeThingNodeInResponse(node *protos.Node, thingResponse *models.ThingGetResponse) {
	attribute := node.Attribute

	printNode(0, node)
	if attribute == "things" {
		thingResponse.Schema = make(map[string]interface{})
		for _, prop := range node.GetProperties() {
			if prop.Prop == "creationTimeUnix" {
				thingResponse.CreationTimeUnix = prop.GetValue().GetIntVal()
			} else if prop.Prop == "lastUpdateTimeUnix" {
				thingResponse.LastUpdateTimeUnix = prop.GetValue().GetIntVal()
			} else if prop.Prop == "atContext" {
				thingResponse.AtContext = prop.GetValue().GetStrVal()
			} else if prop.Prop == "atClass" {
				thingResponse.AtClass = prop.GetValue().GetStrVal()
			} else if prop.Prop == "uuid" {
				thingResponse.ThingID = strfmt.UUID(prop.GetValue().GetStrVal())
			} else if strings.HasPrefix(prop.Prop, schemaPrefix) {
				propValueObj := prop.GetValue().GetVal()

				var propValue interface{}

				switch propType := fmt.Sprintf("%T", propValueObj); propType {
				case "*protos.Value_DefaultVal":
					propValue = prop.GetValue().GetDefaultVal()
				case "*protos.Value_StrVal":
					propValue = prop.GetValue().GetStrVal()
				case "*protos.Value_PasswordVal":
					propValue = prop.GetValue().GetPasswordVal()
				case "*protos.Value_IntVal":
					propValue = prop.GetValue().GetIntVal()
				case "*protos.Value_BoolVal":
					propValue = prop.GetValue().GetBoolVal()
				case "*protos.Value_DoubleVal":
					propValue = prop.GetValue().GetDoubleVal()
				case "*protos.Value_BytesVal":
					propValue = prop.GetValue().GetBytesVal()
				case "*protos.Value_GeoVal":
					propValue = prop.GetValue().GetGeoVal()
				case "*protos.Value_DateVal":
					propValue = prop.GetValue().GetDateVal()
				case "*protos.Value_DatetimeVal":
					propValue = prop.GetValue().GetDatetimeVal()
				case "*protos.Value_UidVal":
					propValue = prop.GetValue().GetUidVal()
				default:
					propValue = prop.GetValue().GetDefaultVal()
				}

				thingResponse.Schema.(map[string]interface{})[strings.TrimPrefix(prop.Prop, schemaPrefix)] = propValue
			}
		}
	} else if strings.HasPrefix(attribute, schemaPrefix) {
		crefObj := map[string]string{
			"location": "http://localhost/", // TODO, make relative in 2.0.0
		}
		for _, prop := range node.GetProperties() {
			if prop.Prop == "uuid" {
				crefObj["$cref"] = prop.GetValue().GetStrVal() // TODO, make key relative?
			} else if prop.Prop == refTypePointer {
				crefObj["type"] = prop.GetValue().GetStrVal() // TODO, make key relative?
			}
		}
		thingResponse.Schema.(map[string]interface{})[strings.TrimPrefix(attribute, schemaPrefix)] = crefObj
	}

	for _, child := range node.Children {
		f.mergeThingNodeInResponse(child, thingResponse)
	}

}

// mergeActionNodeInResponse based on https://github.com/dgraph-io/dgraph/blob/release/v0.8.0/wiki/resources/examples/goclient/crawlerRDF/crawler.go#L250-L264
func (f *Dgraph) mergeActionNodeInResponse(node *protos.Node, actionResponse *models.ActionGetResponse, parentAttribute string) {
	attribute := node.Attribute

	for _, prop := range node.GetProperties() {
		if attribute == "~id" || attribute == "actions" {
			if prop.Prop == "creationTimeUnix" {
				actionResponse.CreationTimeUnix = prop.GetValue().GetIntVal()
			} else if prop.Prop == "lastUpdateTimeUnix" {
				actionResponse.LastUpdateTimeUnix = prop.GetValue().GetIntVal()
			} else {
				// actionResponse.Schema[prop.Prop] = map[string]models.JSONValue{
				// 	"value": prop.GetValue().GetStrVal(),
				// }
			}
		} else if attribute == "type" && (parentAttribute == "~id" || parentAttribute == "actions") {
			if prop.Prop == "context" {
				// actionResponse.AtType = "Person" TODO: FIX?
			} else if prop.Prop == "class" {
				actionResponse.AtContext = prop.GetValue().GetStrVal()
			}
		} else if attribute == "id" && (parentAttribute == "~id" || parentAttribute == "actions") {
			if prop.Prop == "uuid" {
				actionResponse.ActionID = strfmt.UUID(prop.GetValue().GetStrVal())
			}
		} else if attribute == "type" && parentAttribute == "action.target" {
			if prop.Prop == "context" {
				// actionResponse.AtType = "Person" TODO: FIX?
			} else if prop.Prop == "class" {
				// actionResponse.ThingID = prop.GetValue().GetStrVal() // TODO: THING ID has to be fixed, it is a REF object
			}
		} else if attribute == "id" && parentAttribute == "action.target" {
			if prop.Prop == "uuid" {
				actionResponse.Things.Object.NrDollarCref = strfmt.UUID(prop.GetValue().GetStrVal())
			}
		} else if attribute == "type" && parentAttribute == "~action.of" {
			if prop.Prop == "context" {
				// actionResponse.AtType = "Person" TODO: FIX?
			} else if prop.Prop == "class" {
				// actionResponse.Subject = prop.GetValue().GetStrVal() // TODO: Subject ID has to be fixed
			}
		} else if attribute == "id" && parentAttribute == "~action.of" {
			if prop.Prop == "uuid" {
				// actionResponse.Subject = strfmt.UUID(prop.GetValue().GetStrVal())
			}
		}
	}

	for _, child := range node.Children {
		f.mergeActionNodeInResponse(child, actionResponse, attribute)
	}

}

func (f *Dgraph) getNodeByUUID(UUID strfmt.UUID) (dgraphClient.Node, error) {
	// Search for the class to make the connection, create variables
	variables := make(map[string]string)
	variables["$uuid"] = string(UUID)

	// Create the query for existing class
	req := dgraphClient.Req{}
	req.SetQueryWithVariables(`{ 
		node(func: eq(uuid, $uuid)) {
			uuid
			_uid_
		}
	}`, variables)

	// Run the query
	resp, err := f.client.Run(f.getContext(), &req)

	if err != nil {
		return dgraphClient.Node{}, err
	}

	// Unmarshal the result
	var idResult NodeIDResult
	err = dgraphClient.Unmarshal(resp.N, &idResult)

	if err != nil {
		return dgraphClient.Node{}, err
	}

	// Create the classNode from the result
	node := f.client.NodeUid(idResult.Root.ID)

	return node, err
}

func (f *Dgraph) getContext() context.Context {
	return context.Background()
}

// TODO REMOVE, JUST FOR TEST
func printNode(depth int, node *protos.Node) {

	fmt.Println(strings.Repeat(" ", depth), "Atrribute : ", node.Attribute)

	// the values at this level
	for _, prop := range node.GetProperties() {
		fmt.Println(strings.Repeat(" ", depth), "Prop : ", prop.Prop, " Value : ", prop.Value, " Type : %T", prop.Value)
	}

	for _, child := range node.Children {
		fmt.Println(strings.Repeat(" ", depth), "+")
		printNode(depth+1, child)
	}

}
