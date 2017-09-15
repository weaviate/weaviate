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
	errors_ "errors"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"io/ioutil"
	"log"
	"math"
	"strings"

	"github.com/go-openapi/strfmt"
	"google.golang.org/grpc"

	dgraphClient "github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos"
	"github.com/dgraph-io/dgraph/types"
	gouuid "github.com/satori/go.uuid"

	"github.com/weaviate/weaviate/config"
	"github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/models"
	"github.com/weaviate/weaviate/schema"
)

// Dgraph has some basic variables.
type Dgraph struct {
	client *dgraphClient.Dgraph
	kind   string

	config Config
}

// Config represents the config outline for Dgraph. The Database config shoud be of the following form:
// "database_config" : {
//     "host": "127.0.0.1",
//     "port": 9080
// }
// Notice that the port is the GRPC-port.
type Config struct {
	Host string
	Port int
}

const refTypePointer string = "_type_"
const edgeNameKey string = "key"
const edgeNameKeyParent string = "key.parent"
const schemaPrefix string = "schema."
const ipOriginDelimiter string = ";"

var (
	defaultSingleNodeQueryTemplate = `{ 
		get(func: eq(uuid, "%s")) @filter(eq(%s, "%s")) {
			expand(_all_) {
				expand(_all_)
			}
		}
	}`

	keyChildrenQueryTemplate = `{ 
		get(func: eq(uuid, "%s")) @filter(eq(` + refTypePointer + `, "` + connutils.RefTypeKey + `")) {
			children: ~` + edgeNameKeyParent + ` {
				uuid: uuid
			}
		}
	}`
)

// GetName returns a unique connector name
func (f *Dgraph) GetName() string {
	return "dgraph"
}

// SetConfig is used to fill in a struct with config variables
func (f *Dgraph) SetConfig(configInput *config.Environment) error {
	err := mapstructure.Decode(configInput.Database.DatabaseConfig, &f.config)

	if err != nil || len(f.config.Host) == 0 || f.config.Port == 0 {
		return errors_.New("could not get Dgraph host/port from config")
	}

	return nil
}

// SetSchema is used to fill in a struct with schema
func (f *Dgraph) SetSchema(schemaInput *schema.WeaviateSchema) error {
	// TODO: not implemented yet

	return nil
}

// Connect creates connection and tables if not already available
func (f *Dgraph) Connect() error {
	// Connect with Dgraph host, create connection dail
	dgraphGrpcAddress := fmt.Sprintf("%s:%d", f.config.Host, f.config.Port)

	conn, err := grpc.Dial(dgraphGrpcAddress, grpc.WithInsecure())
	if err != nil {
		return errors_.New("error while connecting to the database")
	}
	// defer conn.Close()

	// Create temp-folder for caching
	dir, err := ioutil.TempDir("temp", "weaviate_dgraph")

	if err != nil {
		return errors_.New("error while creating temp directory")
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
	var err error

	// Init flush variable
	flushIt := false

	// Add schema to database TODO, needed for reversing nodes?
	// for _, class := range cfv.schema.Classes {
	// 	// for _, prop := range class.Properties {
	// 	for _ = range class.Properties {
	// 		// Add Dgraph-schema for every property of individual nodes
	// 		// err = f.client.AddSchema(protos.SchemaUpdate{
	// 		// 	Predicate: "schema." + prop.Name,
	// 		// 	ValueType: uint32(types.UidID),
	// 		// 	Directive: protos.SchemaUpdate_REVERSE,
	// 		// })

	// 		// if err != nil {
	// 		// 	return err
	// 		// }

	// 		// TODO: Add specific schema for datatypes
	// 		// http://schema.org/DataType
	// 	}
	// }

	// Add class schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "atClass",
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding 'atClass' Dgraph-schema")
	}

	// Add context schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "atContext",
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding 'atContext' Dgraph-schema")
	}

	// Add UUID schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "uuid",
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding 'uuid' Dgraph-schema")
	}

	// Add refTypePointer schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: refTypePointer,
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding '" + refTypePointer + "' Dgraph-schema")
	}

	// Add key connection schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: edgeNameKey,
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding '" + edgeNameKey + "' Dgraph-schema")
	}

	// Add 'things.subject' schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "things.subject",
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding 'things.subject' Dgraph-schema")
	}

	// Add 'things.object' schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "things.object",
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding 'things.object' Dgraph-schema")
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
			return errors_.New("error while adding '" + ms + "' Dgraph-schema")
		}
	}

	// KEYS
	// Add index for keys
	// if err := f.client.AddSchema(protos.SchemaUpdate{
	// 	Predicate: edgeNameKeyChild,
	// 	ValueType: uint32(types.UidID),
	// 	Directive: protos.SchemaUpdate_REVERSE,
	// 	Count:     true,
	// }); err != nil {
	// 	return errors_.New("error while adding '" + edgeNameKeyChild + "' Dgraph-schema")
	// }

	// Add index for keys parent
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: edgeNameKeyParent,
		ValueType: uint32(types.UidID),
		Directive: protos.SchemaUpdate_REVERSE,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding '" + edgeNameKeyParent + "' Dgraph-schema")
	}

	// Add key for searching root
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "key.root",
		ValueType: uint32(types.BoolID),
		Tokenizer: []string{"bool"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding 'key.root' Dgraph-schema")
	}

	// Add token schema in Dgraph
	if err := f.client.AddSchema(protos.SchemaUpdate{
		Predicate: "key.token",
		ValueType: uint32(types.StringID),
		Tokenizer: []string{"exact", "term"},
		Directive: protos.SchemaUpdate_INDEX,
		Count:     true,
	}); err != nil {
		return errors_.New("error while adding 'key.token' Dgraph-schema")
	}

	// Call flush to flush buffers after all mutations are added
	if flushIt || true {
		err = f.client.BatchFlush()

		if err != nil {
			return errors_.New("error while performing BatchFlush()")
		}
	}

	// Add ROOT-key if not exists
	// Search for Root key
	req := dgraphClient.Req{}
	req.SetQuery(`{
		totalResults(func: eq(key.root, true))  {
			count()
		}
	}`)

	// Run query created above
	var resp *protos.Response
	if resp, err = f.client.Run(f.getContext(), &req); err != nil {
		return err
	}

	// Unmarshal the dgraph response into a struct
	var totalResult TotalResultsResult
	if err = dgraphClient.Unmarshal(resp.N, &totalResult); err != nil {
		return err
	}

	// Set the total results
	if totalResult.Root.Count == 0 {
		log.Println("No root-key found.")

		// Create new object and fill it
		keyObject := models.Key{}
		token := connutils.CreateFirstUserObject(&keyObject)

		err = f.AddKey(&keyObject, connutils.GenerateUUID(), token)

		if err != nil {
			return err
		}
	}
	// END KEYS

	return nil
}

// AddThing adds a thing to the Dgraph database with the given UUID
func (f *Dgraph) AddThing(thing *models.Thing, UUID strfmt.UUID, keyUUID strfmt.UUID) error {
	// Create new node with base vars
	newNode, err := f.addNewNode(
		connutils.RefTypeThing,
		UUID,
	)

	if err != nil {
		return err
	}

	// Connect the Key to the Thing
	err = f.connectKey(newNode, keyUUID)

	// Add first level properties to node
	newNode, err = f.addNodeFirstLevelProperties(
		thing.AtContext,
		thing.AtClass,
		thing.CreationTimeUnix,
		thing.LastUpdateTimeUnix,
		newNode,
	)

	if err != nil {
		return err
	}

	// Add all given schema-information to the new node
	err = f.updateNodeSchemaProperties(newNode, thing.Schema)

	return err

	// TODO: Reset batch before and flush after every function??
}

// GetThing returns the thing in the ThingGetResponse format
func (f *Dgraph) GetThing(UUID strfmt.UUID) (models.ThingGetResponse, error) {
	// Initialize response
	thingResponse := models.ThingGetResponse{}
	thingResponse.Schema = map[string]models.JSONObject{}

	// Get raw node for response
	rawNode, err := f.getRawNodeByUUID(UUID, connutils.RefTypeThing, defaultSingleNodeQueryTemplate)
	if err != nil {
		return thingResponse, err
	}

	// Merge the results into the model to return
	f.mergeThingNodeInResponse(rawNode, &thingResponse)

	return thingResponse, nil
}

// ListThings returns the thing in the ThingGetResponse format
func (f *Dgraph) ListThings(limit int, page int) (models.ThingsListResponse, error) {
	// Initialize response
	thingsResponse := models.ThingsListResponse{}

	// Do a query to get all node-information
	req := dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{ 
		things(func: eq(%s, %s), orderdesc: creationTimeUnix, first: %d, offset: %d)  {
			expand(_all_) {
				expand(_all_)
			}
		}
	}
	`, refTypePointer, connutils.RefTypeThing, limit, (page-1)*limit))

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
	}`, connutils.RefTypeThing))

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
	// Get the thing-node from the database
	updateNode, err := f.getNodeByUUID(UUID)

	if err != nil {
		return err
	}

	// Update first level properties to node
	updateNode, err = f.addNodeFirstLevelProperties(
		thing.AtContext,
		thing.AtClass,
		thing.CreationTimeUnix,
		thing.LastUpdateTimeUnix,
		updateNode,
	)

	if err != nil {
		return err
	}

	// Update in DB
	err = f.updateNodeSchemaProperties(updateNode, thing.Schema)

	return err
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Dgraph) DeleteThing(UUID strfmt.UUID) error {
	// Call function for deleting node
	err := f.deleteNodeByUUID(UUID)
	return err
}

// AddAction adds an Action to the Dgraph database with the given UUID
func (f *Dgraph) AddAction(action *models.Action, UUID strfmt.UUID, keyUUID strfmt.UUID) error {
	// Add new node
	newNode, err := f.addNewNode(
		connutils.RefTypeAction,
		UUID,
	)

	if err != nil {
		return err
	}

	// Connect the Key to the Action
	err = f.connectKey(newNode, keyUUID)

	if err != nil {
		return err
	}

	// Add first level properties to node
	newNode, err = f.addNodeFirstLevelProperties(
		action.AtContext,
		action.AtClass,
		action.CreationTimeUnix,
		action.LastUpdateTimeUnix,
		newNode,
	)

	if err != nil {
		return err
	}

	// Add all given schema-information to the new node
	err = f.updateNodeSchemaProperties(newNode, action.Schema)

	if err != nil {
		return err
	}

	// Add given thing-object and thing-subject
	err = f.updateActionRelatedThings(newNode, action)

	return err
}

// GetAction returns an action from the database
func (f *Dgraph) GetAction(UUID strfmt.UUID) (models.ActionGetResponse, error) {
	// Initialize response
	actionResponse := models.ActionGetResponse{}
	actionResponse.Schema = map[string]models.JSONObject{}
	actionResponse.Things = &models.ObjectSubject{}

	// Get raw node for response
	rawNode, err := f.getRawNodeByUUID(UUID, connutils.RefTypeAction, defaultSingleNodeQueryTemplate)
	if err != nil {
		return actionResponse, err
	}

	// Merge the results into the model to return
	f.mergeActionNodeInResponse(rawNode, &actionResponse, "")

	return actionResponse, nil
}

// ListActions lists actions for a specific thing
func (f *Dgraph) ListActions(UUID strfmt.UUID, limit int, page int) (models.ActionsListResponse, error) {
	// Initialize response
	actionsResponse := models.ActionsListResponse{}

	// Do a query to get all node-information
	req := dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{
		things(func: eq(uuid, "%s")) {
      		actions: ~things.object (orderdesc: creationTimeUnix) (first: %d, offset: %d) {
      			expand(_all_) {
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

	// No nodes = not found error. First level is root (always exists) so check children.
	if len(nodes[0].GetChildren()) == 0 {
		return actionsResponse, errors_.New("No actions found in database.")
	}

	// Get subitems because we use a query with related actions of a thing
	resultItems := nodes[0].Children[0].Children

	// Set the return array length
	actionsResponse.Actions = make([]*models.ActionGetResponse, len(resultItems))

	// Loop to add all items in the return object
	for i, node := range resultItems {
		actionResponse := &models.ActionGetResponse{}
		actionResponse.Schema = map[string]models.JSONObject{}
		actionResponse.Things = &models.ObjectSubject{}
		f.mergeActionNodeInResponse(node, actionResponse, "")
		actionsResponse.Actions[i] = actionResponse
	}

	// Create query to count total results
	// TODO: Combine the total results code with the code of the 'things' if possible
	req = dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{
		totalResults(func: eq(uuid, "%s")) {
			related: ~things.object {
				count()
			}
		}
	}`, UUID))

	// Run query created above
	resp, err = f.client.Run(f.getContext(), &req)
	if err != nil {
		return actionsResponse, err
	}

	// Unmarshal the dgraph response into a struct
	var totalResult TotalResultsRelatedResult
	err = dgraphClient.Unmarshal(resp.N, &totalResult)
	if err != nil {
		return actionsResponse, nil
	}

	// Set the total results
	actionsResponse.TotalResults = totalResult.Root.Related.Count
	// TODO: NOT WORKING WITH @NORMALIZE, 1 level deeper now, MISSING 'totalResults' IN RETURN OBJ, DGRAPH bug?

	return actionsResponse, nil
}

// UpdateAction updates a specific action
func (f *Dgraph) UpdateAction(action *models.Action, UUID strfmt.UUID) error {
	// Get the thing-node from the database
	updateNode, err := f.getNodeByUUID(UUID)

	if err != nil {
		return err
	}

	// Update first level properties to node
	updateNode, err = f.addNodeFirstLevelProperties(
		action.AtContext,
		action.AtClass,
		action.CreationTimeUnix,
		action.LastUpdateTimeUnix,
		updateNode,
	)

	if err != nil {
		return err
	}

	// Update in DB
	err = f.updateNodeSchemaProperties(updateNode, action.Schema)

	return err
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *Dgraph) DeleteAction(UUID strfmt.UUID) error {
	// Call function for deleting node
	err := f.deleteNodeByUUID(UUID)
	return err
}

// AddKey adds a key to the Dgraph database with the given UUID
func (f *Dgraph) AddKey(key *models.Key, UUID strfmt.UUID, token strfmt.UUID) error {
	// Create new node with base vars
	newNode, err := f.addNewNode(
		connutils.RefTypeKey,
		UUID,
	)

	// Init the request
	req := dgraphClient.Req{}

	// Add root value to edge
	isRoot := (key.Parent == nil)
	edge := newNode.Edge("key.root")
	if err := edge.SetValueBool(isRoot); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add delete-rights value to edge
	edge = newNode.Edge("key.delete")
	if err := edge.SetValueBool(key.Delete); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add execute-rights value to edge
	edge = newNode.Edge("key.execute")
	if err := edge.SetValueBool(key.Execute); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add read-rights value to edge
	edge = newNode.Edge("key.read")
	if err := edge.SetValueBool(key.Read); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add write-rights value to edge
	edge = newNode.Edge("key.write")
	if err := edge.SetValueBool(key.Write); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add email value to edge
	edge = newNode.Edge("key.email")
	if err := edge.SetValueString(key.Email); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add token value to edge
	edge = newNode.Edge("key.token")
	if err := edge.SetValueString(string(token)); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add expiry value to edge
	edge = newNode.Edge("key.expiry")
	if err := edge.SetValueInt(key.KeyExpiresUnix); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Add ip-origin value to edge
	edge = newNode.Edge("key.iporigin")
	if err := edge.SetValueString(strings.Join(key.IPOrigin, ipOriginDelimiter)); err != nil {
		return err
	}
	if err = req.Set(edge); err != nil {
		return err
	}

	// Set parent node, if node is not root
	if !isRoot {
		// Get Parent Node
		parentNode, err := f.getNodeByUUID(strfmt.UUID(key.Parent.NrDollarCref))
		if err != nil {
			return err
		}

		// Connect parent node as parent
		err = f.connectRef(&req, newNode, edgeNameKeyParent, parentNode)
		if err != nil {
			return err
		}

		// // Connect child node as child to parent
		// err = f.connectRef(&req, parentNode, edgeNameKeyChild, newNode)
		// if err != nil {
		// 	return err
		// }
	}

	// Call run after all mutations are added
	if _, err = f.client.Run(f.getContext(), &req); err != nil {
		return err
	}

	return err
	// TODO: Reset batch before and flush after every function??
}

// ValidateToken adds a key to the Dgraph database with the given UUID
func (f *Dgraph) ValidateToken(UUID strfmt.UUID) (models.KeyTokenGetResponse, error) {
	// Create key
	key := models.KeyTokenGetResponse{}

	// Search for Root key
	req := dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(`{
		get(func: eq(key.token, "%s"))  {
			expand(_all_)
		}
	}`, UUID))

	// Run query created above
	var err error
	var resp *protos.Response
	if resp, err = f.client.Run(f.getContext(), &req); err != nil {
		return key, err
	}

	// Unmarshal the dgraph response into a struct
	nodes := resp.GetN()

	// No nodes = not found error. First level is root (always exists) so check children.
	if len(nodes[0].GetChildren()) == 0 {
		return key, errors_.New("Key not found in database.")
	}

	// Merge the results into the model to return
	f.mergeKeyNodeInResponse(nodes[0], &key)

	return key, nil
}

// GetKey returns the key in the KeyGetResponse format
func (f *Dgraph) GetKey(UUID strfmt.UUID) (models.KeyTokenGetResponse, error) {
	// Initialize response
	keyResponse := models.KeyTokenGetResponse{}

	// Get raw node for response
	rawNode, err := f.getRawNodeByUUID(UUID, connutils.RefTypeKey, defaultSingleNodeQueryTemplate)
	if err != nil {
		return keyResponse, err
	}

	// Merge the results into the model to return
	f.mergeKeyNodeInResponse(rawNode, &keyResponse)

	return keyResponse, nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Dgraph) DeleteKey(UUID strfmt.UUID) error {
	// Call function for deleting node
	err := f.deleteNodeByUUID(UUID)
	return err
}

// GetKeyChildrenUUIDs returns the key in the an array format
func (f *Dgraph) GetKeyChildrenUUIDs(UUID strfmt.UUID) ([]strfmt.UUID, error) {
	// Create the response object
	response := []strfmt.UUID{}

	// Init the request
	req := dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(keyChildrenQueryTemplate, string(UUID)))

	// Run query created above
	var resp *protos.Response
	var err error
	if resp, err = f.client.Run(f.getContext(), &req); err != nil {
		return response, err
	}

	// Unmarshal the dgraph response into a struct
	var childrenResult KeyChildrenResult
	if err = dgraphClient.Unmarshal(resp.N, &childrenResult); err != nil {
		return response, err
	}

	// Convert the query result to function response
	for _, v := range childrenResult.Root.Children {
		response = append(response, v.UUID)
	}

	return response, nil
}

func (f *Dgraph) addNewNode(nType string, UUID strfmt.UUID) (dgraphClient.Node, error) {
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
		return newNode, err
	}
	if err = req.Set(edge); err != nil {
		return newNode, err
	}

	// Add type (thing/key/action)
	edge = newNode.Edge(refTypePointer)
	if err = edge.SetValueString(nType); err != nil {
		return newNode, err
	}
	if err = req.Set(edge); err != nil {
		return newNode, err
	}

	// Call run after all mutations are added
	if _, err = f.client.Run(f.getContext(), &req); err != nil {
		return newNode, err
	}

	return newNode, nil
}

func (f *Dgraph) addNodeFirstLevelProperties(nodeContext string, nodeClass string, creationTimeUnix int64, lastUpdateTimeUnix int64, newNode dgraphClient.Node) (dgraphClient.Node, error) {
	// Init the request
	req := dgraphClient.Req{}

	var err error

	// Add context and class to node
	edge := newNode.Edge("atContext")
	if err := edge.SetValueString(nodeContext); err != nil {
		return newNode, err
	}
	if err = req.Set(edge); err != nil {
		return newNode, err
	}

	edge = newNode.Edge("atClass")
	if err = edge.SetValueString(nodeClass); err != nil {
		return newNode, err
	}
	if err = req.Set(edge); err != nil {
		return newNode, err
	}

	// Add timing nodes
	edge = newNode.Edge("creationTimeUnix")
	if err = edge.SetValueInt(creationTimeUnix); err != nil {
		return newNode, err
	}
	if err = req.Set(edge); err != nil {
		return newNode, err
	}

	edge = newNode.Edge("lastUpdateTimeUnix")
	if err = edge.SetValueInt(lastUpdateTimeUnix); err != nil {
		return newNode, err
	}
	if err = req.Set(edge); err != nil {
		return newNode, err
	}

	// Call run after all mutations are added
	if _, err = f.client.Run(f.getContext(), &req); err != nil {
		return newNode, err
	}

	return newNode, nil
}

// updateNodeSchemaProperties updates all the edges of the node in 'schema', used with a new node or to update/patch a node
func (f *Dgraph) updateNodeSchemaProperties(node dgraphClient.Node, nodeSchema models.Schema) error {
	// Create update request
	req := dgraphClient.Req{}

	// Init error var
	var err error

	// Add Thing properties
	for propKey, propValue := range nodeSchema.(map[string]interface{}) {
		err = f.addPropertyEdge(&req, node, propKey, propValue)
	}

	// Call run after all mutations are added
	_, err = f.client.Run(f.getContext(), &req)

	return err
}

// updateActionRelatedThings updates all the edges of the node, used with a new node or to update/patch a node
func (f *Dgraph) updateActionRelatedThings(node dgraphClient.Node, action *models.Action) error {
	// Create update request
	req := dgraphClient.Req{}

	// Init error var
	var err error

	// Add Thing that gets the action
	// TODO: Use 'locationUrl' and 'type'
	objectNode, err := f.getNodeByUUID(strfmt.UUID(action.Things.Object.NrDollarCref))
	if err != nil {
		return err
	}
	err = f.connectRef(&req, node, "things.object", objectNode)
	if err != nil {
		return err
	}

	// Add subject Thing by $ref TODO: make interactive
	// TODO: Use 'locationUrl' and 'type'
	subjectNode, err := f.getNodeByUUID(strfmt.UUID(action.Things.Subject.NrDollarCref))
	if err != nil {
		return err
	}
	err = f.connectRef(&req, node, "things.subject", subjectNode)
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

// connectKey function to connect two nodes.
func (f *Dgraph) connectKey(nodeFrom dgraphClient.Node, keyUUID strfmt.UUID) error {
	// Create update request
	req := dgraphClient.Req{}

	// Find the
	keyNode, err := f.getNodeByUUID(keyUUID)
	if err != nil {
		return err
	}

	err = f.connectRef(&req, nodeFrom, edgeNameKey, keyNode)

	if err != nil {
		return err
	}

	// Call run after all mutations are added
	_, err = f.client.Run(f.getContext(), &req)

	return err
}

// mergeThingNodeInResponse based on https://github.com/dgraph-io/dgraph/blob/release/v0.8.0/wiki/resources/examples/goclient/crawlerRDF/crawler.go#L250-L264
func (f *Dgraph) mergeThingNodeInResponse(node *protos.Node, thingResponse *models.ThingGetResponse) {
	// Get node attribute, this is the name of the parent node.
	attribute := node.Attribute

	// Depending on the given function name in the query or depth in response, switch on the attribute.
	if attribute == "things" || attribute == "get" {
		// Initiate thing response schema
		thingResponse.Schema = make(map[string]interface{})

		// For all properties, fill them.
		for _, prop := range node.GetProperties() {
			// Fill basic properties of each thing.
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
				// Fill all the properties starting with 'schema.'
				// That are the properties that are specific for a class
				propValue := f.getPropValue(prop)

				// Add the 'schema.' value to the response.
				thingResponse.Schema.(map[string]interface{})[strings.TrimPrefix(prop.Prop, schemaPrefix)] = propValue
			}
		}
	} else if strings.HasPrefix(attribute, schemaPrefix) {
		// When the attribute has 'schema.' in it, it is 1 level deeper.
		crefObj := f.createCrefObject(node)

		// Add the 'cref'-node into the response.
		thingResponse.Schema.(map[string]interface{})[strings.TrimPrefix(attribute, schemaPrefix)] = crefObj
	} else if attribute == edgeNameKey {
		// When the attribute is 'key', add the reference object
		thingResponse.Key = f.createCrefObject(node)
	}

	// Go level deeper to find cref nodes.
	for _, child := range node.Children {
		f.mergeThingNodeInResponse(child, thingResponse)
	}

}

func (f *Dgraph) getPropValue(prop *protos.Property) interface{} {
	// Get the propvalye object
	propValueObj := prop.GetValue().GetVal()

	var propValue interface{}

	// Switch on every possible type in the proptype
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

	return propValue
}

func (f *Dgraph) createCrefObject(node *protos.Node) *models.SingleRef {
	// Create the 'cref'-node for the response.
	url := "http://localhost/" // TODO, make relative in 2.0.0
	crefObj := models.SingleRef{
		LocationURL: &url,
	}

	// Loop through the given node properties to generate response object
	for _, prop := range node.GetProperties() {
		if prop.Prop == "uuid" {
			crefObj.NrDollarCref = strfmt.UUID(prop.GetValue().GetStrVal()) // TODO, make key relative?
		} else if prop.Prop == refTypePointer {
			crefObj.Type = prop.GetValue().GetStrVal() // TODO, make key relative?
		}
	}

	return &crefObj
}

// mergeActionNodeInResponse based on https://github.com/dgraph-io/dgraph/blob/release/v0.8.0/wiki/resources/examples/goclient/crawlerRDF/crawler.go#L250-L264
func (f *Dgraph) mergeActionNodeInResponse(node *protos.Node, actionResponse *models.ActionGetResponse, parentAttribute string) {
	// Get node attribute, this is the name of the parent node.
	attribute := node.Attribute

	// Depending on the given function name in the query or depth in response, switch on the attribute.
	if attribute == "actions" || attribute == "get" {
		// Initiate thing response schema
		actionResponse.Schema = make(map[string]interface{})

		// For all properties, fill them.
		for _, prop := range node.GetProperties() {
			// Fill basic properties of each thing.
			if prop.Prop == "creationTimeUnix" {
				actionResponse.CreationTimeUnix = prop.GetValue().GetIntVal()
			} else if prop.Prop == "lastUpdateTimeUnix" {
				actionResponse.LastUpdateTimeUnix = prop.GetValue().GetIntVal()
			} else if prop.Prop == "atContext" {
				actionResponse.AtContext = prop.GetValue().GetStrVal()
			} else if prop.Prop == "atClass" {
				actionResponse.AtClass = prop.GetValue().GetStrVal()
			} else if prop.Prop == "uuid" {
				actionResponse.ActionID = strfmt.UUID(prop.GetValue().GetStrVal())
			} else if strings.HasPrefix(prop.Prop, schemaPrefix) {
				// Fill all the properties starting with 'schema.'
				// That are the properties that are specific for a class
				propValue := f.getPropValue(prop)

				// Add the 'schema.' value to the response.
				actionResponse.Schema.(map[string]interface{})[strings.TrimPrefix(prop.Prop, schemaPrefix)] = propValue
			}
		}
	} else if strings.HasPrefix(attribute, schemaPrefix) {
		// When the attribute has 'schema.' in it, it is 1 level deeper.
		crefObj := f.createCrefObject(node)

		// Add the 'cref'-node into the response.
		actionResponse.Schema.(map[string]interface{})[strings.TrimPrefix(attribute, schemaPrefix)] = *crefObj
	} else if attribute == "things.subject" {
		// Add the 'cref'-node into the response.
		actionResponse.Things.Subject = f.createCrefObject(node)
	} else if attribute == "things.object" {
		// Add the 'cref'-node into the response.
		actionResponse.Things.Object = f.createCrefObject(node)
	} else if attribute == edgeNameKey {
		// When the attribute is 'key', add the reference object
		actionResponse.Key = f.createCrefObject(node)
	}

	// Go level deeper to find cref nodes.
	for _, child := range node.Children {
		f.mergeActionNodeInResponse(child, actionResponse, attribute)
	}
}

func (f *Dgraph) getRawNodeByUUID(UUID strfmt.UUID, typeName string, queryTemplate string) (*protos.Node, error) {
	// Search for the class to make the connection, create variables
	variables := make(map[string]string)
	variables["$uuid"] = string(UUID)

	// Create the query for existing class
	req := dgraphClient.Req{}
	req.SetQuery(fmt.Sprintf(queryTemplate, string(UUID), refTypePointer, typeName))

	// Run the query
	resp, err := f.client.Run(f.getContext(), &req)

	if err != nil {
		return &protos.Node{}, err
	}

	nodes := resp.N

	// No nodes = not found error. First level is root (always exists) so check children.
	if len(nodes[0].GetChildren()) == 0 {
		return &protos.Node{}, errors_.New("'" + typeName + "' not found in database.")
	}

	return nodes[0], err
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

func (f *Dgraph) deleteNodeByUUID(UUID strfmt.UUID) error {
	// Create the query for removing query
	variables := make(map[string]string)
	variables["$uuid"] = string(UUID)

	req := dgraphClient.Req{}
	req.SetQueryWithVariables(`{
		id_node_to_delete as var(func: eq(uuid, $uuid))
	}
	mutation {
		delete {
			uid(id_node_to_delete) * * .
		}
	}`, variables)

	_, err := f.client.Run(f.getContext(), &req)

	if err != nil {
		return err
	}

	return nil
}

func (f *Dgraph) getContext() context.Context {
	return context.Background()
}

// mergeKeyNodeInResponse based on https://github.com/dgraph-io/dgraph/blob/release/v0.8.0/wiki/resources/examples/goclient/crawlerRDF/crawler.go#L250-L264
func (f *Dgraph) mergeKeyNodeInResponse(node *protos.Node, key *models.KeyTokenGetResponse) {
	// Get node attribute, this is the name of the parent node.
	attribute := node.Attribute

	// Depending on the given function name in the query or depth in response, switch on the attribute.
	if attribute == "get" {
		// For all properties, fill them.
		for _, prop := range node.GetProperties() {
			// Fill basic properties of each thing.
			if prop.Prop == "key.delete" {
				key.Delete = prop.GetValue().GetBoolVal()
			} else if prop.Prop == "key.email" {
				key.Email = prop.GetValue().GetStrVal()
			} else if prop.Prop == "key.execute" {
				key.Execute = prop.GetValue().GetBoolVal()
			} else if prop.Prop == "key.expiry" {
				key.KeyExpiresUnix = prop.GetValue().GetIntVal()
			} else if prop.Prop == "key.iporigin" {
				key.IPOrigin = strings.Split(prop.GetValue().GetStrVal(), ipOriginDelimiter)
			} else if prop.Prop == "key.read" {
				key.Read = prop.GetValue().GetBoolVal()
			} else if prop.Prop == "key.token" {
				key.Token = strfmt.UUID(prop.GetValue().GetStrVal())
			} else if prop.Prop == "key.write" {
				key.Write = prop.GetValue().GetBoolVal()
			} else if prop.Prop == "uuid" {
				key.KeyID = strfmt.UUID(prop.GetValue().GetStrVal())
			}
		}
	} else if attribute == edgeNameKeyParent {
		// When the attribute is 'key', add the reference object
		key.Parent = f.createCrefObject(node)
	}

	// Go level deeper to find cref nodes.
	for _, child := range node.Children {
		f.mergeKeyNodeInResponse(child, key)
	}

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
