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

package inmemory

import (
	"context"
	errors_ "errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/go-openapi/strfmt"
	"github.com/mitchellh/mapstructure"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"

	"sort"
)

// inmemory has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type InMemory struct {
	schema    *schema.WeaviateSchema
	messaging *messages.Messaging

	keys         map[strfmt.UUID]models.Key
	key_tokens   map[strfmt.UUID]string
	key_children map[strfmt.UUID][]strfmt.UUID
	actions      map[strfmt.UUID]models.Action
	things       map[strfmt.UUID]models.Thing

	Config InMemoryConfig
}

type InMemoryConfig struct {
	RootKey   string `json:"root_key"`
	RootToken string `json:"root_token"`
}

// GetName returns a unique connector name, this name is used to define the connector in the weaviate config
func (f *InMemory) GetName() string {
	return "in-memory"
}

func (f *InMemory) SetConfig(configInput *config.Environment) error {
	err := mapstructure.Decode(configInput.Database.DatabaseConfig, &f.Config)
	if err != nil {
		return fmt.Errorf("Could not configure in-memory connector; %v", err)
	}

	if f.Config.RootKey == "" || f.Config.RootToken == "" {
		f.messaging.InfoMessage("No RootKey or RootToken provided; will generate random tokes")
	}

	// We ignore configuration for the in-memory connector.
	return nil
}

// SetSchema takes actionSchema and thingsSchema as an input and makes them available globally at f.schema
// In case you want to modify the schema, this is the place to do so.
// Note: When this function is called, the schemas (action + things) are already validated, so you don't have to build the validation.
func (f *InMemory) SetSchema(schemaInput *schema.WeaviateSchema) error {
	f.schema = schemaInput

	// If success return nil, otherwise return the error
	return nil
}

// SetMessaging is used to send messages to the service.
// Available message types are: f.messaging.Infomessage ...DebugMessage ...ErrorMessage ...ExitError (also exits the service) ...InfoMessage
func (f *InMemory) SetMessaging(m *messages.Messaging) error {

	// mandatory, adds the message functions to f.messaging to make them globally accessible.
	f.messaging = m

	// If success return nil, otherwise return the error
	return nil
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
// Does not return anything
func (f *InMemory) SetServerAddress(addr string) {
	// no-op; don't care about the server address.
}

// Connect creates a connection to the database and tables if not already available.
// The connections could not be closed because it is used more often.
func (f *InMemory) Connect() error {

	// We'll initialise the top-level maps.

	f.keys = make(map[strfmt.UUID]models.Key, 0)
	f.key_tokens = make(map[strfmt.UUID]string, 0)
	f.key_children = make(map[strfmt.UUID][]strfmt.UUID)
	f.actions = make(map[strfmt.UUID]models.Action, 0)
	f.things = make(map[strfmt.UUID]models.Thing, 0)

	return nil
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *InMemory) Init() error {

	/*
	 * 1.  If a schema is needed, you need to add the schema to the DB here.
	 * 1.1 Create the (thing or action) classes first, classes that a node (subject or object) can have (for example: Building, Person, etcetera)
	 * 2.  Create a root key.
	 */
	keyObject := models.Key{}

	var key_id strfmt.UUID
	var hashed_token string

	if f.Config.RootKey != "" && f.Config.RootToken != "" {
		f.messaging.InfoMessage("Using user-provided key and token")
		hashed_token, key_id = connutils.CreateRootKeyObjectWithKeyAndToken(&keyObject, strfmt.UUID(f.Config.RootKey), strfmt.UUID(f.Config.RootToken))
	} else {
		hashed_token, key_id = connutils.CreateRootKeyObject(&keyObject)
	}

	err := f.AddKey(nil, &keyObject, key_id, hashed_token)
	return err
}

// Attach can attach something to the request-context
func (f *InMemory) Attach(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

// AddThing adds a thing to the InMemory database with the given UUID.
// Takes the thing and a UUID as input.
// Thing is already validated against the ontology
func (f *InMemory) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	_, found := f.things[UUID]

	if found {
		return fmt.Errorf("There is already a thing with UUID %v", UUID)
	} else {
		f.things[UUID] = *thing
		return nil
	}
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *InMemory) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {

	thing, found := f.things[UUID]

	if found {
		fillThingGetResponseWithThing(UUID, thingResponse, &thing)
		return nil
	} else {
		return errors_.New(connutils.StaticThingNotFound)
	}
}

func fillThingGetResponseWithThing(uuid strfmt.UUID, thingResponse *models.ThingGetResponse, thing *models.Thing) {
	thingResponse.ThingID = uuid
	thingResponse.AtClass = thing.AtClass
	thingResponse.AtContext = thing.AtContext
	thingResponse.Schema = thing.Schema
	thingResponse.CreationTimeUnix = thing.CreationTimeUnix
	thingResponse.Key = thing.Key
	thingResponse.LastUpdateTimeUnix = thing.LastUpdateTimeUnix
}

// GetThings fills the given ThingsListResponse with the values from the database, based on the given UUIDs.
func (f *InMemory) GetThings(ctx context.Context, UUIDs []strfmt.UUID, response *models.ThingsListResponse) error {
	f.messaging.DebugMessage(fmt.Sprintf("GetThings: %s", UUIDs))

	response.TotalResults = 0
	response.Things = make([]*models.ThingGetResponse, 0)

	for _, uuid := range UUIDs {
		thing, found := f.things[uuid]

		if found {
			response.TotalResults += 1
			var thing_response models.ThingGetResponse
			fillThingGetResponseWithThing(uuid, &thing_response, &thing)

			response.Things = append(response.Things, &thing_response)
		}
	}

	// If success return nil, otherwise return the error
	return nil
}

type thingsCreationTimeSorter struct {
	things []*models.ThingGetResponse
}

func (t *thingsCreationTimeSorter) Len() int {
	return len(t.things)
}

// Sort in descending order, so newest first.
func (t *thingsCreationTimeSorter) Less(i, j int) bool {
	return t.things[i].CreationTimeUnix < t.things[j].CreationTimeUnix
}

func (t *thingsCreationTimeSorter) Swap(i, j int) {
	t.things[i].CreationTimeUnix, t.things[j].CreationTimeUnix = t.things[j].CreationTimeUnix, t.things[i].CreationTimeUnix
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *InMemory) ListThings(ctx context.Context, limit int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, response *models.ThingsListResponse) error {

	// thingsResponse should be populated with the response that comes from the DB.
	// thingsResponse = based on the ontology

	response.TotalResults = 0
	response.Things = make([]*models.ThingGetResponse, 0)

	for uuid, thing := range f.things {
		ok := true // should we add this thing to the result list?
		if len(wheres) > 0 {
			// TODO: implement
			return fmt.Errorf("Where queries not supported")
		}

		if ok {
			response.TotalResults += 1
			var thing_response models.ThingGetResponse
			fillThingGetResponseWithThing(uuid, &thing_response, &thing)

			response.Things = append(response.Things, &thing_response)
		}
	}

	sorter := thingsCreationTimeSorter{
		things: response.Things,
	}

	sort.Sort(&sorter)
	response.Things = sorter.things

	// now reverse the things, because there is an undocumented requirement
	// that the newest things should be returned first.
	for i, j := 0, len(response.Things)-1; i < j; i, j = i+1, j-1 {
		response.Things[i], response.Things[j] = response.Things[j], response.Things[i]
	}

	// Higher offset than what's available? Nope.
	if offset > len(response.Things) {
		response.Things = make([]*models.ThingGetResponse, 0)
	} else if offset+limit > len(response.Things) {
		response.Things = response.Things[offset:]
	} else {
		response.Things = response.Things[offset:(offset + limit)]
	}

	// If success return nil, otherwise return the error
	return nil
}

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *InMemory) UpdateThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	_, found := f.things[UUID]

	if !found {
		return fmt.Errorf("There is no such thing with UUID %v", UUID)
	} else {
		f.things[UUID] = *thing
		return nil
	}
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *InMemory) DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	_, found := f.things[UUID]

	if !found {
		return fmt.Errorf("There is no such thing with UUID %v", UUID)
	} else {
		delete(f.things, UUID)
		return nil
	}
}

// HistoryThing fills the history of a thing based on its UUID
func (f *InMemory) HistoryThing(ctx context.Context, UUID strfmt.UUID, history *models.ThingHistory) error {
	//TODO
	return nil
}

// MoveToHistoryThing moves a thing to history
func (f *InMemory) MoveToHistoryThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID, deleted bool) error {
	//TODO
	return nil
}

// AddAction adds an action to the InMemory database with the given UUID.
// Takes the action and a UUID as input.
// Action is already validated against the ontology
func (f *InMemory) AddAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	//TODO
	return nil
}

// GetAction fills the given ActionGetResponse with the values from the database, based on the given UUID.
func (f *InMemory) GetAction(ctx context.Context, UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	// actionResponse should be populated with the response that comes from the DB.
	// actionResponse = based on the ontology

	// If success return nil, otherwise return the error
	//TODO
	return nil
}

// GetActions fills the given ActionsListResponse with the values from the database, based on the given UUIDs.
func (f *InMemory) GetActions(ctx context.Context, UUIDs []strfmt.UUID, actionsResponse *models.ActionsListResponse) error {
	// If success return nil, otherwise return the error
	//TODO
	return nil
}

// ListActions fills the given ActionListResponse with the values from the database, based on the given parameters.
func (f *InMemory) ListActions(ctx context.Context, UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	// actionsResponse should be populated with the response that comes from the DB.
	// actionsResponse = based on the ontology

	// If success return nil, otherwise return the error
	//TODO
	return nil
}

// UpdateAction updates the Thing in the DB at the given UUID.
func (f *InMemory) UpdateAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {

	// If success return nil, otherwise return the error
	//TODO
	return nil
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *InMemory) DeleteAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {

	// Run the query to delete the action based on its UUID.

	// If success return nil, otherwise return the error
	//TODO
	return nil
}

// HistoryAction fills the history of a Action based on its UUID
func (f *InMemory) HistoryAction(ctx context.Context, UUID strfmt.UUID, history *models.ActionHistory) error {
	//TODO
	return nil
}

// MoveToHistoryAction moves an action to history
func (f *InMemory) MoveToHistoryAction(ctx context.Context, action *models.Action, UUID strfmt.UUID, deleted bool) error {
	//TODO
	return nil
}

// AddKey adds a key to the InMemory database with the given UUID and token.
// UUID  = reference to the key
// token = is the actual access token used in the API's header
func (f *InMemory) AddKey(ctx context.Context, key *models.Key, UUID strfmt.UUID, token string) error {
	// Key struct should be stored
	// If success return nil, otherwise return the error

	_, found := f.keys[UUID]
	if found {
		return fmt.Errorf("Key already exists")
	} else {
		if key.Parent != nil {
			if key.Parent.Type != "Key" {
				f.messaging.DebugMessage("AddKey FAILED: parent of a key should be of type key!")
				return fmt.Errorf("Parent of a key should be of type Key!")
			}

			if key.Parent.LocationURL != nil && *key.Parent.LocationURL != "http://localhost" {
				f.messaging.ErrorMessage(fmt.Sprintf("AddKey added non-local locations for parents are not supported, location is: %+v", *key.Parent.LocationURL))
			}

			parent_id := key.Parent.NrDollarCref

			err := f.GetKey(ctx, parent_id, nil)
			if err != nil {
				f.messaging.DebugMessage(fmt.Sprintf("AddKey FAILED: Parent %v does not exist; %+v", parent_id, err))
				return fmt.Errorf("Parent does not exist")
			}

			f.key_children[parent_id] = append(f.key_children[parent_id], UUID)

			f.messaging.DebugMessage(fmt.Sprintf("KEY_CHILDREN: %v", f.key_children))
		}

		f.keys[UUID] = *key
		f.key_tokens[UUID] = token
		log.Debugf("Added key %v %+v", UUID, *key)

		return nil
	}
}

// ValidateToken validates/gets a key to the InMemory database with the given token (=UUID)
func (f *InMemory) ValidateToken(ctx context.Context, UUID strfmt.UUID, keyResponse *models.KeyGetResponse) (token string, err error) {
	token, found := f.key_tokens[UUID]
	if found {
		err := f.GetKey(ctx, UUID, keyResponse)
		if err != nil {
			return "", err
		} else {
			return token, nil
		}
	} else {
		return "", errors_.New(connutils.StaticKeyNotFound)
	}
}

// GetKey fills the given KeyGetResponse with the values from the database, based on the given UUID.
func (f *InMemory) GetKey(ctx context.Context, UUID strfmt.UUID, keyResponse *models.KeyGetResponse) error {
	key, found := f.keys[UUID]
	if found {
		response := models.KeyGetResponse{
			Key:   key,
			KeyID: UUID,
		}

		if keyResponse != nil {
			*keyResponse = response
		}

		return nil
	} else {
		return errors_.New(connutils.StaticKeyNotFound)
	}
}

// GetKeys fills the given []KeyGetResponse with the values from the database, based on the given UUIDs.
func (f *InMemory) GetKeys(ctx context.Context, UUIDs []strfmt.UUID, keysResponse *[]*models.KeyGetResponse) error {
	panic("NOT IMPLEMENTED")
	fmt.Printf("GET KEYS%v\n", UUIDs)
	//TODO
	return nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *InMemory) DeleteKey(ctx context.Context, key *models.Key, UUID strfmt.UUID) error {
	panic("not supported")
	//TODO
	return nil
}

// GetKeyChildren fills the given KeyGetResponse array with the values from the database, based on the given UUID.
func (f *InMemory) GetKeyChildren(ctx context.Context, UUID strfmt.UUID, children *[]*models.KeyGetResponse) error {
	for _, child_id := range f.key_children[UUID] {
		var response models.KeyGetResponse
		err := f.GetKey(ctx, child_id, &response)
		if err != nil {
			return fmt.Errorf("Could not fetch child key %v, because: %v", UUID, err)
		}
		*children = append(*children, &response)
	}

	return nil
}

func remove_uuid_from_list(l []strfmt.UUID, item strfmt.UUID) []strfmt.UUID {
	for i, other := range l {
		if other == item {
			return append(l[:i], l[i+1:]...)
		}
	}
	return l
}

// UpdateKey updates the Key in the DB at the given UUID.
func (f *InMemory) UpdateKey(ctx context.Context, key *models.Key, UUID strfmt.UUID, token string) error {
	existing_key, found := f.keys[UUID]
	if !found {
		return fmt.Errorf("Key does not exists")
	} else {
		if existing_key.Parent != nil {
			old_parent_id := key.Parent.NrDollarCref
			remove_uuid_from_list(f.key_children[old_parent_id], UUID)
		}

		if key.Parent != nil {
			if key.Parent.Type != "Key" {
				f.messaging.DebugMessage("AddKey FAILED: parent of a key should be of type key!")
				return fmt.Errorf("Parent of a key should be of type Key!")
			}

			if key.Parent.LocationURL != nil && *key.Parent.LocationURL != "http://localhost" {
				f.messaging.ErrorMessage(fmt.Sprintf("AddKey added non-local locations for parents are not supported, location is: %+v", *key.Parent.LocationURL))
			}

			parent_id := key.Parent.NrDollarCref

			err := f.GetKey(ctx, parent_id, nil)
			if err != nil {
				f.messaging.DebugMessage(fmt.Sprintf("AddKey FAILED: Parent %v does not exist; %+v", parent_id, err))
				return fmt.Errorf("Parent does not exist")
			}

			f.key_children[parent_id] = append(f.key_children[parent_id], UUID)

			f.messaging.DebugMessage(fmt.Sprintf("KEY_CHILDREN: %v", f.key_children))
		}

		f.keys[UUID] = *key
		f.key_tokens[UUID] = token
		log.Debugf("Updated key %v %+v", UUID, *key)

		return nil
	}
}
