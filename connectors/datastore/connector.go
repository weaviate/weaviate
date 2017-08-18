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

package datastore

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/models"
	"log"

	"cloud.google.com/go/datastore"
	gouuid "github.com/satori/go.uuid"

	"github.com/weaviate/weaviate/connectors/config"
	"github.com/weaviate/weaviate/connectors/utils"
)

// Datastore has some basic variables.
type Datastore struct {
	client *datastore.Client
}

// SetConfig is used to fill in a struct with config variables
func (f *Datastore) SetConfig(configInput connectorConfig.Environment) {
	// NOTHING HERE
}

// GetName returns a unique connector name
func (f *Datastore) GetName() string {
	return "datastore"
}

// Connect to datastore
func (f *Datastore) Connect() error {
	// Set ctx, your Google Cloud Platform project ID and kind.
	ctx := context.Background()
	projectID := "weaviate-dev-001"

	// Create new client
	client, err := datastore.NewClient(ctx, projectID)

	// If error, return it. Otherwise set client.
	if err != nil {
		return err
	}

	f.client = client
	return nil
}

// Creates a root key and tables if not already available
func (f *Datastore) Init() error {

	ctx := context.Background()

	kind := "weaviate_users"

	// create query to check for root key
	query := datastore.NewQuery(kind).Filter("Parent =", "*").Limit(1)

	dbKeyObjects := []connector_utils.DatabaseUsersObject{}

	_, err := f.client.GetAll(ctx, query, &dbKeyObjects)

	if err != nil {
		panic("ERROR INITIALIZING SERVER: " + err.Error())
	}

	// No key was found, create one
	if len(dbKeyObjects) == 0 {
		// Generate a basic DB object and print it's key.
		dbObject := connector_utils.CreateFirstUserObject()

		// Creates a Key instance.
		taskKey := datastore.NameKey(kind, dbObject.Uuid, nil)

		// Saves the new entity.
		if _, err := f.client.Put(ctx, taskKey, &dbObject); err != nil {
			log.Fatalf("Failed to save task: %v", err)
		}
	}

	return nil
}

// Add item to DB
func (f *Datastore) Add(dbObject connector_utils.DatabaseObject) (string, error) {
	// Move all other objects to history
	f.MoveToHistory(dbObject.Uuid)

	// Add item to Datastore
	newUUID, _ := f.AddByKind(dbObject, "weaviate")

	// Return the ID that is used to create.
	return newUUID, nil
}

// AddHistory adds an item to the history kind
func (f *Datastore) MoveToHistory(UUIDToMove string) (bool, error) {
	// Set ctx and kind.
	ctx := context.Background()

	// Make list query with all items
	query := datastore.NewQuery("weaviate").Filter("Uuid =", UUIDToMove)

	// Fill object with results
	dbObjectsToMove := connector_utils.DatabaseObjects{}
	keys, err := f.client.GetAll(ctx, query, &dbObjectsToMove)

	for index, dbObjectToMove := range dbObjectsToMove {
		// Add item to Datastore
		if _, errAdd := f.AddByKind(dbObjectToMove, "weaviate_history"); errAdd != nil {
			log.Fatalf("Failed to add history task: %v", errAdd)
		}

		// Deletes the old entity.
		if err := f.client.Delete(ctx, keys[index]); err != nil {
			log.Fatalf("Failed to delete task: %v", err)
		}
	}

	// Return true
	return true, err
}

// AddByKind adds using a kind
func (f *Datastore) AddByKind(dbObject connector_utils.DatabaseObject, kind string) (string, error) {
	// Set ctx and kind.
	ctx := context.Background()

	// Generate an UUID
	nameUUID := fmt.Sprintf("%v", gouuid.NewV4())

	// Creates a Key instance.
	taskKey := datastore.NameKey(kind, nameUUID, nil)

	// Saves the new entity.
	if _, err := f.client.Put(ctx, taskKey, &dbObject); err != nil {
		log.Fatalf("Failed to save task: %v", err)
		return "Error", err
	}

	// Return the ID that is used to create.
	return dbObject.Uuid, nil
}

// Get DatabaseObject from DB by uuid
func (f *Datastore) Get(uuid string) (connector_utils.DatabaseObject, error) {
	// Set ctx and kind.
	ctx := context.Background()
	kind := "weaviate"

	// Make get Query
	query := datastore.NewQuery(kind).Filter("Uuid =", uuid).Order("-CreateTimeMs").Limit(1)

	// Fill object
	object := connector_utils.DatabaseObjects{}
	keys, err := f.client.GetAll(ctx, query, &object)

	// Return error
	if err != nil {
		log.Fatalf("Failed to load task: %v", err)
		return connector_utils.DatabaseObject{}, err
	}

	// Return error 'not found'
	if len(keys) == 0 {
		notFoundErr := errors.New("no object with such UUID found")
		return connector_utils.DatabaseObject{}, notFoundErr
	}

	// Return found object
	return object[0], nil
}

// List lists the items from Datastore by refType and limit
func (f *Datastore) List(refType string, ownerUUID string, limit int, page int, referenceFilter *connector_utils.ObjectReferences) (connector_utils.DatabaseObjects, int64, error) {
	// Set ctx and kind.
	ctx := context.Background()
	kind := "weaviate"

	// Calculate offset
	offset := (page - 1) * limit

	// Make list queries
	query := datastore.NewQuery(kind).Filter("RefType =", refType).Filter("Owner =", ownerUUID).Filter("Deleted =", false).Order("-CreateTimeMs")

	// Add more to queries for reference filters
	if referenceFilter != nil {
		if referenceFilter.ThingID != "" {
			query = query.Filter("RelatedObjects.ThingID = ", string(referenceFilter.ThingID))
		}
	}

	// Make total results query
	totalResultsQuery := query

	// finish query
	query = query.Limit(limit).Offset(offset)

	// Fill object with results
	dbObjects := connector_utils.DatabaseObjects{}
	_, err := f.client.GetAll(ctx, query, &dbObjects)
	totalResults, errTotal := f.client.Count(ctx, totalResultsQuery)

	// Return error and empty object
	if err != nil || errTotal != nil {
		log.Fatalf("Failed to load task: %v", err)

		return connector_utils.DatabaseObjects{}, 0, err
	}

	// Return list with objects
	return dbObjects, int64(totalResults), nil
}

// Validate if a user has access, returns permissions object
func (f *Datastore) ValidateKey(token string) ([]connector_utils.DatabaseUsersObject, error) {
	// Set ctx and kind.
	ctx := context.Background()
	kind := "weaviate_users"

	// Check on token and deletion
	query := datastore.NewQuery(kind).Filter("KeyToken =", token).Limit(1)

	dbUsersObjects := []connector_utils.DatabaseUsersObject{}

	_, err := f.client.GetAll(ctx, query, &dbUsersObjects)

	if err != nil {
		return dbUsersObjects, err
	}

	// keys are found, return them
	return dbUsersObjects, nil
}

// GetKey returns user object by ID
func (f *Datastore) GetKey(uuid string) (connector_utils.DatabaseUsersObject, error) {
	// Set ctx and kind.
	ctx := context.Background()
	kind := "weaviate_users"

	// Create get Query
	query := datastore.NewQuery(kind).Filter("Uuid =", uuid).Filter("Deleted =", false).Limit(1)

	// Fill User object
	userObject := []connector_utils.DatabaseUsersObject{}
	keys, err := f.client.GetAll(ctx, query, &userObject)

	// Return error
	if err != nil {
		log.Fatalf("Failed to load task: %v", err)
		return connector_utils.DatabaseUsersObject{}, err
	}

	// Return error 'not found'
	if len(keys) == 0 {
		notFoundErr := errors.New("No userObject with such UUID found")
		return connector_utils.DatabaseUsersObject{}, notFoundErr
	}

	// Return found object
	return userObject[0], nil
}

// AddKey to DB
func (f *Datastore) AddKey(parentUUID string, dbObject connector_utils.DatabaseUsersObject) (connector_utils.DatabaseUsersObject, error) {
	ctx := context.Background()

	kind := "weaviate_users"

	nameUUID := fmt.Sprintf("%v", gouuid.NewV4())

	// Creates a Key instance.
	taskKey := datastore.NameKey(kind, nameUUID, nil)

	// Auto set the parent ID
	dbObject.Parent = parentUUID

	// Saves the new entity.
	if _, err := f.client.Put(ctx, taskKey, &dbObject); err != nil {
		log.Fatalf("Failed to save task: %v", err)
		return dbObject, err
	}

	// Return the ID that is used to create.
	return dbObject, nil
}

// DeleteKey removes a key from the database
func (f *Datastore) DeleteKey(UUID string) error {
	ctx := context.Background()

	kind := "weaviate_users"

	// Create get Query
	query := datastore.NewQuery(kind).Filter("Uuid =", UUID).Limit(1)

	// Fill User object
	userObjects := []connector_utils.DatabaseUsersObject{}
	keys, _ := f.client.GetAll(ctx, query, &userObjects)

	if len(keys) == 0 {
		notFoundErr := errors.New("no object with such UUID found")
		return notFoundErr
	}

	userObject := userObjects[0]
	userObject.Deleted = true

	// Deletes the user itself
	_, errDel := f.client.Put(ctx, keys[0], &userObject)
	if errDel != nil {
		log.Fatalf("Failed to delete task: %v", errDel)
	}

	// Return error if exists
	return errDel
}

// GetChildKeys returns all the child keys
func (f *Datastore) GetChildObjects(UUID string, filterOutDeleted bool) ([]connector_utils.DatabaseUsersObject, error) {
	ctx := context.Background()

	// Find its children
	var queryChildren *datastore.Query
	if filterOutDeleted {
		queryChildren = datastore.NewQuery("weaviate_users").Filter("Parent =", UUID).Filter("Deleted = ", false)
	} else {
		queryChildren = datastore.NewQuery("weaviate_users").Filter("Parent =", UUID)
	}

	// Fill children array
	childUserObjects := []connector_utils.DatabaseUsersObject{}
	f.client.GetAll(ctx, queryChildren, &childUserObjects)

	return childUserObjects, nil
}

func (f *Datastore) AddThing(thing *models.ThingCreate, uuid strfmt.UUID) error {
	return nil
}

func (f *Datastore) GetThing(UUID strfmt.UUID) (models.ThingGetResponse, error) {
	thingResponse := models.ThingGetResponse{}

	return thingResponse, nil
}

func (f *Datastore) ListThings(limit int, page int) (models.ThingsListResponse, error) {
	thingsListResponse := models.ThingsListResponse{}

	return thingsListResponse, nil
}
