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
	"log"

	gouuid "github.com/satori/go.uuid"
	"github.com/weaviate/weaviate/connectors"

	"cloud.google.com/go/datastore"
	"fmt"
	"time"
)

// Datastore has some basic variables.
type Datastore struct {
	client *datastore.Client
	kind   string
}

// Connect to datastore
func (f *Datastore) Connect() error {
	// Set ctx and your Google Cloud Platform project ID.
	ctx := context.Background()

	f.kind = "weaviate"

	projectID := "weaviate-dev-001"

	client, err := datastore.NewClient(ctx, projectID)

	if err != nil {
		return err
	}

	f.client = client
	return nil
}

// Add item to DB
func (f *Datastore) Add(dbObject dbconnector.DatabaseObject) (string, error) {
	ctx := context.Background()

	nameUUID := fmt.Sprintf("%v", gouuid.NewV4())

	// Creates a Key instance.
	taskKey := datastore.NameKey(f.kind, nameUUID, nil)

	// Saves the new entity.
	if _, err := f.client.Put(ctx, taskKey, &dbObject); err != nil {
		log.Fatalf("Failed to save task: %v", err)
		return "Error", err
	}

	// Return the ID that is used to create.
	return dbObject.Uuid, nil
}

// Get DatabaseObject from DB by uuid
func (f *Datastore) Get(uuid string) (dbconnector.DatabaseObject, error) {
	ctx := context.Background()

	query := datastore.NewQuery(f.kind).Filter("Uuid =", uuid).Order("-CreateTimeMs").Limit(1)

	object := []dbconnector.DatabaseObject{}

	keys, err := f.client.GetAll(ctx, query, &object)

	if err != nil {
		log.Fatalf("Failed to load task: %v", err)

		return dbconnector.DatabaseObject{}, err
	}

	if len(keys) == 0 {
		return dbconnector.DatabaseObject{}, nil
	}

	return object[0], nil
}

// Delete item from Datastore by changing deleted status
func (f *Datastore) Delete(uuid string) error {
	databaseObject, errGet := f.Get(uuid)

	// Not found or deleted
	if errGet != nil {
		return errGet
	}

	databaseObject.Deleted = true
	databaseObject.CreateTimeMs = time.Now().UnixNano() / int64(time.Millisecond)

	_, errAdd := f.Add(databaseObject)

	// Cannot add
	if errAdd != nil {
		return errAdd
	}

	return nil
}

// List lists the items from Datastore by refType and limit
func (f *Datastore) List(refType string, limit int) ([]dbconnector.DatabaseObject, error) {
	ctx := context.Background()

	query := datastore.NewQuery(f.kind).Filter("RefType =", refType).Filter("Deleted =", false).Order("-CreateTimeMs").Limit(limit)

	dbObjects := []dbconnector.DatabaseObject{}

	keys, err := f.client.GetAll(ctx, query, &dbObjects)

	if err != nil {
		log.Fatalf("Failed to load task: %v", err)

		return []dbconnector.DatabaseObject{}, err
	}

	if len(keys) == 0 {
		return []dbconnector.DatabaseObject{}, nil
	}

	return dbObjects, nil
}
