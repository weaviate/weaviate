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
	"fmt"
	"log"
	"time"

	"github.com/weaviate/weaviate/connectors"

	uuid "github.com/satori/go.uuid"

	"cloud.google.com/go/datastore"
)

type Datastore struct {
	client *datastore.Client
}

// Connect to datastore
func (f *Datastore) Connect() error {
	// Set ctx and your Google Cloud Platform project ID.
	ctx := context.Background()

	projectID := "weaviate-dev-001"

	client, err := datastore.NewClient(ctx, projectID)

	if err != nil {
		return err
	}

	f.client = client
	return nil
}

// Add item to DB
func (f *Datastore) Add(owner string, refType string, object string) (string, error) {
	ctx := context.Background()

	// Sets the kind for the new entity.
	kind := "weaviate"

	// Sets the name/ID for the new entity.
	uuid := fmt.Sprintf("%v", uuid.NewV4())

	// Creates a Key instance.
	taskKey := datastore.NameKey(kind, uuid, nil)

	// Creates a Task instance.
	task := dbconnector.Object{
		Uuid:         uuid,
		Owner:        owner,
		RefType:      refType,
		CreateTimeMs: time.Now().UnixNano() / int64(time.Millisecond),
		Object:       object,
		Deleted:      false,
	}

	// Saves the new entity.
	if _, err := f.client.Put(ctx, taskKey, &task); err != nil {
		log.Fatalf("Failed to save task: %v", err)
		return "Error", err
	}

	// Return the ID that is used to create.
	return uuid, nil

}

func (f *Datastore) Get(Uuid string) (dbconnector.Object, error) {
	ctx := context.Background()

	query := datastore.NewQuery("weaviate").Filter("Uuid =", Uuid).Order("-CreateTimeMs").Limit(1)

	object := []dbconnector.Object{}

	if keys, err := f.client.GetAll(ctx, query, &object); err != nil {
		log.Fatalf("Failed to load task: %v", err)

		return dbconnector.Object{}, err
	} else {
		if len(keys) == 0 {
			return dbconnector.Object{}, nil
		} else {
			return object[0], nil
		}
	}
}
