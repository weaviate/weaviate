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

	uuid "github.com/satori/go.uuid"

	"cloud.google.com/go/datastore"
)

type Datastore struct{}

type Object struct {
	Uuid         string // uuid, also used in Object's id
	Owner        string // uuid of the owner
	RefType      string // type, as defined
	CreateTimeMs int64  // creation time in ms
	Object       string // the JSON object, id will be collected from current uuid
	Deleted      bool   // if true, it does not exsist anymore
}

// Add item to DB
func (f Datastore) Add(owner string, refType string, object string) string {

	// Setx your Google Cloud Platform project ID.
	ctx := context.Background()

	projectID := "weaviate-dev-001"

	// Creates a client.
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Sets the kind for the new entity.
	kind := "weaviate"

	// Sets the name/ID for the new entity.
	uuid := fmt.Sprintf("%v", uuid.NewV4())

	// Creates a Key instance.
	taskKey := datastore.NameKey(kind, uuid, nil)

	// Creates a Task instance.
	task := Object{
		Uuid:         uuid,
		Owner:        owner,
		RefType:      refType,
		CreateTimeMs: time.Now().UnixNano() / int64(time.Millisecond),
		Object:       object,
		Deleted:      false,
	}

	// Saves the new entity.
	if _, err := client.Put(ctx, taskKey, &task); err != nil {
		log.Fatalf("Failed to save task: %v", err)
	}

	// return the ID that is used to create.
	return uuid

}
