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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

/*
 * Package for Google Datastore
 */

package datastore

import (
	"log"

	"cloud.google.com/go/datastore"
)

/*
 * Add
 * Add item with latest id
 */
func Add(id int64) int64 {
	return id
}

/*
 * Get
 * Gets item with latest id
 */
func Get(id int64) int64 {

	projectID := "weaviate-dev-001"

	// Creates a client.
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	return id
}

/*
 * List
 * Lists a certain item
 */
func List(item int64) int64 {
	return item
}

/*
 * Remove
 * Add a removed tag, should not turn up when using Get
 */
func Remove(id int64) int64 {
	return id
}
