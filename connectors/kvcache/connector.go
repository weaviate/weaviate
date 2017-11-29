/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package kvcache

import (
	"fmt"
	"github.com/go-openapi/strfmt"
	cache "github.com/patrickmn/go-cache"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

// KVCache has some basic variables.
type KVCache struct {
	databaseConnector dbconnector.DatabaseConnector
	cache             *cache.Cache
	messaging         *messages.Messaging
}

// SetDatabaseConnector sets the used DB-connector
func (f *KVCache) SetDatabaseConnector(dbConnector dbconnector.DatabaseConnector) {
	f.databaseConnector = dbConnector
}

// GetName returns a unique connector name
func (f *KVCache) GetName() string {
	return "kv-cache"
}

// Connect function
func (f *KVCache) Connect() error {
	return f.databaseConnector.Connect()
}

// Init function
func (f *KVCache) Init() error {
	f.cache = cache.New(0, 0)

	return f.databaseConnector.Init()
}

// SetServerAddress function
func (f *KVCache) SetServerAddress(serverAddress string) {
	f.databaseConnector.SetServerAddress(serverAddress)
}

// SetConfig function
func (f *KVCache) SetConfig(configInput *config.Environment) error {
	return f.databaseConnector.SetConfig(configInput)
}

// SetMessaging is used to fill the messaging object
func (f *KVCache) SetMessaging(m *messages.Messaging) error {
	f.messaging = m
	f.databaseConnector.SetMessaging(m)

	return nil
}

// SetSchema function
func (f *KVCache) SetSchema(schemaInput *schema.WeaviateSchema) error {
	return f.databaseConnector.SetSchema(schemaInput)
}

// AddThing function
func (f *KVCache) AddThing(thing *models.Thing, UUID strfmt.UUID) error {
	return f.databaseConnector.AddThing(thing, UUID)
}

// GetThing function
func (f *KVCache) GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	// Create a cache key
	key := fmt.Sprintf("Thing#%s", UUID)

	// Get the item from the cache
	v, found := f.cache.Get(key)

	// If it is found in the cache, set the response pointer to the same thing as in the cache is pointed to
	if found {
		*thingResponse = *v.(*models.ThingGetResponse)

		return nil
	}

	// If not found, get it from the DB
	err := f.databaseConnector.GetThing(UUID, thingResponse)

	// If no error is given, set the pointer in the cache on the created key
	if err == nil {
		f.cache.Set(key, thingResponse, 0)
	}

	return err
}

// ListThings function
func (f *KVCache) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {
	return f.databaseConnector.ListThings(first, offset, keyID, wheres, thingsResponse)
}

// UpdateThing function
func (f *KVCache) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {
	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Thing#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.UpdateThing(thing, UUID)
}

// DeleteThing function
func (f *KVCache) DeleteThing(UUID strfmt.UUID) error {
	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Thing#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.DeleteThing(UUID)
}

// AddAction function
func (f *KVCache) AddAction(action *models.Action, UUID strfmt.UUID) error {
	return f.databaseConnector.AddAction(action, UUID)
}

// GetAction function
func (f *KVCache) GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	// Create a cache key
	key := fmt.Sprintf("Action#%s", UUID)

	// Get the item from the cache
	v, found := f.cache.Get(key)

	// If it is found in the cache, set the response pointer to the same action as in the cache is pointed to
	if found {
		*actionResponse = *v.(*models.ActionGetResponse)

		return nil
	}

	// If not found, get it from the DB
	err := f.databaseConnector.GetAction(UUID, actionResponse)

	// If no error is given, set the pointer in the cache on the created key
	if err == nil {
		f.cache.Set(key, actionResponse, 0)
	}

	return err
}

// ListActions function
func (f *KVCache) ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	return f.databaseConnector.ListActions(UUID, first, offset, wheres, actionsResponse)
}

// UpdateAction function
func (f *KVCache) UpdateAction(action *models.Action, UUID strfmt.UUID) error {
	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Action#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.UpdateAction(action, UUID)
}

// DeleteAction function
func (f *KVCache) DeleteAction(UUID strfmt.UUID) error {
	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Action#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.DeleteAction(UUID)
}

// AddKey function
func (f *KVCache) AddKey(key *models.Key, UUID strfmt.UUID, token strfmt.UUID) error {
	return f.databaseConnector.AddKey(key, UUID, token)
}

// ValidateToken function
func (f *KVCache) ValidateToken(UUID strfmt.UUID, key *models.KeyTokenGetResponse) error {
	// Delete from cache before updating, otherwise the old version still exists
	ck := fmt.Sprintf("Key#%s", UUID)
	f.cache.Delete(ck)

	return f.databaseConnector.ValidateToken(UUID, key)
}

// GetKey function
func (f *KVCache) GetKey(UUID strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {
	// Create a cache key
	key := fmt.Sprintf("Key#%s", UUID)

	// Get the item from the cache
	v, found := f.cache.Get(key)

	// If it is found in the cache, set the response pointer to the same key as in the cache is pointed to
	if found {
		*keyResponse = *v.(*models.KeyTokenGetResponse)

		return nil
	}

	// If not found, get it from the DB
	err := f.databaseConnector.GetKey(UUID, keyResponse)

	// If no error is given, set the pointer in the cache on the created key
	if err == nil {
		f.cache.Set(key, keyResponse, 0)
	}

	return err
}

// DeleteKey function
func (f *KVCache) DeleteKey(UUID strfmt.UUID) error {
	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Key#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.DeleteKey(UUID)
}

// GetKeyChildren function
func (f *KVCache) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyTokenGetResponse) error {
	return f.databaseConnector.GetKeyChildren(UUID, children)
}
