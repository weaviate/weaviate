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

package kvcache

import (
	"context"
	"fmt"
	"time"

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

// Attach can attach something to the request-context
func (f *KVCache) Attach(ctx context.Context) (context.Context, error) {
	return ctx, nil
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
func (f *KVCache) AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.AddThing(ctx, thing, UUID)
}

// GetThing function
func (f *KVCache) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	defer f.messaging.TimeTrack(time.Now())

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
	err := f.databaseConnector.GetThing(ctx, UUID, thingResponse)

	// If no error is given, set the pointer in the cache on the created key
	if err == nil {
		f.cache.Set(key, thingResponse, 0)
	}

	return err
}

// GetThings funciton
func (f *KVCache) GetThings(ctx context.Context, UUIDs []strfmt.UUID, thingResponse *models.ThingsListResponse) error {
	return f.databaseConnector.GetThings(ctx, UUIDs, thingResponse)
}

// ListThings function
func (f *KVCache) ListThings(ctx context.Context, first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.ListThings(ctx, first, offset, keyID, wheres, thingsResponse)
}

// UpdateThing function
func (f *KVCache) UpdateThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Thing#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.UpdateThing(ctx, thing, UUID)
}

// DeleteThing function
func (f *KVCache) DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Thing#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.DeleteThing(ctx, thing, UUID)
}

// HistoryThing fills the history of a thing based on its UUID
func (f *KVCache) HistoryThing(ctx context.Context, UUID strfmt.UUID, history *models.ThingHistory) error {
	return f.databaseConnector.HistoryThing(ctx, UUID, history)
}

// MoveToHistoryThing moves a thing to history
func (f *KVCache) MoveToHistoryThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID, deleted bool) error {
	return f.databaseConnector.MoveToHistoryThing(ctx, thing, UUID, deleted)
}

// AddAction function
func (f *KVCache) AddAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.AddAction(ctx, action, UUID)
}

// GetAction function
func (f *KVCache) GetAction(ctx context.Context, UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	defer f.messaging.TimeTrack(time.Now())

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
	err := f.databaseConnector.GetAction(ctx, UUID, actionResponse)

	// If no error is given, set the pointer in the cache on the created key
	if err == nil {
		f.cache.Set(key, actionResponse, 0)
	}

	return err
}

// ListActions function
func (f *KVCache) ListActions(ctx context.Context, UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.ListActions(ctx, UUID, first, offset, wheres, actionsResponse)
}

// UpdateAction function
func (f *KVCache) UpdateAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Action#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.UpdateAction(ctx, action, UUID)
}

// DeleteAction function
func (f *KVCache) DeleteAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// Delete from cache before updating, otherwise the old version still exists
	key := fmt.Sprintf("Action#%s", UUID)
	f.cache.Delete(key)

	return f.databaseConnector.DeleteAction(ctx, action, UUID)
}

// HistoryAction fills the history of a Action based on its UUID
func (f *KVCache) HistoryAction(ctx context.Context, UUID strfmt.UUID, history *models.ActionHistory) error {
	return f.databaseConnector.HistoryAction(ctx, UUID, history)
}

// MoveToHistoryAction moves a action to history
func (f *KVCache) MoveToHistoryAction(ctx context.Context, action *models.Action, UUID strfmt.UUID, deleted bool) error {
	return f.databaseConnector.MoveToHistoryAction(ctx, action, UUID, deleted)
}

// AddKey function
func (f *KVCache) AddKey(ctx context.Context, key *models.Key, UUID strfmt.UUID, token string) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.AddKey(ctx, key, UUID, token)
}

// ValidateToken function
func (f *KVCache) ValidateToken(ctx context.Context, UUID strfmt.UUID, keyResponse *models.KeyGetResponse) (token string, err error) {
	defer f.messaging.TimeTrack(time.Now())

	token, err = f.databaseConnector.ValidateToken(ctx, UUID, keyResponse)

	return token, err
}

// GetKey function
func (f *KVCache) GetKey(ctx context.Context, UUID strfmt.UUID, keyResponse *models.KeyGetResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	// Create a cache key
	ck := fmt.Sprintf("Key#%s", UUID)

	// Get the item from the cache
	v, found := f.cache.Get(ck)

	// If it is found in the cache, set the response pointer to the same key as in the cache is pointed to
	if found {
		*keyResponse = *v.(*models.KeyGetResponse)

		return nil
	}

	// If not found, get it from the DB
	err := f.databaseConnector.GetKey(ctx, UUID, keyResponse)

	// If no error is given, set the pointer in the cache on the created key
	if err == nil {
		f.cache.Set(ck, keyResponse, 0)
	}

	return err
}

// DeleteKey function
func (f *KVCache) DeleteKey(ctx context.Context, key *models.Key, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// Delete from cache before updating, otherwise the old version still exists
	ck := fmt.Sprintf("Key#%s", UUID)
	f.cache.Delete(ck)

	// Also remove the 'token'-cache, by loading the key first
	keyResponse := &models.KeyGetResponse{}
	f.GetKey(ctx, UUID, keyResponse)

	return f.databaseConnector.DeleteKey(ctx, key, UUID)
}

// GetKeyChildren function
func (f *KVCache) GetKeyChildren(ctx context.Context, UUID strfmt.UUID, children *[]*models.KeyGetResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.GetKeyChildren(ctx, UUID, children)
}

// UpdateKey updates the Key in the DB at the given UUID.
func (f *KVCache) UpdateKey(ctx context.Context, key *models.Key, UUID strfmt.UUID, token string) error {
	return f.databaseConnector.UpdateKey(ctx, key, UUID, token)
}
