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

package dataloader

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"gopkg.in/nicksrandall/dataloader.v5"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

// DataLoader has some basic variables.
type DataLoader struct {
	databaseConnector dbconnector.DatabaseConnector
	messaging         *messages.Messaging
}

const thingsDataLoader string = "thingsDataLoader"

// SetDatabaseConnector sets the used DB-connector
func (f *DataLoader) SetDatabaseConnector(dbConnector dbconnector.DatabaseConnector) {
	f.databaseConnector = dbConnector
}

// GetName returns a unique connector name
func (f *DataLoader) GetName() string {
	return "dataloader"
}

// Connect function
func (f *DataLoader) Connect() error {
	return f.databaseConnector.Connect()
}

// Init function
func (f *DataLoader) Init(ctx context.Context) (context.Context, error) {
	// setup batch function
	batchFn := func(ctx context.Context, keys dataloader.Keys) []*dataloader.Result {
		results := []*dataloader.Result{}
		// do some aync work to get data for specified keys
		// append to this list resolved values
		things := &models.ThingsListResponse{}

		UUIDs := []strfmt.UUID{}

		for _, v := range keys {
			UUIDs = append(UUIDs, strfmt.UUID(v.String()))

		}

		err := f.databaseConnector.GetThings(ctx, UUIDs, things)

		if err != nil {
			for _ = range keys {
				results = append(results, &dataloader.Result{Error: err})
			}
			return results
		}

		for _, k := range keys {
			found := false
			for _, v := range things.Things {
				if k.String() == string(v.ThingID) {
					results = append(results, &dataloader.Result{Data: v})
					found = true
					break
				}
			}
			if !found {
				results = append(results, &dataloader.Result{Data: &models.ThingGetResponse{}, Error: fmt.Errorf("thing not found")})
			}
		}

		return results
	}

	// create Loader with an in-memory cache
	ctx = context.WithValue(ctx, thingsDataLoader, dataloader.NewBatchedLoader(
		batchFn,
		dataloader.WithWait(50*time.Millisecond),
		dataloader.WithBatchCapacity(100),
	))

	return f.databaseConnector.Init(ctx)
}

// SetServerAddress function
func (f *DataLoader) SetServerAddress(serverAddress string) {
	f.databaseConnector.SetServerAddress(serverAddress)
}

// SetConfig function
func (f *DataLoader) SetConfig(configInput *config.Environment) error {
	return f.databaseConnector.SetConfig(configInput)
}

// SetMessaging is used to fill the messaging object
func (f *DataLoader) SetMessaging(m *messages.Messaging) error {
	f.messaging = m
	f.databaseConnector.SetMessaging(m)

	return nil
}

// SetSchema function
func (f *DataLoader) SetSchema(schemaInput *schema.WeaviateSchema) error {
	return f.databaseConnector.SetSchema(schemaInput)
}

// AddThing function
func (f *DataLoader) AddThing(thing *models.Thing, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.AddThing(thing, UUID)
}

// GetThing function
func (f *DataLoader) GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("GetThing '%s'", UUID))

	var result interface{}
	// StringKey is a convenience method that make wraps string to implement `Key` interface
	loader, _ := ctx.Value(thingsDataLoader).(*dataloader.Loader)
	thunk := loader.Load(ctx, dataloader.StringKey(string(UUID)))
	result, err := thunk()

	if err == nil {
		thingResponse.Thing = result.(*models.ThingGetResponse).Thing
		thingResponse.ThingID = result.(*models.ThingGetResponse).ThingID
	}

	return err

	// return f.databaseConnector.GetThing(UUID, thingResponse)
}

// GetThings funciton
func (f *DataLoader) GetThings(ctx context.Context, UUIDs []strfmt.UUID, thingResponse *models.ThingsListResponse) error {
	defer f.messaging.TimeTrack(time.Now(), fmt.Sprintf("GetThings '%s'", UUIDs))
	return f.databaseConnector.GetThings(ctx, UUIDs, thingResponse)
}

// ListThings function
func (f *DataLoader) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.ListThings(first, offset, keyID, wheres, thingsResponse)
}

// UpdateThing function
func (f *DataLoader) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// f.thingsDataLoader.Clear(f.context, dataloader.StringKey(string(UUID)))

	return f.databaseConnector.UpdateThing(thing, UUID)
}

// DeleteThing function
func (f *DataLoader) DeleteThing(thing *models.Thing, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// f.thingsDataLoader.Clear(f.context, dataloader.StringKey(string(UUID)))

	return f.databaseConnector.DeleteThing(thing, UUID)
}

// HistoryThing fills the history of a thing based on its UUID
func (f *DataLoader) HistoryThing(UUID strfmt.UUID, history *models.ThingHistory) error {
	return f.databaseConnector.HistoryThing(UUID, history)
}

// MoveToHistoryThing moves a thing to history
func (f *DataLoader) MoveToHistoryThing(thing *models.Thing, UUID strfmt.UUID, deleted bool) error {
	return f.databaseConnector.MoveToHistoryThing(thing, UUID, deleted)
}

// AddAction function
func (f *DataLoader) AddAction(action *models.Action, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.AddAction(action, UUID)
}

// GetAction function
func (f *DataLoader) GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.GetAction(UUID, actionResponse)
}

// ListActions function
func (f *DataLoader) ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.ListActions(UUID, first, offset, wheres, actionsResponse)
}

// UpdateAction function
func (f *DataLoader) UpdateAction(action *models.Action, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// f.thingsDataLoader.Clear(f.context, dataloader.StringKey(string(action.Things.Subject.NrDollarCref)))
	// f.thingsDataLoader.Clear(f.context, dataloader.StringKey(string(action.Things.Object.NrDollarCref)))

	return f.databaseConnector.UpdateAction(action, UUID)
}

// DeleteAction function
func (f *DataLoader) DeleteAction(action *models.Action, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	// f.thingsDataLoader.Clear(f.context, dataloader.StringKey(string(action.Things.Subject.NrDollarCref)))
	// f.thingsDataLoader.Clear(f.context, dataloader.StringKey(string(action.Things.Object.NrDollarCref)))

	return f.databaseConnector.DeleteAction(action, UUID)
}

// HistoryAction fills the history of a Action based on its UUID
func (f *DataLoader) HistoryAction(UUID strfmt.UUID, history *models.ActionHistory) error {
	return f.databaseConnector.HistoryAction(UUID, history)
}

// MoveToHistoryAction moves a action to history
func (f *DataLoader) MoveToHistoryAction(action *models.Action, UUID strfmt.UUID, deleted bool) error {
	return f.databaseConnector.MoveToHistoryAction(action, UUID, deleted)
}

// AddKey function
func (f *DataLoader) AddKey(key *models.Key, UUID strfmt.UUID, token string) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.AddKey(key, UUID, token)
}

// ValidateToken function
func (f *DataLoader) ValidateToken(UUID strfmt.UUID, keyResponse *models.KeyGetResponse) (token string, err error) {
	defer f.messaging.TimeTrack(time.Now())

	token, err = f.databaseConnector.ValidateToken(UUID, keyResponse)

	return token, err
}

// GetKey function
func (f *DataLoader) GetKey(UUID strfmt.UUID, keyResponse *models.KeyGetResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.GetKey(UUID, keyResponse)
}

// DeleteKey function
func (f *DataLoader) DeleteKey(key *models.Key, UUID strfmt.UUID) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.DeleteKey(key, UUID)
}

// GetKeyChildren function
func (f *DataLoader) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyGetResponse) error {
	defer f.messaging.TimeTrack(time.Now())

	return f.databaseConnector.GetKeyChildren(UUID, children)
}

// UpdateKey updates the Key in the DB at the given UUID.
func (f *DataLoader) UpdateKey(key *models.Key, UUID strfmt.UUID, token string) error {
	return f.databaseConnector.UpdateKey(key, UUID, token)
}
