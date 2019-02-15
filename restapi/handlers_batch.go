/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package restapi

import (
	"context"
	"sync"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/lib/delayed_unlock"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	rest_api_utils "github.com/creativesoftwarefdn/weaviate/restapi/rest_api_utils"
	"github.com/creativesoftwarefdn/weaviate/validation"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

type preparedThing struct {
	err   error
	thing *models.Thing
	uuid  strfmt.UUID
}

var WeaviateBatchingThingsCreateHandler = operations.WeaviateBatchingThingsCreateHandlerFunc(func(params operations.WeaviateBatchingThingsCreateParams) middleware.Responder {
	defer messaging.TimeTrack(time.Now())

	dbLock, err := db.ConnectorLock()
	if err != nil {
		return operations.NewWeaviateBatchingThingsCreateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}
	requestLocks := rest_api_utils.RequestLocks{
		DBLock:      dbLock,
		DelayedLock: delayed_unlock.New(dbLock),
		DBConnector: dbLock.Connector(),
	}

	defer requestLocks.DelayedLock.Unlock()

	amount := len(params.Body.Things)
	errorResponse := &models.ErrorResponse{}

	if amount == 0 {
		return operations.NewWeaviateBatchingThingsCreateUnprocessableEntity().WithPayload(errorResponse)
	}

	isThingsCreate := true
	fieldsToKeep := determineResponseFields(params.Body.Fields, isThingsCreate)

	preparedThingsC := make(chan preparedThing, amount)

	wg := new(sync.WaitGroup)

	ctx := params.HTTPRequest.Context()
	// Generate a goroutine for each separate request
	for _, thing := range params.Body.Things {
		wg.Add(1)
		// go handleBatchedThingsCreateRequest(wg, ctx, batchedRequest, requestIndex, &requestResults, async, &requestLocks, fieldsToKeep)
		prepareThings(ctx, wg, thing, &preparedThingsC, fieldsToKeep, &requestLocks)
	}

	wg.Wait()
	close(preparedThingsC)

	batchedRequestResponse := make([]*models.ThingsGetResponse, amount)

	var errors []error
	var things []*models.Thing
	var uuids []strfmt.UUID

	for preparedThing := range preparedThingsC {
		if preparedThing.err != nil {
			errors = append(errors, preparedThing.err)
			continue
		}

		things = append(things, preparedThing.thing)
		uuids = append(uuids, preparedThing.uuid)
	}

	if len(things) > 0 {
		// we must have at least one thing that passed validation to talk to send to the connector
		err := requestLocks.DBConnector.AddThingsBatch(ctx, things, uuids)
		if err != nil {
			return operations.NewWeaviateBatchingThingsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	// TODO: actually take validation into account
	// This is just POC code for now to see what happens in a real life scenario
	for i := range batchedRequestResponse {
		batchedRequestResponse[i] = &models.ThingsGetResponse{Result: &models.ThingsGetResponseAO1Result{}}
	}

	return operations.NewWeaviateBatchingThingsCreateOK().WithPayload(batchedRequestResponse)
})

func prepareThings(ctx context.Context, wg *sync.WaitGroup, thingCreate *models.ThingCreate, resultsC *chan preparedThing,
	fieldsToKeep map[string]int, requestLocks *rest_api_utils.RequestLocks) {
	defer wg.Done()

	// Generate UUID for the new object
	uuid := connutils.GenerateUUID()

	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(requestLocks.DBLock.GetSchema())

	// Create Thing object
	thing := &models.Thing{}
	thing.AtContext = thingCreate.AtContext
	thing.LastUpdateTimeUnix = 0

	if _, ok := fieldsToKeep["@class"]; ok {
		thing.AtClass = thingCreate.AtClass
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		thing.Schema = thingCreate.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		thing.CreationTimeUnix = connutils.NowUnix()
	}

	err := validation.ValidateThingBody(ctx, thingCreate, databaseSchema, requestLocks.DBConnector,
		network, serverConfig)

	*resultsC <- preparedThing{
		uuid:  uuid,
		thing: thing,
		err:   err,
	}
}
