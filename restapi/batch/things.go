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
 */

package batch

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	batchmodels "github.com/creativesoftwarefdn/weaviate/restapi/batch/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	rest_api_utils "github.com/creativesoftwarefdn/weaviate/restapi/rest_api_utils"
	"github.com/creativesoftwarefdn/weaviate/restapi/state"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/creativesoftwarefdn/weaviate/validation"
	middleware "github.com/go-openapi/runtime/middleware"
)

// a helper type that is created for each incoming request and spans the
// lifecycle of the request
type thingsRequest struct {
	*http.Request
	*state.State
	locks *rest_api_utils.RequestLocks
	log   *telemetry.RequestsLog
}

// ThingsCreate is the (go-swagger style) http handler for BatchingThingsCreate
func (b *Batch) ThingsCreate(params operations.WeaviateBatchingThingsCreateParams) middleware.Responder {
	defer b.appState.Messaging.TimeTrack(time.Now())

	r := newThingsRequest(params.HTTPRequest, b.appState, b.requestsLog)
	if errResponder := r.lock(); errResponder != nil {
		return errResponder
	}
	defer r.unlock()

	if errResponder := r.validateForm(params); errResponder != nil {
		return errResponder
	}

	batchThings := r.validateConcurrently(params.Body)
	if err := r.locks.DBConnector.AddThingsBatch(r.Context(), batchThings); err != nil {
		return operations.NewWeaviateBatchingThingsCreateInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	return operations.NewWeaviateBatchingThingsCreateOK().
		WithPayload(batchThings.Response())
}

func newThingsRequest(r *http.Request, deps *state.State, requestsLog *telemetry.RequestLog) *thingsRequest {
	return &thingsRequest{
		Request: r,
		State:   deps,
		log:     requestsLog,
	}
}

// a call to lock() should always be followed by a deferred call to unlock()
func (r *thingsRequest) lock() middleware.Responder {
	dbLock, err := r.Database.ConnectorLock()
	if err != nil {
		return operations.NewWeaviateBatchingThingsCreateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}
	r.locks = &rest_api_utils.RequestLocks{
		DBLock:      dbLock,
		DBConnector: dbLock.Connector(),
	}

	return nil
}

func (r *thingsRequest) unlock() {
	r.locks.DBLock.Unlock()
}

func (r *thingsRequest) validateForm(params operations.WeaviateBatchingThingsCreateParams) middleware.Responder {
	if len(params.Body.Things) == 0 {
		err := fmt.Errorf("field 'things' cannot be empty, need at least one thing for batching")
		return operations.NewWeaviateBatchingThingsCreateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}

	return nil
}

func (r *thingsRequest) validateConcurrently(body operations.WeaviateBatchingThingsCreateBody) batchmodels.Things {
	isThingsCreate := true
	fieldsToKeep := determineResponseFields(body.Fields, isThingsCreate)
	c := make(chan batchmodels.Thing, len(body.Things))

	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, thing := range body.Things {
		wg.Add(1)
		r.validateThing(wg, thing, i, &c, fieldsToKeep)
	}

	wg.Wait()
	close(c)
	return thingsChanToSlice(c)
}

func (r *thingsRequest) validateThing(wg *sync.WaitGroup, thingCreate *models.ThingCreate,
	originalIndex int, resultsC *chan batchmodels.Thing, fieldsToKeep map[string]int) {
	defer wg.Done()

	// Generate UUID for the new object
	uuid := connutils.GenerateUUID()

	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(r.locks.DBLock.GetSchema())

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

	err := validation.ValidateThingBody(r.Context(), thingCreate, databaseSchema, r.locks.DBConnector,
		r.Network, r.ServerConfig)

	if err == nil {
		// Register the request
		go func() {
			requestslog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalAdd))
		}()
	}

	*resultsC <- batchmodels.Thing{
		UUID:          uuid,
		Thing:         thing,
		Err:           err,
		OriginalIndex: originalIndex,
	}
}

func thingsChanToSlice(c chan batchmodels.Thing) batchmodels.Things {
	result := make([]batchmodels.Thing, len(c), len(c))
	for thing := range c {
		result[thing.OriginalIndex] = thing
	}

	return result
}
