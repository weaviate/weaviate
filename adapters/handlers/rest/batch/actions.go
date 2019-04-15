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

	batchmodels "github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/batch/models"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/operations"
	rest_api_utils "github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/rest_api_utils"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/usecases/telemetry"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds/validation"
	middleware "github.com/go-openapi/runtime/middleware"
)

// a helper type that is created for each incoming request and spans the
// lifecycle of the request
type actionsRequest struct {
	*http.Request
	*state.State
	locks *rest_api_utils.RequestLocks
	log   *telemetry.RequestsLog
}

// ActionsCreate is the (go-swagger style) http handler for BatchingActionsCreate
func (b *Batch) ActionsCreate(params operations.WeaviateBatchingActionsCreateParams, principal *models.Principal) middleware.Responder {
	defer b.appState.Messaging.TimeTrack(time.Now())

	r := newActionsRequest(params.HTTPRequest, b.appState, b.requestsLog)
	if errResponder := r.lock(); errResponder != nil {
		return errResponder
	}
	defer r.unlock()

	if errResponder := r.validateForm(params); errResponder != nil {
		return errResponder
	}

	batchActions := r.validateConcurrently(params.Body)
	if err := r.locks.DBConnector.AddActionsBatch(r.Context(), batchActions); err != nil {
		return operations.NewWeaviateBatchingActionsCreateInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	return operations.NewWeaviateBatchingActionsCreateOK().
		WithPayload(batchActions.Response())
}

func newActionsRequest(r *http.Request, deps *state.State, log *telemetry.RequestsLog) *actionsRequest {
	return &actionsRequest{
		Request: r,
		State:   deps,
		log:     log,
	}
}

// a call to lock() should always be followed by a deferred call to unlock()
func (r *actionsRequest) lock() middleware.Responder {
	dbLock, err := r.Database.ConnectorLock()
	if err != nil {
		return operations.NewWeaviateBatchingActionsCreateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}
	r.locks = &rest_api_utils.RequestLocks{
		DBLock:      dbLock,
		DBConnector: dbLock.Connector(),
	}

	return nil
}

func (r *actionsRequest) unlock() {
	r.locks.DBLock.Unlock()
}

func (r *actionsRequest) validateForm(params operations.WeaviateBatchingActionsCreateParams) middleware.Responder {
	if len(params.Body.Actions) == 0 {
		err := fmt.Errorf("field 'actions' cannot be empty, need at least one action for batching")
		return operations.NewWeaviateBatchingActionsCreateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}

	return nil
}

func (r *actionsRequest) validateConcurrently(body operations.WeaviateBatchingActionsCreateBody) batchmodels.Actions {
	isThingsCreate := false
	fieldsToKeep := determineResponseFields(body.Fields, isThingsCreate)
	c := make(chan batchmodels.Action, len(body.Actions))

	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, action := range body.Actions {
		wg.Add(1)
		r.validateAction(wg, action, i, &c, fieldsToKeep)
	}

	wg.Wait()
	close(c)
	return actionsChanToSlice(c)
}

func (r *actionsRequest) validateAction(wg *sync.WaitGroup, actionCreate *models.Action,
	originalIndex int, resultsC *chan batchmodels.Action, fieldsToKeep map[string]int) {
	defer wg.Done()

	// Generate UUID for the new object
	uuid := connutils.GenerateUUID()

	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(r.locks.DBLock.GetSchema())

	// Create Action object
	action := &models.Action{}
	action.LastUpdateTimeUnix = 0

	if _, ok := fieldsToKeep["class"]; ok {
		action.Class = actionCreate.Class
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		action.Schema = actionCreate.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		action.CreationTimeUnix = connutils.NowUnix()
	}

	err := validation.ValidateActionBody(r.Context(), actionCreate, databaseSchema, r.locks.DBConnector,
		r.Network, r.ServerConfig)

	if err == nil {
		// Register the request
		go func() {
			r.log.Register(telemetry.TypeREST, telemetry.LocalAdd)
		}()
	}

	*resultsC <- batchmodels.Action{
		UUID:          uuid,
		Action:        action,
		Err:           err,
		OriginalIndex: originalIndex,
	}
}

func actionsChanToSlice(c chan batchmodels.Action) batchmodels.Actions {
	result := make([]batchmodels.Action, len(c), len(c))
	for action := range c {
		result[action.OriginalIndex] = action
	}

	return result
}
