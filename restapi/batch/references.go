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
 */package batch

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/schema/crossref"
	"github.com/creativesoftwarefdn/weaviate/models"
	batchmodels "github.com/creativesoftwarefdn/weaviate/restapi/batch/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	rest_api_utils "github.com/creativesoftwarefdn/weaviate/restapi/rest_api_utils"
	"github.com/creativesoftwarefdn/weaviate/restapi/state"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	middleware "github.com/go-openapi/runtime/middleware"
)

type referencesRequest struct {
	*http.Request
	*state.State
	locks *rest_api_utils.RequestLocks
	log   *telemetry.RequestsLog
}

// References adds cross-references between classes in batch
func (b *Batch) References(params operations.WeaviateBatchingReferencesCreateParams, principal *models.Principal) middleware.Responder {
	defer b.appState.Messaging.TimeTrack(time.Now())

	r := newReferencesRequest(params.HTTPRequest, b.appState, b.requestsLog)
	if errResponder := r.lock(); errResponder != nil {
		return errResponder
	}
	defer r.unlock()

	if errResponder := r.validateForm(params); errResponder != nil {
		return errResponder
	}

	batchReferences := r.validateConcurrently(params.Body)
	if err := r.locks.DBConnector.AddBatchReferences(r.Context(), batchReferences); err != nil {
		return operations.NewWeaviateBatchingReferencesCreateInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	return operations.NewWeaviateBatchingReferencesCreateOK().
		WithPayload(batchReferences.Response())
}

func newReferencesRequest(r *http.Request, deps *state.State, log *telemetry.RequestsLog) *referencesRequest {
	return &referencesRequest{
		Request: r,
		State:   deps,
		log:     log,
	}
}

// a call to lock() should always be followed by a deferred call to unlock()
func (r *referencesRequest) lock() middleware.Responder {
	dbLock, err := r.Database.ConnectorLock()
	if err != nil {
		return operations.NewWeaviateBatchingReferencesCreateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}
	r.locks = &rest_api_utils.RequestLocks{
		DBLock:      dbLock,
		DBConnector: dbLock.Connector(),
	}

	return nil
}

func (r *referencesRequest) unlock() {
	r.locks.DBLock.Unlock()
}

func (r *referencesRequest) validateForm(params operations.WeaviateBatchingReferencesCreateParams) middleware.Responder {
	if len(params.Body) == 0 {
		err := fmt.Errorf("length cannot be 0, need at least one reference for batching")
		return operations.NewWeaviateBatchingReferencesCreateUnprocessableEntity().WithPayload(errPayloadFromSingleErr(err))
	}

	return nil
}

func (r *referencesRequest) validateConcurrently(refs []*models.BatchReference) batchmodels.References {
	c := make(chan batchmodels.Reference, len(refs))
	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, ref := range refs {
		wg.Add(1)
		go r.validateReference(wg, ref, i, &c)
	}

	wg.Wait()
	close(c)
	return referencesChanToSlice(c)
}

func (r *referencesRequest) validateReference(wg *sync.WaitGroup, ref *models.BatchReference,
	i int, resultsC *chan batchmodels.Reference) {
	defer wg.Done()
	var errors []error
	source, err := crossref.ParseSource(string(ref.From))
	if err != nil {
		errors = append(errors, err)
	} else if !source.Local {
		errors = append(errors, fmt.Errorf("source class must always point to the local peer, but got %s",
			source.PeerName))
	}

	target, err := crossref.Parse(string(ref.To))
	if err != nil {
		errors = append(errors, err)
	} else if !target.Local {
		errors = append(errors, fmt.Errorf("importing network references in batch is not possible. "+
			"Please perform a regular non-batch import for network references, got peer %s",
			target.PeerName))
	}

	if len(errors) == 0 {
		err = nil
	} else {
		err = joinErrors(errors)
	}

	if err == nil {
		// Register the request
		go func() {
			r.log.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()
	}

	*resultsC <- batchmodels.Reference{
		From:          source,
		To:            target,
		Err:           err,
		OriginalIndex: i,
	}
}

func referencesChanToSlice(c chan batchmodels.Reference) batchmodels.References {
	result := make([]batchmodels.Reference, len(c), len(c))
	for reference := range c {
		result[reference.OriginalIndex] = reference
	}

	return result
}

func joinErrors(errors []error) error {
	errorStrings := []string{}
	for _, err := range errors {
		if err != nil {
			errorStrings = append(errorStrings, err.Error())
		}
	}

	if len(errorStrings) == 0 {
		return nil
	}

	return fmt.Errorf(strings.Join(errorStrings, ", "))
}
