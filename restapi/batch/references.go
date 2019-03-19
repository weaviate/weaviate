package batch

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
	"github.com/creativesoftwarefdn/weaviate/validation"
	middleware "github.com/go-openapi/runtime/middleware"
)

type referencesRequest struct {
	*http.Request
	*state.State
	locks *rest_api_utils.RequestLocks
}

// References adds cross-references between classes in batch
func (b *Batch) References(params operations.WeaviateBatchingReferencesCreateParams) middleware.Responder {
	defer b.appState.Messaging.TimeTrack(time.Now())

	r := newReferencesRequest(params.HTTPRequest, b.appState)
	if errResponder := r.lock(); errResponder != nil {
		return errResponder
	}
	defer r.unlock()

	if errResponder := r.validateForm(params); errResponder != nil {
		return errResponder
	}

	batchReferences := r.validateConcurrently(params.Body)

	// TODO: call connector

	return operations.NewWeaviateBatchingReferencesCreateOK().
		WithPayload(batchReferences.Response())
}

func newReferencesRequest(r *http.Request, deps *state.State) *referencesRequest {
	return &referencesRequest{
		Request: r,
		State:   deps,
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
	errors = append(errors, err)

	target, err := crossref.Parse(string(ref.To))
	errors = append(errors, err)

	err = validation.ValidateSingleRef(r.Context(), r.State.ServerConfig, target.SingleRef(),
		r.locks.DBConnector, r.State.Network, "target ref")
	errors = append(errors, err)

	if len(errors) == 0 {
		err = nil
	} else {
		err = joinErrors(errors)
	}

	*resultsC <- batchmodels.Reference{
		From: source,
		To:   target,
		Err:  err,
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
