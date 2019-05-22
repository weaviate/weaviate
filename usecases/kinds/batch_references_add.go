/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
)

// AddReferences Class Instances in batch to the connected DB
func (b *BatchManager) AddReferences(ctx context.Context, principal *models.Principal,
	refs []*models.BatchReference) (BatchReferences, error) {

	err := b.authorizer.Authorize(principal, "update", "batch/*")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return b.addReferences(ctx, refs)
}

func (b *BatchManager) addReferences(ctx context.Context, refs []*models.BatchReference) (BatchReferences, error) {

	if err := b.validateReferenceForm(refs); err != nil {
		return nil, NewErrInvalidUserInput("invalid params: %v", err)
	}

	batchReferences := b.validateReferencesConcurrently(refs)
	if err := b.repo.AddBatchReferences(ctx, batchReferences); err != nil {
		return nil, NewErrInternal("could not add batch request to connector: %v", err)
	}

	return batchReferences, nil
}

func (b *BatchManager) validateReferenceForm(refs []*models.BatchReference) error {
	if len(refs) == 0 {
		return fmt.Errorf("length cannot be 0, need at least one reference for batching")
	}

	return nil
}

func (b *BatchManager) validateReferencesConcurrently(refs []*models.BatchReference) BatchReferences {
	c := make(chan BatchReference, len(refs))
	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, ref := range refs {
		wg.Add(1)
		go b.validateReference(wg, ref, i, &c)
	}

	wg.Wait()
	close(c)
	return referencesChanToSlice(c)
}

func (b *BatchManager) validateReference(wg *sync.WaitGroup, ref *models.BatchReference,
	i int, resultsC *chan BatchReference) {
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

	*resultsC <- BatchReference{
		From:          source,
		To:            target,
		Err:           err,
		OriginalIndex: i,
	}
}

func referencesChanToSlice(c chan BatchReference) BatchReferences {
	result := make([]BatchReference, len(c), len(c))
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
