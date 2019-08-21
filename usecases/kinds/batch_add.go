//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds/validation"
)

// AddActions Class Instances in batch to the connected DB
func (b *BatchManager) AddActions(ctx context.Context, principal *models.Principal,
	classes []*models.Action, fields []*string) (BatchActions, error) {

	err := b.authorizer.Authorize(principal, "create", "batch/actions")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return b.addActions(ctx, principal, classes, fields)
}

func (b *BatchManager) addActions(ctx context.Context, principal *models.Principal,
	classes []*models.Action, fields []*string) (BatchActions, error) {

	if err := b.validateActionForm(classes); err != nil {
		return nil, NewErrInvalidUserInput("invalid param 'actions': %v", err)
	}

	batchActions := b.validateActionsConcurrently(ctx, principal, classes, fields)
	if !b.config.Config.EsvectorOnly {
		if err := b.repo.AddActionsBatch(ctx, batchActions); err != nil {
			return nil, NewErrInternal("could not add batch request to connector: %v", err)
		}
		return batchActions, nil
	}

	var (
		res BatchActions
		err error
	)
	if res, err = b.vectorRepo.BatchPutActions(ctx, batchActions); err != nil {
		return nil, NewErrInternal("batch actions: %#v", err)
	}

	return res, nil
}

func (b *BatchManager) validateActionForm(classes []*models.Action) error {
	if len(classes) == 0 {
		return fmt.Errorf("cannot be empty, need at least one action for batching")
	}

	return nil
}

func (b *BatchManager) validateActionsConcurrently(ctx context.Context, principal *models.Principal,
	classes []*models.Action, fields []*string) BatchActions {
	fieldsToKeep := determineResponseFields(fields)
	c := make(chan BatchAction, len(classes))

	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, action := range classes {
		wg.Add(1)
		b.validateAction(ctx, principal, wg, action, i, &c, fieldsToKeep)
	}

	wg.Wait()
	close(c)
	return actionsChanToSlice(c)
}

func (b *BatchManager) validateAction(ctx context.Context, principal *models.Principal,
	wg *sync.WaitGroup, concept *models.Action, originalIndex int, resultsC *chan BatchAction, fieldsToKeep map[string]int) {
	defer wg.Done()

	var (
		id strfmt.UUID
	)

	ec := &errorCompounder{}

	if concept.ID == "" {
		// Generate UUID for the new object
		uuid, err := generateUUID()
		id = uuid
		ec.add(err)
	} else {
		_, err := uuid.FromString(concept.ID.String())
		ec.add(err)
		id = concept.ID
	}

	// Validate schema given in body with the weaviate schema
	s, err := b.schemaManager.GetSchema(principal)
	ec.add(err)

	// Create Action object
	action := &models.Action{}
	action.LastUpdateTimeUnix = 0
	action.ID = id

	if _, ok := fieldsToKeep["class"]; ok {
		action.Class = concept.Class
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		action.Schema = concept.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		action.CreationTimeUnix = unixNow()
	}

	err = validation.New(s, b.exists, b.network, b.config).Action(ctx, action)
	ec.add(err)

	vector, err := b.vectorizer.Action(ctx, action)
	ec.add(err)

	*resultsC <- BatchAction{
		UUID:          id,
		Action:        action,
		Err:           ec.toError(),
		OriginalIndex: originalIndex,
		Vector:        vector,
	}
}

func (b *BatchManager) exists(ctx context.Context, k kind.Kind, id strfmt.UUID) (bool, error) {
	if !b.config.Config.EsvectorOnly {
		return b.repo.ClassExists(ctx, id)
	} else {
		switch k {
		case kind.Thing:
			res, err := b.vectorRepo.ThingByID(ctx, id, 0)
			return res != nil, err
		case kind.Action:
			res, err := b.vectorRepo.ActionByID(ctx, id, 0)
			return res != nil, err
		default:
			panic("impossible kind")
		}
	}
}

func actionsChanToSlice(c chan BatchAction) BatchActions {
	result := make([]BatchAction, len(c), len(c))
	for action := range c {
		result[action.OriginalIndex] = action
	}

	return result
}

// AddThings Class Instances in batch to the connected DB
func (b *BatchManager) AddThings(ctx context.Context, principal *models.Principal,
	classes []*models.Thing, fields []*string) (BatchThings, error) {
	err := b.authorizer.Authorize(principal, "create", "batch/things")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return b.addThings(ctx, principal, classes, fields)
}

func (b *BatchManager) addThings(ctx context.Context, principal *models.Principal,
	classes []*models.Thing, fields []*string) (BatchThings, error) {

	if err := b.validateThingForm(classes); err != nil {
		return nil, NewErrInvalidUserInput("invalid param 'things': %v", err)
	}

	batchThings := b.validateThingsConcurrently(ctx, principal, classes, fields)

	if !b.config.Config.EsvectorOnly {
		if err := b.repo.AddThingsBatch(ctx, batchThings); err != nil {
			return nil, NewErrInternal("could not add batch request to connector: %v", err)
		}
		return batchThings, nil
	}

	var (
		res BatchThings
		err error
	)
	if res, err = b.vectorRepo.BatchPutThings(ctx, batchThings); err != nil {
		return nil, NewErrInternal("batch things: %#v", err)
	}

	return res, nil
}

func (b *BatchManager) validateThingForm(classes []*models.Thing) error {
	if len(classes) == 0 {
		return fmt.Errorf("cannot be empty, need at least one thing for batching")
	}

	return nil
}

func (b *BatchManager) validateThingsConcurrently(ctx context.Context, principal *models.Principal,
	classes []*models.Thing, fields []*string) BatchThings {
	fieldsToKeep := determineResponseFields(fields)
	c := make(chan BatchThing, len(classes))

	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, thing := range classes {
		wg.Add(1)
		b.validateThing(ctx, principal, wg, thing, i, &c, fieldsToKeep)
	}

	wg.Wait()
	close(c)
	return thingsChanToSlice(c)
}

func (b *BatchManager) validateThing(ctx context.Context, principal *models.Principal,
	wg *sync.WaitGroup, concept *models.Thing, originalIndex int, resultsC *chan BatchThing, fieldsToKeep map[string]int) {
	defer wg.Done()

	var (
		id strfmt.UUID
	)

	ec := &errorCompounder{}

	if concept.ID == "" {
		// Generate UUID for the new object
		uuid, err := generateUUID()
		id = uuid
		ec.add(err)
	} else {
		_, err := uuid.FromString(concept.ID.String())
		ec.add(err)
		id = concept.ID
	}

	// Validate schema given in body with the weaviate schema
	s, err := b.schemaManager.GetSchema(principal)
	ec.add(err)

	// Create Thing object
	thing := &models.Thing{}
	thing.LastUpdateTimeUnix = 0

	if _, ok := fieldsToKeep["class"]; ok {
		thing.Class = concept.Class
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		thing.Schema = concept.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		thing.CreationTimeUnix = unixNow()
	}

	thing.ID = id

	err = validation.New(s, b.exists, b.network, b.config).Thing(ctx, thing)
	ec.add(err)

	vector, err := b.vectorizer.Thing(ctx, thing)
	ec.add(err)

	*resultsC <- BatchThing{
		UUID:          id,
		Thing:         thing,
		Err:           ec.toError(),
		OriginalIndex: originalIndex,
		Vector:        vector,
	}
}

func thingsChanToSlice(c chan BatchThing) BatchThings {
	result := make([]BatchThing, len(c), len(c))
	for thing := range c {
		result[thing.OriginalIndex] = thing
	}

	return result
}

type errorCompounder struct {
	errors []error
}

func (ec *errorCompounder) add(err error) {
	if err != nil {
		ec.errors = append(ec.errors, err)
	}
}

func (ec *errorCompounder) toError() error {
	if len(ec.errors) == 0 {
		return nil
	}

	var msg strings.Builder
	for i, err := range ec.errors {
		if i != 0 {
			msg.WriteString(", ")
		}

		msg.WriteString(err.Error())
	}

	return errors.New(msg.String())
}
