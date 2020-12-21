//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds/validation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// AddObjects Class Instances in batch to the connected DB
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	classes []*models.Object, fields []*string) (BatchObjects, error) {
	err := b.authorizer.Authorize(principal, "create", "batch/objects")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return b.addObjects(ctx, principal, classes, fields)
}

func (b *BatchManager) addObjects(ctx context.Context, principal *models.Principal,
	classes []*models.Object, fields []*string) (BatchObjects, error) {
	if err := b.validateObjectForm(classes); err != nil {
		return nil, NewErrInvalidUserInput("invalid param 'objects': %v", err)
	}

	batchObjects := b.validateObjectsConcurrently(ctx, principal, classes, fields)

	var (
		res BatchObjects
		err error
	)
	if res, err = b.vectorRepo.BatchPutObjects(ctx, batchObjects); err != nil {
		return nil, NewErrInternal("batch objects: %#v", err)
	}

	return res, nil
}

func (b *BatchManager) validateObjectForm(classes []*models.Object) error {
	if len(classes) == 0 {
		return fmt.Errorf("cannot be empty, need at least one object for batching")
	}

	return nil
}

func (b *BatchManager) validateObjectsConcurrently(ctx context.Context, principal *models.Principal,
	classes []*models.Object, fields []*string) BatchObjects {
	fieldsToKeep := determineResponseFields(fields)
	c := make(chan BatchObject, len(classes))

	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, object := range classes {
		wg.Add(1)
		b.validateObject(ctx, principal, wg, object, i, &c, fieldsToKeep)
	}

	wg.Wait()
	close(c)
	return objectsChanToSlice(c)
}

func (b *BatchManager) validateObject(ctx context.Context, principal *models.Principal,
	wg *sync.WaitGroup, concept *models.Object, originalIndex int, resultsC *chan BatchObject, fieldsToKeep map[string]int) {
	defer wg.Done()

	var id strfmt.UUID

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
	object := &models.Object{}
	object.LastUpdateTimeUnix = 0
	object.ID = id

	if _, ok := fieldsToKeep["class"]; ok {
		object.Class = concept.Class
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		object.Schema = concept.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		object.CreationTimeUnix = unixNow()
	}

	err = validation.New(s, b.exists, b.config).Object(ctx, object)
	ec.add(err)

	vector, source, err := b.vectorizer.Object(ctx, object)
	ec.add(err)

	object.Interpretation = &models.Interpretation{
		Source: sourceFromInputElements(source),
	}

	*resultsC <- BatchObject{
		UUID:          id,
		Object:        object,
		Err:           ec.toError(),
		OriginalIndex: originalIndex,
		Vector:        vector,
	}
}

func (b *BatchManager) exists(ctx context.Context, k kind.Kind, id strfmt.UUID) (bool, error) {
	res, err := b.vectorRepo.ObjectByID(ctx, id, traverser.SelectProperties{}, traverser.UnderscoreProperties{})
	return res != nil, err
}

func objectsChanToSlice(c chan BatchObject) BatchObjects {
	result := make([]BatchObject, len(c))
	for object := range c {
		result[object.OriginalIndex] = object
	}

	return result
}

// AddThings Class Instances in batch to the connected DB
// func (b *BatchManager) AddThings(ctx context.Context, principal *models.Principal,
// 	classes []*models.Thing, fields []*string) (BatchThings, error) {
// 	err := b.authorizer.Authorize(principal, "create", "batch/things")
// 	if err != nil {
// 		return nil, err
// 	}

// 	unlock, err := b.locks.LockConnector()
// 	if err != nil {
// 		return nil, NewErrInternal("could not acquire lock: %v", err)
// 	}
// 	defer unlock()

// 	return b.addThings(ctx, principal, classes, fields)
// }

// func (b *BatchManager) addThings(ctx context.Context, principal *models.Principal,
// 	classes []*models.Thing, fields []*string) (BatchThings, error) {
// 	if err := b.validateThingForm(classes); err != nil {
// 		return nil, NewErrInvalidUserInput("invalid param 'things': %v", err)
// 	}

// 	batchThings := b.validateThingsConcurrently(ctx, principal, classes, fields)

// 	var (
// 		res BatchThings
// 		err error
// 	)
// 	if res, err = b.vectorRepo.BatchPutThings(ctx, batchThings); err != nil {
// 		return nil, NewErrInternal("batch things: %#v", err)
// 	}

// 	return res, nil
// }

// func (b *BatchManager) validateThingForm(classes []*models.Thing) error {
// 	if len(classes) == 0 {
// 		return fmt.Errorf("cannot be empty, need at least one thing for batching")
// 	}

// 	return nil
// }

// func (b *BatchManager) validateThingsConcurrently(ctx context.Context, principal *models.Principal,
// 	classes []*models.Thing, fields []*string) BatchThings {
// 	fieldsToKeep := determineResponseFields(fields)
// 	c := make(chan BatchThing, len(classes))

// 	wg := new(sync.WaitGroup)

// 	// Generate a goroutine for each separate request
// 	for i, thing := range classes {
// 		wg.Add(1)
// 		b.validateThing(ctx, principal, wg, thing, i, &c, fieldsToKeep)
// 	}

// 	wg.Wait()
// 	close(c)
// 	return thingsChanToSlice(c)
// }

// func (b *BatchManager) validateThing(ctx context.Context, principal *models.Principal,
// 	wg *sync.WaitGroup, concept *models.Thing, originalIndex int, resultsC *chan BatchThing, fieldsToKeep map[string]int) {
// 	defer wg.Done()

// 	var id strfmt.UUID

// 	ec := &errorCompounder{}

// 	if concept.ID == "" {
// 		// Generate UUID for the new object
// 		uuid, err := generateUUID()
// 		id = uuid
// 		ec.add(err)
// 	} else {
// 		_, err := uuid.FromString(concept.ID.String())
// 		ec.add(err)
// 		id = concept.ID
// 	}

// 	// Validate schema given in body with the weaviate schema
// 	s, err := b.schemaManager.GetSchema(principal)
// 	ec.add(err)

// 	// Create Thing object
// 	thing := &models.Thing{}
// 	thing.LastUpdateTimeUnix = 0

// 	if _, ok := fieldsToKeep["class"]; ok {
// 		thing.Class = concept.Class
// 	}
// 	if _, ok := fieldsToKeep["schema"]; ok {
// 		thing.Schema = concept.Schema
// 	}
// 	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
// 		thing.CreationTimeUnix = unixNow()
// 	}

// 	thing.ID = id

// 	err = validation.New(s, b.exists, b.config).Thing(ctx, thing)
// 	ec.add(err)

// 	vector, source, err := b.vectorizer.Thing(ctx, thing)
// 	ec.add(err)

// 	thing.Interpretation = &models.Interpretation{
// 		Source: sourceFromInputElements(source),
// 	}

// 	*resultsC <- BatchThing{
// 		UUID:          id,
// 		Thing:         thing,
// 		Err:           ec.toError(),
// 		OriginalIndex: originalIndex,
// 		Vector:        vector,
// 	}
// }

// func thingsChanToSlice(c chan BatchThing) BatchThings {
// 	result := make([]BatchThing, len(c))
// 	for thing := range c {
// 		result[thing.OriginalIndex] = thing
// 	}

// 	return result
// }

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

func unixNow() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
