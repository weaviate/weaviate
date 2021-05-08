//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
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
		go b.validateObject(ctx, principal, wg, object, i, &c, fieldsToKeep)
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
		uid, err := generateUUID()
		id = uid
		ec.add(err)
	} else {
		if _, err := uuid.Parse(concept.ID.String()); err != nil {
			ec.add(err)
		}
		id = concept.ID
	}

	// Validate schema given in body with the weaviate schema
	s, err := b.schemaManager.GetSchema(principal)
	ec.add(err)

	// Create Action object
	object := &models.Object{}
	object.LastUpdateTimeUnix = 0
	object.ID = id
	object.Vector = concept.Vector

	if _, ok := fieldsToKeep["class"]; ok {
		object.Class = concept.Class
	}
	if _, ok := fieldsToKeep["properties"]; ok {
		object.Properties = concept.Properties
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		object.CreationTimeUnix = unixNow()
	}

	err = validation.New(s, b.exists, b.config).Object(ctx, object)
	ec.add(err)

	err = b.obtainVector(ctx, object, principal)
	ec.add(err)

	*resultsC <- BatchObject{
		UUID:          id,
		Object:        object,
		Err:           ec.toError(),
		OriginalIndex: originalIndex,
		Vector:        object.Vector,
	}
}

func (b *BatchManager) exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	res, err := b.vectorRepo.ObjectByID(ctx, id, traverser.SelectProperties{}, traverser.AdditionalProperties{})
	return res != nil, err
}

func (b *BatchManager) getVectorizerOfClass(className string,
	principal *models.Principal) (string, error) {
	s, err := b.schemaManager.GetSchema(principal)
	if err != nil {
		return "", err
	}

	class := s.FindClassByName(schema.ClassName(className))
	if class == nil {
		// this should be impossible by the time this method gets called, but let's
		// be 100% certain
		return "", errors.Errorf("class %s not present", className)
	}

	return class.Vectorizer, nil
}

// TODO: too much repitition between here and add.go. Vector obtainer should be
// outscoped to be its own type used by both
func (b *BatchManager) obtainVector(ctx context.Context, class *models.Object,
	principal *models.Principal) error {
	vectorizerName, err := b.getVectorizerOfClass(class.Class, principal)
	if err != nil {
		return err
	}

	if vectorizerName == config.VectorizerModuleNone {
		if err := b.validateVectorPresent(class); err != nil {
			return NewErrInvalidUserInput("%v", err)
		}
	} else {
		vectorizer, err := b.vectorizerProvider.Vectorizer(vectorizerName, class.Class)
		if err != nil {
			return err
		}

		if err := vectorizer.UpdateObject(ctx, class); err != nil {
			return NewErrInternal("%v", err)
		}
	}

	return nil
}

func (b *BatchManager) validateVectorPresent(class *models.Object) error {
	if len(class.Vector) == 0 {
		return errors.Errorf("this class is configured to use vectorizer 'none' " +
			"thus a vector must be present when importing, got: field 'vector' is empty " +
			"or contains a zero-length vector")
	}

	return nil
}

func objectsChanToSlice(c chan BatchObject) BatchObjects {
	result := make([]BatchObject, len(c))
	for object := range c {
		result[object.OriginalIndex] = object
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

	return errors.Errorf(msg.String())
}

func unixNow() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
