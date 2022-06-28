//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
)

// AddObjects Class Instances in batch to the connected DB
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string) (BatchObjects, error) {
	err := b.authorizer.Authorize(principal, "create", "batch/objects")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	if b.metrics != nil {
		before := time.Now()
		defer func() {
			tookMs := time.Since(before) / time.Millisecond
			b.metrics.BatchTime.With(prometheus.Labels{
				"operation":  "total_uc_level",
				"class_name": "n/a",
				"shard_name": "n/a",
			}).Observe(float64(tookMs))
		}()
	}
	return b.addObjects(ctx, principal, objects, fields)
}

func (b *BatchManager) addObjects(ctx context.Context, principal *models.Principal,
	classes []*models.Object, fields []*string) (BatchObjects, error) {
	beforePreProcessing := time.Now()
	if err := b.validateObjectForm(classes); err != nil {
		return nil, NewErrInvalidUserInput("invalid param 'objects': %v", err)
	}

	batchObjects := b.validateObjectsConcurrently(ctx, principal, classes, fields)

	if b.metrics != nil {
		b.metrics.BatchTime.With(prometheus.Labels{
			"operation":  "total_preprocessing",
			"class_name": "n/a",
			"shard_name": "n/a",
		}).
			Observe(float64(time.Since(beforePreProcessing) / time.Millisecond))
	}

	var (
		res BatchObjects
		err error
	)

	if b.metrics != nil {
		beforePersistence := time.Now()
		defer func() {
			b.metrics.BatchTime.With(prometheus.Labels{
				"operation":  "total_persistence_level",
				"class_name": "n/a",
				"shard_name": "n/a",
			}).
				Observe(float64(time.Since(beforePersistence) / time.Millisecond))
		}()
	}
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
	wg *sync.WaitGroup, concept *models.Object, originalIndex int, resultsC *chan BatchObject,
	fieldsToKeep map[string]struct{}) {
	defer wg.Done()

	var id strfmt.UUID

	ec := &errorCompounder{}

	// Auto Schema
	err := b.autoSchemaManager.autoSchema(ctx, principal, concept)
	ec.add(err)

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

	now := unixNow()
	if _, ok := fieldsToKeep["creationTimeUnix"]; ok {
		object.CreationTimeUnix = now
	}
	if _, ok := fieldsToKeep["lastUpdateTimeUnix"]; ok {
		object.LastUpdateTimeUnix = now
	}

	err = validation.New(s, b.vectorRepo.Exists, b.config).Object(ctx, object)
	ec.add(err)

	err = newVectorObtainer(b.vectorizerProvider, b.schemaManager,
		b.logger).Do(ctx, object, principal)
	ec.add(err)

	*resultsC <- BatchObject{
		UUID:          id,
		Object:        object,
		Err:           ec.toError(),
		OriginalIndex: originalIndex,
		Vector:        object.Vector,
	}
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
