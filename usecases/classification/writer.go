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

package classification

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

type writer interface {
	Start()
	Store(item search.Result) error
	Stop() batchWriterResults
}

type batchWriter struct {
	mutex           sync.RWMutex
	vectorRepo      vectorRepo
	batchItemsCount int
	batchObjects    kinds.BatchObjects
	saveObjectItems chan kinds.BatchObjects
	errorCount      int64
	ec              *errorCompounder
	cancel          chan struct{}
	batchTreshold   int
}

type batchWriterResults struct {
	successCount int64
	errorCount   int64
	err          error
}

func newBatchWriter(vectorRepo vectorRepo) writer {
	return &batchWriter{
		vectorRepo:      vectorRepo,
		batchItemsCount: 0,
		batchObjects:    kinds.BatchObjects{},
		saveObjectItems: make(chan kinds.BatchObjects),
		errorCount:      0,
		ec:              &errorCompounder{},
		cancel:          make(chan struct{}),
		batchTreshold:   100,
	}
}

// Store puts an item to batch list
func (r *batchWriter) Store(item search.Result) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	switch item.Kind {
	case kind.Object:
		return r.storeObject(item)
	default:
		return errors.Errorf("impossible kind")
	}
}

// Start starts the batch save goroutine
func (r *batchWriter) Start() {
	go r.batchSave()
}

// Stop stops the batch save goroutine and saves the last items
func (r *batchWriter) Stop() batchWriterResults {
	r.cancel <- struct{}{}
	r.saveObjects(r.batchObjects)
	return batchWriterResults{int64(r.batchItemsCount) - r.errorCount, r.errorCount, r.ec.toError()}
}

func (r *batchWriter) storeObject(item search.Result) error {
	r.batchItemsCount++
	batchObject := kinds.BatchObject{
		UUID:          item.ID,
		Object:        item.Object(),
		OriginalIndex: r.batchItemsCount,
		Vector:        item.Vector,
	}
	r.batchObjects = append(r.batchObjects, batchObject)
	if len(r.batchObjects) >= r.batchTreshold {
		r.saveObjectItems <- r.batchObjects
		r.batchObjects = kinds.BatchObjects{}
	}
	return nil
}

// This goroutine is created in order to make possible the batch save operation to be run in background
// and not to block the Store(item) operation invocation which is being done by the worker threads
func (r *batchWriter) batchSave() {
	for {
		select {
		case <-r.cancel:
			return
		case items := <-r.saveObjectItems:
			r.saveObjects(items)
		}
	}
}

func (r *batchWriter) saveObjects(items kinds.BatchObjects) {
	// we need to allow quite some time as this is now a batch, no longer just a
	// single item and we don't have any control over what other load is
	// currently going on, such as imports. TODO: should this be
	// user-configurable?
	ctx, cancel := contextWithTimeout(30 * time.Second)
	defer cancel()

	if len(items) > 0 {
		saved, err := r.vectorRepo.BatchPutObjects(ctx, items)
		if err != nil {
			r.ec.add(err)
		}
		for i := range saved {
			if saved[i].Err != nil {
				r.ec.add(saved[i].Err)
				r.errorCount++
			}
		}
	}
}
