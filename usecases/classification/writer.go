//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package classification

import (
	"sync"
	"time"

	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/objects"
)

type batchWriterResults struct {
	successCount int64
	errorCount   int64
	err          error
}

func (w batchWriterResults) SuccessCount() int64 {
	return w.successCount
}

func (w batchWriterResults) ErrorCount() int64 {
	return w.errorCount
}

func (w batchWriterResults) Err() error {
	return w.err
}

type batchWriter struct {
	mutex           sync.RWMutex
	vectorRepo      vectorRepo
	batchItemsCount int
	batchIndex      int
	batchObjects    objects.BatchObjects
	saveObjectItems chan objects.BatchObjects
	errorCount      int64
	ec              *errorcompounder.SafeErrorCompounder
	cancel          chan struct{}
	batchThreshold  int
}

func newBatchWriter(vectorRepo vectorRepo) Writer {
	return &batchWriter{
		vectorRepo:      vectorRepo,
		batchItemsCount: 0,
		batchObjects:    objects.BatchObjects{},
		saveObjectItems: make(chan objects.BatchObjects),
		errorCount:      0,
		ec:              &errorcompounder.SafeErrorCompounder{},
		cancel:          make(chan struct{}),
		batchThreshold:  100,
	}
}

// Store puts an item to batch list
func (r *batchWriter) Store(item search.Result) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.storeObject(item)
}

// Start starts the batch save goroutine
func (r *batchWriter) Start() {
	go r.batchSave()
}

// Stop stops the batch save goroutine and saves the last items
func (r *batchWriter) Stop() WriterResults {
	r.cancel <- struct{}{}
	r.saveObjects(r.batchObjects)
	return batchWriterResults{int64(r.batchItemsCount) - r.errorCount, r.errorCount, r.ec.ToError()}
}

func (r *batchWriter) storeObject(item search.Result) error {
	batchObject := objects.BatchObject{
		UUID:          item.ID,
		Object:        item.Object(),
		OriginalIndex: r.batchIndex,
		Vector:        item.Vector,
	}
	r.batchItemsCount++
	r.batchIndex++
	r.batchObjects = append(r.batchObjects, batchObject)
	if len(r.batchObjects) >= r.batchThreshold {
		r.saveObjectItems <- r.batchObjects
		r.batchObjects = objects.BatchObjects{}
		r.batchIndex = 0
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

func (r *batchWriter) saveObjects(items objects.BatchObjects) {
	// we need to allow quite some time as this is now a batch, no longer just a
	// single item and we don't have any control over what other load is
	// currently going on, such as imports. TODO: should this be
	// user-configurable?
	ctx, cancel := contextWithTimeout(30 * time.Second)
	defer cancel()

	if len(items) > 0 {
		saved, err := r.vectorRepo.BatchPutObjects(ctx, items, nil)
		if err != nil {
			r.ec.Add(err)
		}
		for i := range saved {
			if saved[i].Err != nil {
				r.ec.Add(saved[i].Err)
				r.errorCount++
			}
		}
	}
}
