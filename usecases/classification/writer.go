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
	batchThings     kinds.BatchThings
	batchActions    kinds.BatchActions
	saveThingItems  chan kinds.BatchThings
	saveActionItems chan kinds.BatchActions
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
		batchThings:     kinds.BatchThings{},
		batchActions:    kinds.BatchActions{},
		saveThingItems:  make(chan kinds.BatchThings, 1),
		saveActionItems: make(chan kinds.BatchActions, 1),
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
	case kind.Thing:
		return r.storeThing(item)
	case kind.Action:
		return r.storeAction(item)
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
	r.saveThings(r.batchThings)
	r.saveActions(r.batchActions)
	return batchWriterResults{int64(r.batchItemsCount) - r.errorCount, r.errorCount, r.ec.toError()}
}

func (r *batchWriter) storeThing(item search.Result) error {
	r.batchItemsCount++
	batchThing := kinds.BatchThing{
		UUID:          item.ID,
		Thing:         item.Thing(),
		OriginalIndex: r.batchItemsCount,
		Vector:        item.Vector,
	}
	r.batchThings = append(r.batchThings, batchThing)
	if len(r.batchThings) >= r.batchTreshold {
		r.saveThingItems <- r.batchThings
		r.batchThings = kinds.BatchThings{}
	}
	return nil
}

func (r *batchWriter) storeAction(item search.Result) error {
	r.batchItemsCount++
	batchAction := kinds.BatchAction{
		UUID:          item.ID,
		Action:        item.Action(),
		OriginalIndex: r.batchItemsCount,
		Vector:        item.Vector,
	}
	r.batchActions = append(r.batchActions, batchAction)
	if len(r.batchActions) >= r.batchTreshold {
		r.saveActionItems <- r.batchActions
		r.batchActions = kinds.BatchActions{}
	}
	return nil
}

func (r *batchWriter) batchSave() {
	for {
		select {
		case <-r.cancel:
			return
		case items := <-r.saveThingItems:
			r.saveThings(items)
		case items := <-r.saveActionItems:
			r.saveActions(items)
		}
	}
}

func (r *batchWriter) saveThings(items kinds.BatchThings) {
	ctx, cancel := contextWithTimeout(5 * time.Second)
	defer cancel()
	if len(items) > 0 {
		saved, err := r.vectorRepo.BatchPutThings(ctx, items)
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

func (r *batchWriter) saveActions(items kinds.BatchActions) {
	ctx, cancel := contextWithTimeout(5 * time.Second)
	defer cancel()
	if len(items) > 0 {
		saved, err := r.vectorRepo.BatchPutActions(ctx, items)
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
