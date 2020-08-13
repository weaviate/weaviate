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
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/sirupsen/logrus"
)

// the contents of this file deal with anything about a classification run
// which is generic, whereas the individual classify_item fns can be found in
// the respective files such as classifier_run_knn.go

type classifyItemFn func(item search.Result, itemIndex int, kind kind.Kind,
	params models.Classification, filters filters) error

func (c *Classifier) run(params models.Classification, kind kind.Kind,
	filters filters) {
	ctx, cancel := contextWithTimeout(30 * time.Second)
	defer cancel()

	c.logBegin(params, filters)
	unclassifiedItems, err := c.vectorRepo.GetUnclassified(ctx,
		kind, params.Class, params.ClassifyProperties, filters.source)
	if err != nil {
		c.failRunWithError(params, errors.Wrap(err, "retrieve to-be-classifieds"))
		return
	}

	if unclassifiedItems == nil || len(unclassifiedItems) == 0 {
		c.failRunWithError(params,
			fmt.Errorf("no classes to be classified - did you run a previous classification already?"))
		return
	}
	c.logItemsFetched(params, unclassifiedItems)

	classifyItem, err := c.prepareRun(kind, params, filters, unclassifiedItems)
	if err != nil {
		c.failRunWithError(params, errors.Wrap(err, "prepare classification"))
		return
	}

	params, err = c.runItems(classifyItem, kind, params, filters, unclassifiedItems)
	if err != nil {
		c.failRunWithError(params, err)
		return
	}

	c.succeedRun(params)
}

func (c *Classifier) prepareRun(kind kind.Kind, params models.Classification, filters filters,
	unclassifiedItems []search.Result) (classifyItemFn, error) {
	var classifyItem classifyItemFn
	c.logBeginPreparation(params)
	// safe to deref as we have passed validation at this point and or setting of
	// default values
	switch *params.Type {
	case "knn":
		classifyItem = c.classifyItemUsingKNN
	case "contextual":
		// 1. do preparation here once
		preparedContext, err := c.prepareContextualClassification(kind, params, filters, unclassifiedItems)
		if err != nil {
			return nil, errors.Wrap(err, "prepare context for contexual classification")
		}

		// 2. use higher order function to inject preparation data so it is then present for each single run
		classifyItem = c.makeClassifyItemContextual(preparedContext)
	default:
		return nil, fmt.Errorf("unsupported type '%s', have no classify item fn for this", *params.Type)
	}

	c.logFinishPreparation(params)
	return classifyItem, nil
}

// runItems splits the job list into batches that can be worked on parallely
// depending on the available CPUs
func (c *Classifier) runItems(classifyItem classifyItemFn, kind kind.Kind, params models.Classification, filters filters,
	items []search.Result) (models.Classification, error) {

	workerCount := runtime.GOMAXPROCS(0)
	if len(items) < workerCount {
		workerCount = len(items)
	}

	workers := newRunWorkers(workerCount, classifyItem, kind, params, filters)
	workers.addJobs(items)
	res := workers.work()

	params.Meta.Completed = strfmt.DateTime(time.Now())
	params.Meta.CountSucceeded = res.successCount
	params.Meta.CountFailed = res.errorCount
	params.Meta.Count = res.successCount + res.errorCount

	return params, res.err
}

func (c *Classifier) succeedRun(params models.Classification) {
	params.Status = models.ClassificationStatusCompleted
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()
	err := c.repo.Put(ctx, params)
	if err != nil {
		c.logExecutionError("store succeded run", err, params)
	}
	c.logFinish(params)
}

func (c *Classifier) failRunWithError(params models.Classification, err error) {
	params.Status = models.ClassificationStatusFailed
	params.Error = fmt.Sprintf("classification failed: %v", err)
	err = c.repo.Put(context.Background(), params)
	if err != nil {
		c.logExecutionError("store failed run", err, params)

	}
	c.logFinish(params)
}

func (c *Classifier) store(item search.Result) error {
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()
	switch item.Kind {
	case kind.Thing:
		return c.vectorRepo.PutThing(ctx, item.Thing(), item.Vector)
	case kind.Action:
		return c.vectorRepo.PutAction(ctx, item.Action(), item.Vector)
	default:
		return fmt.Errorf("impossible kind")
	}
}

func (c *Classifier) extendItemWithObjectMeta(item *search.Result,
	params models.Classification, classified []string) {
	// don't overwrite existing non-classification meta info
	if item.UnderscoreProperties == nil {
		item.UnderscoreProperties = &models.UnderscoreProperties{}
	}

	item.UnderscoreProperties.Classification = &models.UnderscorePropertiesClassification{
		ID:               params.ID,
		Scope:            params.ClassifyProperties,
		ClassifiedFields: classified,
		Completed:        strfmt.DateTime(time.Now()),
	}
}

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// Logging helper methods
func (c *Classifier) logBase(params models.Classification, event string) *logrus.Entry {
	return c.logger.WithField("action", "classification_run").
		WithField("event", event).
		WithField("params", params).
		WithField("classification_type", params.Type)
}

func (c *Classifier) logBegin(params models.Classification, filters filters) {
	c.logBase(params, "classification_begin").
		WithField("filters", filters).
		Debug("classification started")
}

func (c *Classifier) logFinish(params models.Classification) {
	c.logBase(params, "classification_finish").
		WithField("status", params.Status).
		Debug("classification finished")
}

func (c *Classifier) logItemsFetched(params models.Classification, items search.Results) {
	c.logBase(params, "classification_items_fetched").
		WithField("status", params.Status).
		WithField("item_count", len(items)).
		Debug("fetched source items")
}

func (c *Classifier) logBeginClassifyItem(params models.Classification, item search.Result) {
	c.logBase(params, "classification_item_begin").
		WithField("uuid", item.ID).
		Debug("begin classifiy item")
}

func (c *Classifier) logFinishClassifyItem(params models.Classification, item search.Result) {
	c.logBase(params, "classification_item_finish").
		WithField("uuid", item.ID).
		Debug("finish classifiy item")
}

func (c *Classifier) logBeginPreparation(params models.Classification) {
	c.logBase(params, "classification_preparation_begin").
		Debug("begin run preparation")
}

func (c *Classifier) logFinishPreparation(params models.Classification) {
	c.logBase(params, "classification_preparation_finish").
		Debug("finish run preparation")
}

func (c *Classifier) logExecutionError(event string, err error, params models.Classification) {
	c.logBase(params, event).
		WithError(err).
		Error("classification execution failure")
}
