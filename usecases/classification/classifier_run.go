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
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

// the contents of this file deal with anything about a classification run
// which is generic, whereas the individual classify_item fns can be found in
// the respective files such as classifier_run_knn.go

func (c *Classifier) run(params models.Classification,
	filters Filters,
) {
	ctx, cancel := contextWithTimeout(30 * time.Minute)
	defer cancel()

	go c.monitorClassification(ctx, cancel, schema.ClassName(params.Class))

	c.logBegin(params, filters)
	unclassifiedItems, err := c.vectorRepo.GetUnclassified(ctx,
		params.Class, params.ClassifyProperties, filters.Source())
	if err != nil {
		c.failRunWithError(params, errors.Wrap(err, "retrieve to-be-classifieds"))
		return
	}

	if len(unclassifiedItems) == 0 {
		c.failRunWithError(params,
			fmt.Errorf("no classes to be classified - did you run a previous classification already?"))
		return
	}
	c.logItemsFetched(params, unclassifiedItems)

	classifyItem, err := c.prepareRun(params, filters, unclassifiedItems)
	if err != nil {
		c.failRunWithError(params, errors.Wrap(err, "prepare classification"))
		return
	}

	params, err = c.runItems(ctx, classifyItem, params, filters, unclassifiedItems)
	if err != nil {
		c.failRunWithError(params, err)
		return
	}

	c.succeedRun(params)
}

func (c *Classifier) monitorClassification(ctx context.Context, cancelFn context.CancelFunc,
	className schema.ClassName,
) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			schema := c.schemaGetter.GetSchemaSkipAuth()
			class := schema.FindClassByName(className)
			if class == nil {
				cancelFn()
				return
			}
		}
	}
}

func (c *Classifier) prepareRun(params models.Classification, filters Filters,
	unclassifiedItems []search.Result,
) (ClassifyItemFn, error) {
	c.logBeginPreparation(params)
	defer c.logFinishPreparation(params)

	if params.Type == "knn" {
		return c.classifyItemUsingKNN, nil
	}

	if params.Type == "zeroshot" {
		return c.classifyItemUsingZeroShot, nil
	}

	if c.modulesProvider != nil {
		classifyItemFn, err := c.modulesProvider.GetClassificationFn(params.Class, params.Type,
			c.getClassifyParams(params, filters, unclassifiedItems))
		if err != nil {
			return nil, errors.Wrapf(err, "cannot classify")
		}
		if classifyItemFn == nil {
			return nil, errors.Errorf("cannot classify: empty classifier for %s", params.Type)
		}
		classification := &moduleClassification{classifyItemFn}
		return classification.classifyFn, nil
	}

	return nil, errors.Errorf("unsupported type '%s', have no classify item fn for this", params.Type)
}

func (c *Classifier) getClassifyParams(params models.Classification,
	filters Filters, unclassifiedItems []search.Result,
) modulecapabilities.ClassifyParams {
	return modulecapabilities.ClassifyParams{
		Schema:            c.schemaGetter.GetSchemaSkipAuth(),
		Params:            params,
		Filters:           filters,
		UnclassifiedItems: unclassifiedItems,
		VectorRepo:        c.vectorClassSearchRepo,
	}
}

// runItems splits the job list into batches that can be worked on parallelly
// depending on the available CPUs
func (c *Classifier) runItems(ctx context.Context, classifyItem ClassifyItemFn, params models.Classification, filters Filters,
	items []search.Result,
) (models.Classification, error) {
	workerCount := runtime.GOMAXPROCS(0)
	if len(items) < workerCount {
		workerCount = len(items)
	}

	workers := newRunWorkers(workerCount, classifyItem, params, filters, c.vectorRepo)
	workers.addJobs(items)
	res := workers.work(ctx)

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
		c.logExecutionError("store succeeded run", err, params)
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

func (c *Classifier) extendItemWithObjectMeta(item *search.Result,
	params models.Classification, classified []string,
) {
	// don't overwrite existing non-classification meta info
	if item.AdditionalProperties == nil {
		item.AdditionalProperties = models.AdditionalProperties{}
	}

	item.AdditionalProperties["classification"] = additional.Classification{
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

func (c *Classifier) logBegin(params models.Classification, filters Filters) {
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
