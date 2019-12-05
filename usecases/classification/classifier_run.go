//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package classification

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
)

// the contents of this file deal with anything about a classification run
// which is generic, whereas the individual classify_item fns can be found in
// the respective files such as classifier_run_knn.go

type classifyItemFn func(item search.Result, kind kind.Kind, params models.Classification) error

func (c *Classifier) run(params models.Classification, kind kind.Kind,
	filters filters) {
	ctx, cancel := contextWithTimeout(30 * time.Second)
	defer cancel()

	unclassifiedItems, err := c.vectorRepo.GetUnclassified(ctx,
		kind, params.Class, params.ClassifyProperties, filters.source)
	if err != nil {
		c.failRunWithError(params, err)
		return
	}

	if unclassifiedItems == nil || len(unclassifiedItems) == 0 {
		c.failRunWithError(params,
			fmt.Errorf("no classes to be classified - did you run a previous classification already?"))
		return
	}

	var (
		successCount int64
		errorCount   int64
	)

	var classifyItem classifyItemFn

	// safe to deref as we have passed validation at this point and or setting of
	// default values
	switch *params.Type {
	case "knn":
		classifyItem = c.classifyItemUsingKNN
	case "contextual":
		classifyItem = c.classifyItemContextual
	default:
		c.failRunWithError(params,
			fmt.Errorf("unsupported type '%s', have no classify item fn for this", *params.Type))
		return
	}

	errors := &errorCompounder{}
	for _, item := range unclassifiedItems {
		err := classifyItem(item, kind, params)
		if err != nil {
			errors.add(err)
			errorCount++
		} else {
			successCount++
		}

		time.Sleep(10 * time.Millisecond)

	}

	params.Meta.Completed = strfmt.DateTime(time.Now())
	params.Meta.CountSucceeded = successCount
	params.Meta.CountFailed = errorCount
	params.Meta.Count = successCount + errorCount

	err = errors.toError()
	if err != nil {
		c.failRunWithError(params, err)
		return
	}

	c.succeedRun(params)
}

func (c *Classifier) succeedRun(params models.Classification) {
	params.Status = models.ClassificationStatusCompleted
	ctx, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()
	err := c.repo.Put(ctx, params)
	if err != nil {
		// TODO: log

	}
}

func (c *Classifier) failRunWithError(params models.Classification, err error) {
	params.Status = models.ClassificationStatusFailed
	params.Error = fmt.Sprintf("classification failed: %v", err)
	err = c.repo.Put(context.Background(), params)
	if err != nil {
		// TODO: log

	}
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
	if item.Meta == nil {
		item.Meta = &models.ObjectMeta{}
	}

	item.Meta.Classification = &models.ObjectMetaClassification{
		ID:               params.ID,
		Scope:            params.ClassifyProperties,
		ClassifiedFields: classified,
		Completed:        strfmt.DateTime(time.Now()),
	}
}

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
