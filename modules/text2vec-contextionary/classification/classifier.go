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
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

type vectorizer interface {
	// MultiVectorForWord must keep order, if an item cannot be vectorized, the
	// element should be explicit nil, not skipped
	MultiVectorForWord(ctx context.Context, words []string) ([][]float32, error)
	VectorOnlyForCorpi(ctx context.Context, corpi []string, overrides map[string]string) ([]float32, error)
}

type Classifier struct {
	vectorizer vectorizer
}

func New(vectorizer vectorizer) modulecapabilities.Classifier {
	return &Classifier{vectorizer: vectorizer}
}

func (c *Classifier) Name() string {
	return "text2vec-contextionary-contextual"
}

func (c *Classifier) ClassifyFn(params modulecapabilities.ClassifyParams) (modulecapabilities.ClassifyItemFn, error) {
	if c.vectorizer == nil {
		return nil, errors.Errorf("cannot use text2vec-contextionary-contextual " +
			"without the respective module")
	}

	// 1. do preparation here once
	preparedContext, err := c.prepareContextualClassification(params.Schema, params.VectorRepo,
		params.Params, params.Filters, params.UnclassifiedItems)
	if err != nil {
		return nil, errors.Wrap(err, "prepare context for text2vec-contextionary-contextual classification")
	}

	// 2. use higher order function to inject preparation data so it is then present for each single run
	return c.makeClassifyItemContextual(params.Schema, preparedContext), nil
}

func (c *Classifier) ParseClassifierSettings(params *models.Classification) error {
	raw := params.Settings
	settings := &ParamsContextual{}
	if raw == nil {
		settings.SetDefaults()
		params.Settings = settings
		return nil
	}

	asMap, ok := raw.(map[string]interface{})
	if !ok {
		return errors.Errorf("settings must be an object got %T", raw)
	}

	v, err := c.extractNumberFromMap(asMap, "minimumUsableWords")
	if err != nil {
		return err
	}
	settings.MinimumUsableWords = v

	v, err = c.extractNumberFromMap(asMap, "informationGainCutoffPercentile")
	if err != nil {
		return err
	}
	settings.InformationGainCutoffPercentile = v

	v, err = c.extractNumberFromMap(asMap, "informationGainMaximumBoost")
	if err != nil {
		return err
	}
	settings.InformationGainMaximumBoost = v

	v, err = c.extractNumberFromMap(asMap, "tfidfCutoffPercentile")
	if err != nil {
		return err
	}
	settings.TfidfCutoffPercentile = v

	settings.SetDefaults()
	params.Settings = settings

	return nil
}

func (c *Classifier) extractNumberFromMap(in map[string]interface{}, field string) (*int32, error) {
	unparsed, present := in[field]
	if present {
		parsed, ok := unparsed.(json.Number)
		if !ok {
			return nil, errors.Errorf("settings.%s must be number, got %T",
				field, unparsed)
		}

		asInt64, err := parsed.Int64()
		if err != nil {
			return nil, errors.Wrapf(err, "settings.%s", field)
		}

		asInt32 := int32(asInt64)
		return &asInt32, nil
	}

	return nil, nil
}
