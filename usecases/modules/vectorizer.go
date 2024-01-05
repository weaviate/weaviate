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

package modules

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	errorVectorizerCapability = "module %q exists, but does not provide the " +
		"Vectorizer or ReferenceVectorizer capability"

	errorVectorIndexType = "vector index config (%T) is not of type HNSW, " +
		"but objects manager is restricted to HNSW"

	warningVectorIgnored = "This vector will be ignored. If you meant to index " +
		"the vector, make sure to set vectorIndexConfig.skip to 'false'. If the previous " +
		"setting is correct, make sure you set vectorizer to 'none' in the schema and " +
		"provide a null-vector (i.e. no vector) at import time."

	warningSkipVectorGenerated = "this class is configured to skip vector indexing, " +
		"but a vector was generated by the %q vectorizer. " + warningVectorIgnored

	warningSkipVectorProvided = "this class is configured to skip vector indexing, " +
		"but a vector was explicitly provided. " + warningVectorIgnored
)

func (p *Provider) ValidateVectorizer(moduleName string) error {
	mod := p.GetByName(moduleName)
	if mod == nil {
		return errors.Errorf("no module with name %q present", moduleName)
	}

	_, okVec := mod.(modulecapabilities.Vectorizer)
	_, okRefVec := mod.(modulecapabilities.ReferenceVectorizer)
	if !okVec && !okRefVec {
		return errors.Errorf(errorVectorizerCapability, moduleName)
	}

	return nil
}

func (p *Provider) UsingRef2Vec(className string) bool {
	class, err := p.getClass(className)
	if err != nil {
		return false
	}

	cfg := class.ModuleConfig
	if cfg == nil {
		return false
	}

	for modName := range cfg.(map[string]interface{}) {
		mod := p.GetByName(modName)
		if _, ok := mod.(modulecapabilities.ReferenceVectorizer); ok {
			return true
		}
	}

	return false
}

func (p *Provider) UpdateVector(ctx context.Context, object *models.Object, class *models.Class,
	objectDiff *moduletools.ObjectDiff, findObjectFn modulecapabilities.FindObjectFn,
	logger logrus.FieldLogger,
) error {
	hnswConfig, okHnsw := class.VectorIndexConfig.(hnsw.UserConfig)
	_, okFlat := class.VectorIndexConfig.(flat.UserConfig)
	if !(okHnsw || okFlat) {
		return fmt.Errorf(errorVectorIndexType, class.VectorIndexConfig)
	}

	if class.Vectorizer == config.VectorizerModuleNone {
		if hnswConfig.Skip && len(object.Vector) > 0 {
			logger.WithField("className", object.Class).
				Warningf(warningSkipVectorProvided)
		}

		return nil
	}

	if hnswConfig.Skip {
		logger.WithField("className", object.Class).
			WithField("vectorizer", class.Vectorizer).
			Warningf(warningSkipVectorGenerated, class.Vectorizer)
	}

	modConfig, ok := class.ModuleConfig.(map[string]interface{})
	if !ok {
		return fmt.Errorf("class %v not present", object.Class)
	}
	var found modulecapabilities.Module
	for modName := range modConfig {
		if err := p.ValidateVectorizer(modName); err == nil {
			found = p.GetByName(modName)
			break
		}
	}

	if found == nil {
		return fmt.Errorf(
			"no vectorizer found for class %q", object.Class)
	}

	cfg := NewClassBasedModuleConfig(class, found.Name(), "")

	if vectorizer, ok := found.(modulecapabilities.Vectorizer); ok {
		if object.Vector == nil {
			if err := vectorizer.VectorizeObject(ctx, object, objectDiff, cfg); err != nil {
				return fmt.Errorf("update vector: %w", err)
			}
		}
	} else {
		refVectorizer := found.(modulecapabilities.ReferenceVectorizer)
		if err := refVectorizer.VectorizeObject(
			ctx, object, cfg, findObjectFn); err != nil {
			return fmt.Errorf("update reference vector: %w", err)
		}
	}

	return nil
}

func (p *Provider) VectorizerName(className string) (string, error) {
	name, _, err := p.getClassVectorizer(className)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (p *Provider) getClassVectorizer(className string) (string, interface{}, error) {
	sch := p.schemaGetter.GetSchemaSkipAuth()

	class := sch.FindClassByName(schema.ClassName(className))
	if class == nil {
		// this should be impossible by the time this method gets called, but let's
		// be 100% certain
		return "", nil, fmt.Errorf("class %s not present", className)
	}

	return class.Vectorizer, class.VectorIndexConfig, nil
}
