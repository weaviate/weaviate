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
	"errors"
	"fmt"
	"runtime"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

var _NUMCPU = runtime.NumCPU()

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
		return fmt.Errorf("no module with name %q present", moduleName)
	}

	_, okVec := mod.(modulecapabilities.Vectorizer[[]float32])
	_, okRefVec := mod.(modulecapabilities.ReferenceVectorizer[[]float32])
	if !okVec && !okRefVec {
		return fmt.Errorf(errorVectorizerCapability, moduleName)
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
		switch mod.(type) {
		case modulecapabilities.ReferenceVectorizer[[]float32], modulecapabilities.ReferenceVectorizer[[][]float32]:
			return true
		default:
			// do nothing
		}
	}

	return false
}

func (p *Provider) BatchUpdateVector(ctx context.Context, class *models.Class, objects []*models.Object,
	findObjectFn modulecapabilities.FindObjectFn,
	logger logrus.FieldLogger,
) (map[int]error, error) {
	if !p.hasMultipleVectorsConfiguration(class) {
		// legacy vectorizer classes do not necessarily have a module config - filter them out before getting the moduleconfig
		shouldVectorizeClass, err := p.shouldVectorizeClass(class, "", logger)
		if err != nil {
			return nil, err
		}
		if !shouldVectorizeClass {
			return nil, nil
		}
	}

	modConfigs, err := p.getModuleConfigs(class)
	if err != nil {
		return nil, err
	}

	if !p.hasMultipleVectorsConfiguration(class) {
		modConfig := modConfigs[""]
		return p.batchUpdateVector(ctx, objects, class, findObjectFn, "", modConfig)
	} else {
		if len(modConfigs) == 0 {
			return nil, fmt.Errorf("no vectorizer configs for class %q", class.Class)
		}
		vecErrorsList := make([]map[int]error, len(modConfigs))
		errorList := make([]error, len(modConfigs))
		counter := 0
		eg := enterrors.NewErrorGroupWrapper(logger)
		eg.SetLimit(_NUMCPU)
		for targetVector, modConfig := range modConfigs {
			shouldVectorizeClass, err := p.shouldVectorizeClass(class, targetVector, logger)
			if err != nil {
				errorList[counter] = err
				continue
			}
			if shouldVectorizeClass {
				targetVector := targetVector
				modConfig := modConfig
				counter := counter

				fun := func() error {
					vecErrors, err := p.batchUpdateVector(ctx, objects, class, findObjectFn, targetVector, modConfig)
					errorList[counter] = err
					vecErrorsList[counter] = vecErrors
					return nil // to use error group
				}
				eg.Go(fun)
			}

			counter += 1
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}

		// combine errors from different runs
		combinedErrors := make(map[int]error, 0)
		for _, vecErrors := range vecErrorsList {
			for i, vecError := range vecErrors {
				if existingErr, ok := combinedErrors[i]; ok {
					vecError = errors.Join(existingErr, vecError)
				}
				combinedErrors[i] = vecError
			}
		}

		return combinedErrors, errors.Join(errorList...)

	}
}

func (p *Provider) shouldVectorizeClass(class *models.Class, targetVector string, logger logrus.FieldLogger) (bool, error) {
	hnswConfig, err := p.getVectorIndexConfig(class, targetVector)
	if err != nil {
		return false, err
	}

	vectorizer := p.getVectorizer(class, targetVector)
	if vectorizer == config.VectorizerModuleNone {
		return false, nil
	}

	if hnswConfig.Skip {
		logger.WithField("className", class.Class).
			WithField("vectorizer", vectorizer).
			Warningf(warningSkipVectorGenerated, vectorizer)
	}
	return true, nil
}

func (p *Provider) batchUpdateVector(ctx context.Context, objects []*models.Object, class *models.Class,
	findObjectFn modulecapabilities.FindObjectFn,
	targetVector string, modConfig map[string]interface{},
) (map[int]error, error) {
	found := p.getModule(modConfig)
	if found == nil {
		return nil, fmt.Errorf("no vectorizer found for class %q", class.Class)
	}
	cfg := NewClassBasedModuleConfig(class, found.Name(), "", targetVector)

	if vectorizer, ok := found.(modulecapabilities.Vectorizer[[]float32]); ok {
		// each target vector can have its own associated properties, and we need to determine for each one if we should
		// skip it or not. To simplify things, we create a boolean slice that indicates for each object if the given
		// vectorizer needs to act on it or not. This allows us to use the same objects slice for all vectorizers and
		// simplifies the mapping of the returned vectors to the objects.
		skipRevectorization := make([]bool, len(objects))
		for i, obj := range objects {
			if !p.shouldVectorizeObject(obj, cfg) {
				skipRevectorization[i] = true
				continue
			}
			reVectorize, addProps, vector := reVectorize(ctx, cfg, vectorizer, obj, class, nil, targetVector, findObjectFn)
			if !reVectorize {
				skipRevectorization[i] = true
				p.lockGuard(func() {
					p.addVectorToObject(obj, vector, nil, addProps, cfg)
				})
			}
		}
		vectors, addProps, vecErrors := vectorizer.VectorizeBatch(ctx, objects, skipRevectorization, cfg)
		for i := range objects {
			if _, ok := vecErrors[i]; ok || skipRevectorization[i] {
				continue
			}

			var addProp models.AdditionalProperties = nil
			if addProps != nil { // only present for contextionary and probably nobody is using this
				addProp = addProps[i]
			}

			p.lockGuard(func() {
				p.addVectorToObject(objects[i], vectors[i], nil, addProp, cfg)
			})
		}

		return vecErrors, nil
	} else if vectorizer, ok := found.(modulecapabilities.Vectorizer[[][]float32]); ok {
		// each target vector can have its own associated properties, and we need to determine for each one if we should
		// skip it or not. To simplify things, we create a boolean slice that indicates for each object if the given
		// vectorizer needs to act on it or not. This allows us to use the same objects slice for all vectorizers and
		// simplifies the mapping of the returned vectors to the objects.
		skipRevectorization := make([]bool, len(objects))
		for i, obj := range objects {
			if !p.shouldVectorizeObject(obj, cfg) {
				skipRevectorization[i] = true
				continue
			}
			reVectorize, addProps, multiVector := reVectorizeMulti(ctx, cfg, vectorizer, obj, class, nil, targetVector, findObjectFn)
			if !reVectorize {
				skipRevectorization[i] = true
				p.lockGuard(func() {
					p.addVectorToObject(obj, nil, multiVector, addProps, cfg)
				})
			}
		}
		multiVectors, addProps, vecErrors := vectorizer.VectorizeBatch(ctx, objects, skipRevectorization, cfg)
		for i := range objects {
			if _, ok := vecErrors[i]; ok || skipRevectorization[i] {
				continue
			}

			var addProp models.AdditionalProperties = nil
			if addProps != nil { // only present for contextionary and probably nobody is using this
				addProp = addProps[i]
			}

			p.lockGuard(func() {
				p.addVectorToObject(objects[i], nil, multiVectors[i], addProp, cfg)
			})
		}

		return vecErrors, nil
	} else {
		refVectorizer := found.(modulecapabilities.ReferenceVectorizer[[]float32])
		errs := make(map[int]error, 0)
		for i, obj := range objects {
			vector, err := refVectorizer.VectorizeObject(ctx, obj, cfg, findObjectFn)
			if err != nil {
				errs[i] = fmt.Errorf("update reference vector: %w", err)
			}
			p.lockGuard(func() {
				p.addVectorToObject(obj, vector, nil, nil, cfg)
			})
		}
		return errs, nil
	}
}

func (p *Provider) UpdateVector(ctx context.Context, object *models.Object, class *models.Class,
	findObjectFn modulecapabilities.FindObjectFn,
	logger logrus.FieldLogger,
) error {
	if !p.hasMultipleVectorsConfiguration(class) {
		// legacy vectorizer configuration
		vectorize, err := p.shouldVectorize(object, class, "", logger)
		if err != nil {
			return err
		}
		if !vectorize {
			return nil
		}
	}

	modConfigs, err := p.getModuleConfigs(class)
	if err != nil {
		return err
	}

	if !p.hasMultipleVectorsConfiguration(class) {
		// legacy vectorizer configuration
		for targetVector, modConfig := range modConfigs {
			return p.vectorize(ctx, object, class, findObjectFn, targetVector, modConfig)
		}
	}
	return p.vectorizeMultiple(ctx, object, class, findObjectFn, modConfigs, logger)
}

func (p *Provider) hasMultipleVectorsConfiguration(class *models.Class) bool {
	return len(class.VectorConfig) > 0
}

func (p *Provider) vectorizeMultiple(ctx context.Context, object *models.Object, class *models.Class,
	findObjectFn modulecapabilities.FindObjectFn,
	modConfigs map[string]map[string]interface{}, logger logrus.FieldLogger,
) error {
	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(_NUMCPU)

	for targetVector, modConfig := range modConfigs {
		targetVector := targetVector // https://golang.org/doc/faq#closures_and_goroutines
		modConfig := modConfig       // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := p.vectorizeOne(ctx, object, class, findObjectFn, targetVector, modConfig, logger); err != nil {
				return err
			}
			return nil
		}, targetVector)
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (p *Provider) lockGuard(mutate func()) {
	p.vectorsLock.Lock()
	defer p.vectorsLock.Unlock()
	mutate()
}

func (p *Provider) addVectorToObject(object *models.Object,
	vector []float32, multiVector [][]float32,
	additional models.AdditionalProperties, cfg moduletools.ClassConfig,
) {
	if len(additional) > 0 {
		if object.Additional == nil {
			object.Additional = models.AdditionalProperties{}
		}
		for additionalName, additionalValue := range additional {
			object.Additional[additionalName] = additionalValue
		}
	}
	if cfg.TargetVector() == "" {
		object.Vector = vector
		// TODO: add support for multi vectors
		// object.Vector = vector
		return
	}
	if object.Vectors == nil {
		object.Vectors = models.Vectors{}
	}
	object.Vectors[cfg.TargetVector()] = vector
	// TODO: add support for multi vectors
	// if object.MultiVectors == nil {
	// 	object.MultiVectors = models.Vectors{}
	// }
	// object.MultiVectors[cfg.TargetVector()] = multiVectors
}

func (p *Provider) vectorizeOne(ctx context.Context, object *models.Object, class *models.Class,
	findObjectFn modulecapabilities.FindObjectFn,
	targetVector string, modConfig map[string]interface{},
	logger logrus.FieldLogger,
) error {
	vectorize, err := p.shouldVectorize(object, class, targetVector, logger)
	if err != nil {
		return fmt.Errorf("vectorize check for target vector %s: %w", targetVector, err)
	}
	if vectorize {
		if err := p.vectorize(ctx, object, class, findObjectFn, targetVector, modConfig); err != nil {
			return fmt.Errorf("vectorize target vector %s: %w", targetVector, err)
		}
	}
	return nil
}

func (p *Provider) vectorize(ctx context.Context, object *models.Object, class *models.Class,
	findObjectFn modulecapabilities.FindObjectFn,
	targetVector string, modConfig map[string]interface{},
) error {
	found := p.getModule(modConfig)
	if found == nil {
		return fmt.Errorf(
			"no vectorizer found for class %q", object.Class)
	}

	cfg := NewClassBasedModuleConfig(class, found.Name(), "", targetVector)

	if vectorizer, ok := found.(modulecapabilities.Vectorizer[[]float32]); ok {
		if p.shouldVectorizeObject(object, cfg) {
			var targetProperties []string
			vecConfig, ok := modConfig[found.Name()]
			if ok {
				if properties, ok := vecConfig.(map[string]interface{})["properties"]; ok {
					if propSlice, ok := properties.([]string); ok {
						targetProperties = propSlice
					}
				}
			}
			needsRevectorization, additionalProperties, vector := reVectorize(ctx, cfg, vectorizer, object, class, targetProperties, targetVector, findObjectFn)
			if needsRevectorization {
				var err error
				vector, additionalProperties, err = vectorizer.VectorizeObject(ctx, object, cfg)
				if err != nil {
					return fmt.Errorf("update vector: %w", err)
				}
			}

			p.lockGuard(func() {
				p.addVectorToObject(object, vector, nil, additionalProperties, cfg)
			})
			return nil
		}
	} else if vectorizer, ok := found.(modulecapabilities.Vectorizer[[][]float32]); ok {
		if p.shouldVectorizeObject(object, cfg) {
			var targetProperties []string
			vecConfig, ok := modConfig[found.Name()]
			if ok {
				if properties, ok := vecConfig.(map[string]interface{})["properties"]; ok {
					if propSlice, ok := properties.([]string); ok {
						targetProperties = propSlice
					}
				}
			}
			needsRevectorization, additionalProperties, multiVector := reVectorizeMulti(ctx, cfg, vectorizer, object, class, targetProperties, targetVector, findObjectFn)
			if needsRevectorization {
				var err error
				multiVector, additionalProperties, err = vectorizer.VectorizeObject(ctx, object, cfg)
				if err != nil {
					return fmt.Errorf("update vector: %w", err)
				}
			}

			p.lockGuard(func() {
				p.addVectorToObject(object, nil, multiVector, additionalProperties, cfg)
			})
			return nil
		}
	} else {
		refVectorizer := found.(modulecapabilities.ReferenceVectorizer[[]float32])
		vector, err := refVectorizer.VectorizeObject(ctx, object, cfg, findObjectFn)
		if err != nil {
			return fmt.Errorf("update reference vector: %w", err)
		}
		p.lockGuard(func() {
			p.addVectorToObject(object, vector, nil, nil, cfg)
		})
	}
	return nil
}

func (p *Provider) shouldVectorizeObject(object *models.Object, cfg moduletools.ClassConfig) bool {
	if cfg.TargetVector() == "" {
		return object.Vector == nil
	}

	targetVectorExists := false
	p.lockGuard(func() {
		vec, ok := object.Vectors[cfg.TargetVector()]
		targetVectorExists = ok && len(vec) > 0
	})
	return !targetVectorExists
}

func (p *Provider) shouldVectorize(object *models.Object, class *models.Class,
	targetVector string, logger logrus.FieldLogger,
) (bool, error) {
	hnswConfig, err := p.getVectorIndexConfig(class, targetVector)
	if err != nil {
		return false, err
	}

	vectorizer := p.getVectorizer(class, targetVector)
	if vectorizer == config.VectorizerModuleNone {
		vector := p.getVector(object, targetVector)
		if hnswConfig.Skip && len(vector) > 0 {
			logger.WithField("className", class.Class).
				Warningf(warningSkipVectorProvided)
		}
		return false, nil
	}

	if hnswConfig.Skip {
		logger.WithField("className", class.Class).
			WithField("vectorizer", vectorizer).
			Warningf(warningSkipVectorGenerated, vectorizer)
	}
	return true, nil
}

func (p *Provider) getVectorizer(class *models.Class, targetVector string) string {
	if targetVector != "" && len(class.VectorConfig) > 0 {
		if vectorConfig, ok := class.VectorConfig[targetVector]; ok {
			if vectorizer, ok := vectorConfig.Vectorizer.(map[string]interface{}); ok && len(vectorizer) == 1 {
				for vectorizerName := range vectorizer {
					return vectorizerName
				}
			}
		}
		return ""
	}
	return class.Vectorizer
}

func (p *Provider) getVector(object *models.Object, targetVector string) []float32 {
	p.vectorsLock.Lock()
	defer p.vectorsLock.Unlock()
	if targetVector != "" {
		if len(object.Vectors) == 0 {
			return nil
		}
		return object.Vectors[targetVector]
	}
	return object.Vector
}

func (p *Provider) getVectorIndexConfig(class *models.Class, targetVector string) (hnsw.UserConfig, error) {
	vectorIndexConfig := class.VectorIndexConfig
	if targetVector != "" {
		vectorIndexConfig = class.VectorConfig[targetVector].VectorIndexConfig
	}
	hnswConfig, okHnsw := vectorIndexConfig.(hnsw.UserConfig)
	_, okFlat := vectorIndexConfig.(flat.UserConfig)
	_, okDynamic := vectorIndexConfig.(dynamic.UserConfig)
	if !(okHnsw || okFlat || okDynamic) {
		return hnsw.UserConfig{}, fmt.Errorf(errorVectorIndexType, vectorIndexConfig)
	}
	return hnswConfig, nil
}

func (p *Provider) getModuleConfigs(class *models.Class) (map[string]map[string]interface{}, error) {
	modConfigs := map[string]map[string]interface{}{}
	if len(class.VectorConfig) > 0 {
		// get all named vectorizers for classs
		for name, vectorConfig := range class.VectorConfig {
			modConfig, ok := vectorConfig.Vectorizer.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("class %v vectorizer %s not present", class.Class, name)
			}
			modConfigs[name] = modConfig
		}
		return modConfigs, nil
	}
	modConfig, ok := class.ModuleConfig.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("no moduleconfig for class %v present", class.Class)
	}
	if modConfig != nil {
		// get vectorizer
		modConfigs[""] = modConfig
	}
	return modConfigs, nil
}

func (p *Provider) getModule(modConfig map[string]interface{}) (found modulecapabilities.Module) {
	for modName := range modConfig {
		if err := p.ValidateVectorizer(modName); err == nil {
			found = p.GetByName(modName)
			break
		}
	}
	return
}

func (p *Provider) VectorizerName(className string) (string, error) {
	name, _, err := p.getClassVectorizer(className)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (p *Provider) getClassVectorizer(className string) (string, interface{}, error) {
	class := p.schemaGetter.ReadOnlyClass(className)
	if class == nil {
		// this should be impossible by the time this method gets called, but let's
		// be 100% certain
		return "", nil, fmt.Errorf("class %s not present", className)
	}

	return class.Vectorizer, class.VectorIndexConfig, nil
}
