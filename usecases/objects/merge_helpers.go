//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

// validateObjectAndNormalizeNames validates an incoming object update and normalizes property names.
// This is shared between Manager.MergeObject and BatchManager.PatchObjects.
func validateObjectAndNormalizeNames(
	ctx context.Context,
	config *config.WeaviateConfig,
	vectorRepoExists func(context.Context, string, strfmt.UUID, *additional.ReplicationProperties, string) (bool, error),
	repl *additional.ReplicationProperties,
	incoming *models.Object,
	existing *models.Object,
	fetchedClasses map[string]versioned.Class,
) error {
	// Validate UUID format
	if _, err := uuid.Parse(incoming.ID.String()); err != nil {
		return fmt.Errorf("invalid UUID '%s': %w", incoming.ID, err)
	}

	// Validate that class exists in schema
	cls := fetchedClasses[incoming.Class]
	if cls.Class == nil {
		return fmt.Errorf("class %s not found in schema", incoming.Class)
	}

	// Perform full schema validation including datatype checks, required props, ref formats, etc.
	// This is critical to prevent corrupt data from entering the system
	if err := validation.New(vectorRepoExists, config, repl).Object(ctx, cls.Class, incoming, existing); err != nil {
		return err
	}

	return nil
}

// mergeObjectSchemaAndVectorizeShared is a shared version of mergeObjectSchemaAndVectorize
// that can be used by both Manager and BatchManager.
func mergeObjectSchemaAndVectorizeShared(
	ctx context.Context,
	prevPropsSch models.PropertySchema,
	nextProps map[string]interface{},
	prevVec, nextVec []float32,
	prevVecs models.Vectors,
	nextVecs models.Vectors,
	id strfmt.UUID,
	class *models.Class,
	modulesProvider ModulesProvider,
	findObject modulecapabilities.FindObjectFn,
	logger logrus.FieldLogger,
) (*models.Object, error) {
	var mergedProps map[string]interface{}

	vector := nextVec
	vectors := nextVecs
	if prevPropsSch == nil {
		mergedProps = nextProps
	} else {
		prevProps, ok := prevPropsSch.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected previous schema to be map, but got %#v", prevPropsSch)
		}

		mergedProps = map[string]interface{}{}
		for propName, propValue := range prevProps {
			mergedProps[propName] = propValue
		}
		for propName, propValue := range nextProps {
			mergedProps[propName] = propValue
		}
	}

	// Note: vector could be a nil vector in case a vectorizer is configured,
	// then the vectorizer will set it
	obj := &models.Object{Class: class.Class, Properties: mergedProps, Vector: vector, Vectors: vectors, ID: id}
	if err := modulesProvider.UpdateVector(ctx, obj, class, findObject, logger); err != nil {
		return nil, err
	}

	// If there is no vectorization module and no updated vector, use the previous vector(s)
	if obj.Vector == nil && class.Vectorizer == config.VectorizerModuleNone {
		obj.Vector = prevVec
	}

	if obj.Vectors == nil {
		obj.Vectors = models.Vectors{}
	}

	// check for each named vector if the previous vector should be used. This should only happen if
	// - the vectorizer is none
	// - the vector is not set in the update
	// - the vector was set in the previous object
	for name, vectorConfig := range class.VectorConfig {
		if _, ok := vectorConfig.Vectorizer.(map[string]interface{})[config.VectorizerModuleNone]; !ok {
			continue
		}

		prevTargetVector, ok := prevVecs[name]
		if !ok {
			continue
		}

		if _, ok := obj.Vectors[name]; !ok {
			obj.Vectors[name] = prevTargetVector
		}
	}

	return obj, nil
}
