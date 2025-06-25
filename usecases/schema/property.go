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

package schema

import (
	"context"
	"fmt"
	"strings"

	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// AddClassProperty it is upsert operation. it adds properties to a class and updates
// existing properties if the merge bool passed true.
func (h *Handler) AddClassProperty(ctx context.Context, principal *models.Principal,
	class *models.Class, className string, merge bool, newProps ...*models.Property,
) (*models.Class, uint64, error) {
	if err := h.Authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.CollectionsMetadata(className)...); err != nil {
		return nil, 0, err
	}

	if err := h.Authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsMetadata(className)...); err != nil {
		return nil, 0, err
	}
	classGetterWithAuth := func(name string) (*models.Class, error) {
		if err := h.Authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsMetadata(name)...); err != nil {
			return nil, err
		}
		return h.schemaReader.ReadOnlyClass(name), nil
	}

	if class == nil {
		return nil, 0, fmt.Errorf("class is nil: %w", ErrNotFound)
	}

	if len(newProps) == 0 {
		return nil, 0, nil
	}

	// validate new props
	for _, prop := range newProps {
		if prop.Name == "" {
			return nil, 0, fmt.Errorf("property must contain name")
		}
		prop.Name = schema.LowercaseFirstLetter(prop.Name)
		if prop.DataType == nil {
			return nil, 0, fmt.Errorf("property must contain dataType")
		}
	}

	if err := h.setNewPropDefaults(class, newProps...); err != nil {
		return nil, 0, err
	}

	existingNames := make(map[string]bool, len(class.Properties))
	if !merge {
		for _, prop := range class.Properties {
			existingNames[strings.ToLower(prop.Name)] = true
		}
	}

	if err := h.validateProperty(class, existingNames, false, classGetterWithAuth, newProps...); err != nil {
		return nil, 0, err
	}

	// TODO-RAFT use UpdateProperty() for adding/merging property when index idempotence exists
	// revisit when index idempotence exists and/or allowing merging properties on index.
	props := schema.DedupProperties(class.Properties, newProps)
	if len(props) == 0 {
		return class, 0, nil
	}

	migratePropertySettings(props...)

	class.Properties = clusterSchema.MergeProps(class.Properties, props)
	version, err := h.schemaManager.AddProperty(ctx, class.Class, props...)
	if err != nil {
		return nil, 0, err
	}
	return class, version, err
}

// DeleteClassProperty from existing Schema
func (h *Handler) DeleteClassProperty(ctx context.Context, principal *models.Principal,
	class string, property string,
) error {
	err := h.Authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.CollectionsMetadata(class)...)
	if err != nil {
		return err
	}

	return fmt.Errorf("deleting a property is currently not supported, see " +
		"https://github.com/weaviate/weaviate/issues/973 for details")
	// return h.deleteClassProperty(ctx, class, property, kind.Action)
}

func (h *Handler) setNewPropDefaults(class *models.Class, props ...*models.Property) error {
	setPropertyDefaults(props...)
	h.moduleConfig.SetSinglePropertyDefaults(class, props...)
	return nil
}

func (h *Handler) validatePropModuleConfig(class *models.Class, props ...*models.Property) error {
	configuredVectorizers := map[string]struct{}{}
	if class.Vectorizer != "" {
		configuredVectorizers[class.Vectorizer] = struct{}{}
	}

	for targetVector, cfg := range class.VectorConfig {
		if vm, ok := cfg.Vectorizer.(map[string]interface{}); ok && len(vm) == 1 {
			for vectorizer := range vm {
				configuredVectorizers[vectorizer] = struct{}{}
			}
		} else if len(vm) > 1 {
			return fmt.Errorf("vector index %q has multiple vectorizers", targetVector)
		}
	}

	for _, prop := range props {
		if prop.ModuleConfig == nil {
			continue
		}

		modconfig, ok := prop.ModuleConfig.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%v property config invalid", prop.Name)
		}

		for vectorizer, cfg := range modconfig {
			if err := h.vectorizerValidator.ValidateVectorizer(vectorizer); err != nil {
				continue
			}

			if _, ok := configuredVectorizers[vectorizer]; !ok {
				return fmt.Errorf("vectorizer %q not configured for any of target vectors", vectorizer)
			}

			if _, ok := cfg.(map[string]interface{}); !ok {
				return fmt.Errorf("vectorizer config for vectorizer %q not of type map[string]interface{}", vectorizer)
			}
		}
	}

	return nil
}
