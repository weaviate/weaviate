//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modules

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

func (m *Provider) ValidateVectorizer(moduleName string) error {
	mod := m.GetByName(moduleName)
	if mod == nil {
		return errors.Errorf("no module with name %q present", moduleName)
	}

	_, okVec := mod.(modulecapabilities.Vectorizer)
	_, okRefVec := mod.(modulecapabilities.ReferenceVectorizer)
	if !okVec && !okRefVec {
		return errors.Errorf("module %q exists, but does not provide the "+
			"Vectorizer or ReferenceVectorizer capability", moduleName)
	}

	return nil
}

func (m *Provider) Vectorizer(moduleName, className string) (objects.Vectorizer, error) {
	mod := m.GetByName(moduleName)
	if mod == nil {
		return nil, errors.Errorf("no module with name %q present", moduleName)
	}

	vec, ok := mod.(modulecapabilities.Vectorizer)
	if !ok {
		return nil, errors.Errorf("module %q exists, but does not provide the "+
			"Vectorizer capability", moduleName)
	}

	sch := m.schemaGetter.GetSchemaSkipAuth()
	class := sch.FindClassByName(schema.ClassName(className))
	if class == nil {
		return nil, errors.Errorf("class %q not found in schema", className)
	}

	cfg := NewClassBasedModuleConfig(class, moduleName)
	return NewObjectsVectorizer(vec, cfg), nil
}

func (m *Provider) UsingRef2Vec(className string) bool {
	class, err := m.getClass(className)
	if err != nil {
		return false
	}

	cfg := class.ModuleConfig
	if cfg == nil {
		return false
	}

	for modName := range cfg.(map[string]interface{}) {
		mod := m.GetByName(modName)
		if _, ok := mod.(modulecapabilities.ReferenceVectorizer); ok {
			return true
		}
	}

	return false
}

type ObjectsVectorizer struct {
	modVectorizer modulecapabilities.Vectorizer
	cfg           *ClassBasedModuleConfig
}

func NewObjectsVectorizer(vec modulecapabilities.Vectorizer,
	cfg *ClassBasedModuleConfig,
) *ObjectsVectorizer {
	return &ObjectsVectorizer{modVectorizer: vec, cfg: cfg}
}

func (ov *ObjectsVectorizer) UpdateObject(ctx context.Context,
	obj *models.Object,
) error {
	return ov.modVectorizer.VectorizeObject(ctx, obj, ov.cfg)
}

type ObjectsReferenceVectorizer struct {
	modVectorizer modulecapabilities.ReferenceVectorizer
	cfg           *ClassBasedModuleConfig
}

func NewObjectsReferenceVectorizer(vec modulecapabilities.ReferenceVectorizer,
	cfg *ClassBasedModuleConfig,
) *ObjectsReferenceVectorizer {
	return &ObjectsReferenceVectorizer{modVectorizer: vec, cfg: cfg}
}

func (ov *ObjectsReferenceVectorizer) UpdateObject(ctx context.Context,
	obj *models.Object, findRefVecsFn modulecapabilities.FindRefVectorsFn,
) error {
	return ov.modVectorizer.VectorizeObject(ctx, obj, ov.cfg, findRefVecsFn)
}
