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

package modtransformers

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/text2vec-transformers/vectorizer"
)

func (m *TransformersModule) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		"vectorizeClassName": vectorizer.DefaultVectorizeClassName,
		"poolingStrategy":    vectorizer.DefaultPoolingStrategy,
	}
}

func (m *TransformersModule) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]interface{} {
	return map[string]interface{}{
		"skip":                  !vectorizer.DefaultPropertyIndexed,
		"vectorizePropertyName": vectorizer.DefaultVectorizePropertyName,
	}
}

func (m *TransformersModule) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	settings := vectorizer.NewClassSettings(cfg)
	return NewConfigValidator(m.logger).Do(ctx, class, cfg, settings)
}

var _ = modulecapabilities.ClassConfigurator(New())

type ConfigValidator struct {
	logger logrus.FieldLogger
}

type ClassSettings interface {
	VectorizeClassName() bool
	VectorizePropertyName(propName string) bool
	PropertyIndexed(propName string) bool
}

func NewConfigValidator(logger logrus.FieldLogger) *ConfigValidator {
	return &ConfigValidator{logger: logger}
}

func (cv *ConfigValidator) Do(ctx context.Context, class *models.Class,
	cfg moduletools.ClassConfig, settings ClassSettings,
) error {
	// In text2vec-transformers (as opposed to e.g. text2vec-contextionary) the
	// assumption is that the models will be able to deal with any words, even
	// previously unseen ones. Therefore we do not need to validate individual
	// properties, but only the overall "index state"

	if err := cv.validateIndexState(ctx, class, settings); err != nil {
		return errors.Errorf("invalid combination of properties")
	}

	cv.checkForPossibilityOfDuplicateVectors(ctx, class, settings)

	return nil
}

func (cv *ConfigValidator) validateIndexState(ctx context.Context,
	class *models.Class, settings ClassSettings,
) error {
	if settings.VectorizeClassName() {
		// if the user chooses to vectorize the classname, vector-building will
		// always be possible, no need to investigate further

		return nil
	}

	// search if there is at least one indexed, string/text prop. If found pass
	// validation
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return errors.Errorf("property %s must have at least one datatype: "+
				"got %v", prop.Name, prop.DataType)
		}

		if prop.DataType[0] != string(schema.DataTypeText) {
			// we can only vectorize text-like props
			continue
		}

		if settings.PropertyIndexed(prop.Name) {
			// found at least one, this is a valid schema
			return nil
		}
	}

	return fmt.Errorf("invalid properties: didn't find a single property which is " +
		"of type string or text and is not excluded from indexing. In addition the " +
		"class name is excluded from vectorization as well, meaning that it cannot be " +
		"used to determine the vector position. To fix this, set 'vectorizeClassName' " +
		"to true if the class name is contextionary-valid. Alternatively add at least " +
		"contextionary-valid text/string property which is not excluded from " +
		"indexing.")
}

func (cv *ConfigValidator) checkForPossibilityOfDuplicateVectors(
	ctx context.Context, class *models.Class, settings ClassSettings,
) {
	if !settings.VectorizeClassName() {
		// if the user choses not to vectorize the class name, this means they must
		// have chosen something else to vectorize, otherwise the validation would
		// have error'd before we ever got here. We can skip further checking.

		return
	}

	// search if there is at least one indexed, string/text prop. If found exit
	for _, prop := range class.Properties {
		// length check skipped, because validation has already passed
		if prop.DataType[0] != string(schema.DataTypeText) {
			// we can only vectorize text-like props
			continue
		}

		if settings.PropertyIndexed(prop.Name) {
			// found at least one
			return
		}
	}

	cv.logger.WithField("module", "text2vec-transformers").
		WithField("class", class.Class).
		Warnf("text2vec-contextionary: Class %q does not have any properties "+
			"indexed (or only non text-properties indexed) and the vector position is "+
			"only determined by the class name. Each object will end up with the same "+
			"vector which leads to a severe performance penalty on imports. Consider "+
			"setting vectorIndexConfig.skip=true for this property", class.Class)
}
