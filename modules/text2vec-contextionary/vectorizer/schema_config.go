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

package vectorizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/camelcase"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

type ConfigValidator struct {
	remote RemoteClient
	logger logrus.FieldLogger
}

type IndexChecker interface {
	VectorizeClassName() bool
	VectorizePropertyName(propName string) bool
	PropertyIndexed(propName string) bool
}

type RemoteClient interface {
	IsStopWord(ctx context.Context, word string) (bool, error)
	IsWordPresent(ctx context.Context, word string) (bool, error)
}

func NewConfigValidator(rc RemoteClient,
	logger logrus.FieldLogger,
) *ConfigValidator {
	return &ConfigValidator{remote: rc, logger: logger}
}

func (cv *ConfigValidator) Do(ctx context.Context, class *models.Class,
	cfg moduletools.ClassConfig, icheck IndexChecker,
) error {
	err := cv.validateClassName(ctx, class.Class, icheck.VectorizeClassName())
	if err != nil {
		return fmt.Errorf("invalid class name: %w", err)
	}

	for _, prop := range class.Properties {
		if !icheck.PropertyIndexed(prop.Name) {
			continue
		}

		err = cv.validatePropertyName(ctx, prop.Name,
			icheck.VectorizePropertyName(prop.Name))
		if err != nil {
			return errors.Wrapf(err, "class %q: invalid property name", class.Class)
		}
	}

	if err := cv.validateIndexState(ctx, class, icheck); err != nil {
		return errors.Wrap(err, "invalid combination of properties")
	}

	cv.checkForPossibilityOfDuplicateVectors(ctx, class, icheck)

	return nil
}

func (cv *ConfigValidator) validateClassName(ctx context.Context, className string,
	vectorizeClass bool,
) error {
	// class name
	if !vectorizeClass {
		// if the user chooses not to vectorize the class, we don't need to check
		// if its c11y-valid or not
		return nil
	}

	camelParts := camelcase.Split(className)
	stopWordsFound := 0
	for _, part := range camelParts {
		word := strings.ToLower(part)
		sw, err := cv.remote.IsStopWord(ctx, word)
		if err != nil {
			return fmt.Errorf("check stopword: %v", err)
		}

		if sw {
			stopWordsFound++
			continue
		}

		present, err := cv.remote.IsWordPresent(ctx, word)
		if err != nil {
			return fmt.Errorf("check word presence: %v", err)
		}

		if !present {
			return fmt.Errorf("could not find the word '%s' from the class name '%s' "+
				"in the contextionary", word, className)
		}
	}

	if len(camelParts) == stopWordsFound {
		return fmt.Errorf("className '%s' consists of only stopwords and is therefore "+
			"not a contextionary-valid class name, make sure at least one word in the "+
			"classname is not a stop word", className)
	}

	return nil
}

func (cv *ConfigValidator) validatePropertyName(ctx context.Context,
	propertyName string, vectorize bool,
) error {
	if !vectorize {
		// user does not want to vectorize this property name, so we don't have to
		// validate it
		return nil
	}

	camelParts := camelcase.Split(propertyName)
	stopWordsFound := 0
	for _, part := range camelParts {
		word := strings.ToLower(part)
		sw, err := cv.remote.IsStopWord(ctx, word)
		if err != nil {
			return fmt.Errorf("check stopword: %v", err)
		}

		if sw {
			stopWordsFound++
			continue
		}

		present, err := cv.remote.IsWordPresent(ctx, word)
		if err != nil {
			return fmt.Errorf("check word presence: %v", err)
		}

		if !present {
			return fmt.Errorf("could not find word '%s' of the property '%s' in the "+
				"contextionary", word, propertyName)
		}
	}

	if len(camelParts) == stopWordsFound {
		return fmt.Errorf("the propertyName '%s' consists of only stopwords and is "+
			"therefore not a contextionary-valid property name, make sure at least one word "+
			"in the property name is not a stop word", propertyName)
	}

	return nil
}

func (cv *ConfigValidator) validateIndexState(ctx context.Context,
	class *models.Class, icheck IndexChecker,
) error {
	if icheck.VectorizeClassName() {
		// if the user chooses to vectorize the classname, vector-building will
		// always be possible, no need to investigate further

		return nil
	}

	// search if there is at least one indexed, string/text or string/text[]
	// prop. If found pass validation
	for _, prop := range class.Properties {
		if len(prop.DataType) < 1 {
			return errors.Errorf("property %s must have at least one datatype: "+
				"got %v", prop.Name, prop.DataType)
		}

		if prop.DataType[0] != string(schema.DataTypeText) &&
			prop.DataType[0] != string(schema.DataTypeTextArray) {
			// we can only vectorize text-like props
			continue
		}

		if icheck.PropertyIndexed(prop.Name) {
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
	ctx context.Context, class *models.Class, icheck IndexChecker,
) {
	if !icheck.VectorizeClassName() {
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

		if icheck.PropertyIndexed(prop.Name) {
			// found at least one
			return
		}
	}

	cv.logger.WithField("module", "text2vec-contextionary").
		WithField("class", class.Class).
		Warnf("text2vec-contextionary: Class %q does not have any properties "+
			"indexed (or only non text-properties indexed) and the vector position is "+
			"only determined by the class name. Each object will end up with the same "+
			"vector which leads to a severe performance penalty on imports. Consider "+
			"setting vectorIndexConfig.skip=true for this property", class.Class)
}
