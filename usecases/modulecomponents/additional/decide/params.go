//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package decide

import (
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const (
	// MaxQuestions bounds the questions of one request, and with them the
	// size of every request to the provider.
	MaxQuestions = 25
	// MinChoices and MinLevels: a choice or a score with one option is not
	// a question.
	MinChoices = 2
	MinLevels  = 2
	MaxChoices = 100
	MaxLevels  = 20
)

// Params are the questions of one search.
type Params struct {
	Questions []ent.DecisionQuestion
}

// GetPropertiesToExtract lists the properties the questions are about, so
// that the search fetches them whether or not the client asked for them.
func (p *Params) GetPropertiesToExtract() []string {
	var properties []string
	seen := make(map[string]struct{})
	for _, question := range p.Questions {
		if _, ok := seen[question.Property]; ok || question.Property == "" {
			continue
		}
		seen[question.Property] = struct{}{}
		properties = append(properties, question.Property)
	}
	return properties
}

// Validate checks what no provider could answer: missing names, kinds,
// properties and instructions, duplicate names, options and levels out of
// bounds.
func (p *Params) Validate() error {
	if len(p.Questions) == 0 {
		return fmt.Errorf("no questions")
	}
	if len(p.Questions) > MaxQuestions {
		return fmt.Errorf("%d questions exceed the limit of %d", len(p.Questions), MaxQuestions)
	}
	names := make(map[string]struct{}, len(p.Questions))
	for i, question := range p.Questions {
		if question.Name == "" {
			return fmt.Errorf("question %d has no name", i)
		}
		if _, ok := names[question.Name]; ok {
			return fmt.Errorf("question name %q is used twice", question.Name)
		}
		names[question.Name] = struct{}{}
		if err := validateQuestion(question); err != nil {
			return fmt.Errorf("question %q: %w", question.Name, err)
		}
	}
	return nil
}

func validateQuestion(question ent.DecisionQuestion) error {
	if question.Property == "" {
		return fmt.Errorf("no property")
	}
	if question.Instructions == "" {
		return fmt.Errorf("no instructions")
	}
	switch question.Kind {
	case ent.DecisionPredicate:
		return nil
	case ent.DecisionChoice:
		return validateNames("option", MinChoices, MaxChoices, len(question.Options), func(i int) string {
			return question.Options[i].Value
		})
	case ent.DecisionScore:
		return validateNames("level", MinLevels, MaxLevels, len(question.Levels), func(i int) string {
			return question.Levels[i].Label
		})
	default:
		return fmt.Errorf("unknown kind %q", question.Kind)
	}
}

// validateNames checks the count of the options or levels and that every
// name is set and used once.
func validateNames(what string, lowest, highest, count int, name func(int) string) error {
	if count < lowest || count > highest {
		return fmt.Errorf("needs between %d and %d %ss, got %d", lowest, highest, what, count)
	}
	seen := make(map[string]struct{}, count)
	for i := range count {
		if name(i) == "" {
			return fmt.Errorf("%s %d has no name", what, i)
		}
		if _, ok := seen[name(i)]; ok {
			return fmt.Errorf("%s %q is listed twice", what, name(i))
		}
		seen[name(i)] = struct{}{}
	}
	return nil
}

// Limits are the limits of the class's decisions module, with the module's
// name for the errors.
type Limits struct {
	Module string
	modulecapabilities.DecisionLimits
}

// ValidateFor checks the questions against the class and the limits of its
// decisions module, and the limit against the module's maxDocuments: the
// module judges the page, so the offset does not count. It runs before the
// search, after Validate.
func (p *Params) ValidateFor(class *models.Class, limits Limits, limit int) error {
	if limits.MaxDocuments > 0 && limit > limits.MaxDocuments {
		return fmt.Errorf("limit %d exceeds maxDocuments %d of %s: lower the limit or raise maxDocuments",
			limit, limits.MaxDocuments, limits.Module)
	}
	for _, question := range p.Questions {
		if err := validateQuestionFor(question, class, limits); err != nil {
			return fmt.Errorf("question %q: %w", question.Name, err)
		}
	}
	return nil
}

func validateQuestionFor(question ent.DecisionQuestion, class *models.Class, limits Limits) error {
	property := propertyOf(class, question.Property)
	if property == nil {
		return fmt.Errorf("class %q has no property %q", class.Class, question.Property)
	}
	// AsPrimitive is "" for an array, an object or a reference, so the
	// message names the type as the schema does.
	dataType, _ := schema.AsPrimitive(property.DataType)
	if !slices.Contains(limits.DataTypes, dataType) {
		return fmt.Errorf("property %q is of type %q, %s judges %v",
			question.Property, strings.Join(property.DataType, ","), limits.Module, limits.DataTypes)
	}
	if limits.MaxOptions > 0 && len(question.Options) > limits.MaxOptions {
		return fmt.Errorf("%d options exceed the limit of %d of %s", len(question.Options), limits.MaxOptions, limits.Module)
	}
	if limits.MaxLevels > 0 && len(question.Levels) > limits.MaxLevels {
		return fmt.Errorf("%d levels exceed the limit of %d of %s", len(question.Levels), limits.MaxLevels, limits.Module)
	}
	return nil
}

func propertyOf(class *models.Class, name string) *models.Property {
	for _, property := range class.Properties {
		if property.Name == name {
			return property
		}
	}
	return nil
}
