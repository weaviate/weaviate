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

// Package decide is the shared part of the decide additional property: it
// takes the questions of a search, collects the text of the objects, asks
// the module's client and attaches the answers to the results. A module
// supplies the client.
package decide

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// Property is the name of the additional property.
const Property = "decide"

// Client answers every question for every document. The answers of a
// document are in the order of the questions. All questions are about the
// same documents, so a provider that takes several questions per input can
// send them in one request.
type Client interface {
	Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string,
		cfg moduletools.ClassConfig) ([][]ent.DecisionAnswer, error)
}

type Provider struct {
	client Client
}

func New(client Client) *Provider {
	return &Provider{client: client}
}

func (p *Provider) AdditionalPropertyDefaultValue() any {
	return &Params{}
}

func (p *Provider) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params any, limit *int,
	argumentModuleParams map[string]any, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	parameters, ok := params.(*Params)
	if !ok {
		return nil, errors.New("wrong parameters")
	}
	return p.decide(ctx, cfg, in, parameters)
}

// decide attaches to every result, under the property name, one
// ent.DecisionAnswer per question, in the order of the questions. The
// questions about one property go to the client together. An object whose
// property holds no text gets refused answers and is not sent.
func (p *Provider) decide(ctx context.Context, cfg moduletools.ClassConfig,
	in []search.Result, params *Params,
) ([]search.Result, error) {
	if len(in) == 0 {
		return in, nil
	}
	if err := params.Validate(); err != nil {
		return nil, err
	}

	answers := make([][]ent.DecisionAnswer, len(in))
	for i := range in {
		answers[i] = make([]ent.DecisionAnswer, len(params.Questions))
		for j, question := range params.Questions {
			answers[i][j] = ent.DecisionAnswer{Name: question.Name, Kind: question.Kind, Refused: true}
		}
	}

	for _, group := range groupByProperty(params.Questions) {
		documents := make([]string, 0, len(in))
		pending := make([]int, 0, len(in))
		for i := range in {
			text := propertyText(in[i], group.property)
			if text == "" {
				continue
			}
			documents = append(documents, text)
			pending = append(pending, i)
		}
		if len(pending) == 0 {
			continue
		}
		questions := make([]ent.DecisionQuestion, len(group.indexes))
		for k, index := range group.indexes {
			questions[k] = params.Questions[index]
		}
		got, err := p.client.Decide(ctx, questions, documents, cfg)
		if err != nil {
			return nil, fmt.Errorf("client decide: %w", err)
		}
		if len(got) != len(documents) {
			return nil, fmt.Errorf("client decide: %d documents sent, %d answered", len(documents), len(got))
		}
		for k, i := range pending {
			if len(got[k]) != len(questions) {
				return nil, fmt.Errorf("client decide: %d questions asked, %d answered", len(questions), len(got[k]))
			}
			for q, index := range group.indexes {
				answer := got[k][q]
				if answer.Name != questions[q].Name {
					return nil, fmt.Errorf("client decide: answer for %q where %q was asked", answer.Name, questions[q].Name)
				}
				answers[i][index] = answer
			}
		}
	}

	for i := range in {
		if in[i].AdditionalProperties == nil {
			in[i].AdditionalProperties = models.AdditionalProperties{}
		}
		in[i].AdditionalProperties[Property] = answers[i]
	}
	return in, nil
}

// propertyGroup holds the positions of the questions about one property.
type propertyGroup struct {
	property string
	indexes  []int
}

// groupByProperty keeps the order in which the properties first appear.
func groupByProperty(questions []ent.DecisionQuestion) []propertyGroup {
	var groups []propertyGroup
	position := make(map[string]int)
	for i, question := range questions {
		at, ok := position[question.Property]
		if !ok {
			at = len(groups)
			position[question.Property] = at
			groups = append(groups, propertyGroup{property: question.Property})
		}
		groups[at].indexes = append(groups[at].indexes, i)
	}
	return groups
}

// propertyText returns the text of the property, or "" when the object has
// none or it is not text.
func propertyText(result search.Result, property string) string {
	properties, ok := result.Object().Properties.(map[string]any)
	if !ok {
		return ""
	}
	text, _ := properties[property].(string)
	return text
}
