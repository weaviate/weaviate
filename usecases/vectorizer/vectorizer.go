//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package vectorizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/camelcase"
	"github.com/semi-technologies/weaviate/entities/models"
)

// Vectorizer turns things and actions into vectors
type Vectorizer struct {
	client     client
	indexCheck IndexCheck
}

type client interface {
	VectorForCorpi(ctx context.Context, corpi []string) ([]float32, error)
}

// IndexCheck returns whether a property of a class should be indexed
type IndexCheck interface {
	Indexed(className, property string) bool
}

// New from c11y client
func New(client client, indexCheck IndexCheck) *Vectorizer {
	return &Vectorizer{client, indexCheck}
}

func (v *Vectorizer) SetIndexChecker(ic IndexCheck) {
	v.indexCheck = ic
}

// Thing object to vector
func (v *Vectorizer) Thing(ctx context.Context, object *models.Thing) ([]float32, error) {
	return v.object(ctx, object.Class, object.Schema)
}

// Action object to vector
func (v *Vectorizer) Action(ctx context.Context, object *models.Action) ([]float32, error) {

	return v.object(ctx, object.Class, object.Schema)
}

func (v *Vectorizer) object(ctx context.Context, className string,
	schema interface{}) ([]float32, error) {
	var corpi []string
	corpi = append(corpi, camelCaseToLower(className))

	if schema != nil {
		for prop, value := range schema.(map[string]interface{}) {
			if !v.indexCheck.Indexed(className, prop) {
				continue
			}

			valueString, ok := value.(string)
			if ok {
				// use prop and value
				corpi = append(corpi, strings.ToLower(
					fmt.Sprintf("%s %s", camelCaseToLower(prop), valueString)))
			}
		}
	}

	vector, err := v.client.VectorForCorpi(ctx, []string{strings.Join(corpi, " ")})
	if err != nil {
		return nil, fmt.Errorf("vectorizing thing with corpus '%+v': %v", corpi, err)
	}

	return vector, nil
}

// Corpi takes any list of strings and builds a common vector for all of them
func (v *Vectorizer) Corpi(ctx context.Context, corpi []string,
) ([]float32, error) {

	for i, corpus := range corpi {
		corpi[i] = camelCaseToLower(corpus)
	}

	vector, err := v.client.VectorForCorpi(ctx, corpi)
	if err != nil {
		return nil, fmt.Errorf("vectorizing thing with corpus '%+v': %v", corpi, err)
	}

	return vector, nil
}

func camelCaseToLower(in string) string {
	parts := camelcase.Split(in)
	var sb strings.Builder
	for i, part := range parts {
		if part == " " {
			continue
		}

		if i > 0 {
			sb.WriteString(" ")
		}

		sb.WriteString(strings.ToLower(part))
	}

	return sb.String()
}
