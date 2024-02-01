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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-palm/ent"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

type Vectorizer struct {
	client           Client
	objectVectorizer *objectsvectorizer.ObjectVectorizer
}

func New(client Client) *Vectorizer {
	return &Vectorizer{
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
	}
}

type Client interface {
	Vectorize(ctx context.Context, input []string,
		config ent.VectorizationConfig, titlePropertyValue string) (*ent.VectorizationResult, error)
	VectorizeQuery(ctx context.Context, input []string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	ApiEndpoint() string
	ProjectID() string
	ModelID() string
	TitleProperty() string
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	comp moduletools.VectorizablePropsComparator, settings ClassSettings,
) error {
	vec, err := v.object(ctx, object.Class, comp, settings)
	if err != nil {
		return err
	}

	// TODO[named-vectors]: move the vectorizer code to use a generic method to
	// object = v.objectVectorizer.AddVectorToObject(object, vec, settings)
	object.Vector = vec
	return nil
}

func (v *Vectorizer) object(ctx context.Context, className string,
	comp moduletools.VectorizablePropsComparator, icheck ClassSettings,
) ([]float32, error) {
	vectorize := comp.PrevVector() == nil

	var titlePropertyValue []string
	var corpi []string

	if icheck.VectorizeClassName() {
		corpi = append(corpi, camelCaseToLower(className))
	}

	it := comp.PropsIterator()
	for propName, value, ok := it.Next(); ok; propName, value, ok = it.Next() {
		if !icheck.PropertyIndexed(propName) {
			continue
		}

		switch typed := value.(type) {
		case string:
			vectorize = vectorize || comp.IsChanged(propName)

			str := strings.ToLower(typed)
			if icheck.VectorizePropertyName(propName) {
				str = fmt.Sprintf("%s %s", camelCaseToLower(propName), str)
			}
			corpi = append(corpi, str)
			if propName == icheck.TitleProperty() {
				titlePropertyValue = append(titlePropertyValue, str)
			}

		case []string:
			vectorize = vectorize || comp.IsChanged(propName)

			if len(typed) > 0 {
				isNameVectorizable := icheck.VectorizePropertyName(propName)
				lowerPropertyName := camelCaseToLower(propName)
				isTitleProperty := propName == icheck.TitleProperty()

				for i := range typed {
					str := strings.ToLower(typed[i])
					if isNameVectorizable {
						str = fmt.Sprintf("%s %s", lowerPropertyName, str)
					}
					corpi = append(corpi, str)
					if isTitleProperty {
						titlePropertyValue = append(titlePropertyValue, str)
					}
				}
			}

		case nil:
			vectorize = vectorize || comp.IsChanged(propName)
		}
	}

	// no property was changed, old vector can be used
	if !vectorize {
		return comp.PrevVector(), nil
	}

	if len(corpi) == 0 {
		// fall back to using the class name
		corpi = append(corpi, camelCaseToLower(className))
	}

	text := []string{strings.Join(corpi, " ")}
	titleProperty := strings.Join(titlePropertyValue, " ")
	res, err := v.client.Vectorize(ctx, text, ent.VectorizationConfig{
		ApiEndpoint: icheck.ApiEndpoint(),
		ProjectID:   icheck.ProjectID(),
		Model:       icheck.ModelID(),
	}, titleProperty)
	if err != nil {
		return nil, err
	}
	if len(res.Vectors) == 0 {
		return nil, fmt.Errorf("no vectors generated")
	}

	if len(res.Vectors) > 1 {
		return v.CombineVectors(res.Vectors), nil
	}
	return res.Vectors[0], nil
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
