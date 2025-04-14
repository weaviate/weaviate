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

// TODO: This entire package should be part of the text2vec-contextionary
// module, if methods/objects in here are used from non-modular code, they
// probably shouldn't be in here

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/fatih/camelcase"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	txt2vecmodels "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

// Vectorizer turns objects into vectors
type Vectorizer struct {
	client           client
	objectVectorizer *objectsvectorizer.ObjectVectorizer
}

type ErrNoUsableWords struct {
	Err error
}

func (e ErrNoUsableWords) Error() string {
	return e.Err.Error()
}

func NewErrNoUsableWordsf(pattern string, args ...interface{}) ErrNoUsableWords {
	return ErrNoUsableWords{Err: fmt.Errorf(pattern, args...)}
}

type client interface {
	VectorForCorpi(ctx context.Context, corpi []string,
		overrides map[string]string) ([]float32, []txt2vecmodels.InterpretationSource, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassIndexCheck interface {
	PropertyIndexed(property string) bool
	VectorizeClassName() bool
	VectorizePropertyName(propertyName string) bool
}

// New from c11y client
func New(client client) *Vectorizer {
	return &Vectorizer{
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
	}
}

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return v.Corpi(ctx, inputs)
}

// Object object to vector
func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	var overrides map[string]string
	if object.VectorWeights != nil {
		overrides = object.VectorWeights.(map[string]string)
	}

	vec, sources, err := v.object(ctx, object, overrides, cfg)
	if err != nil {
		return nil, nil, err
	}

	additional := models.AdditionalProperties{}
	additional["interpretation"] = &txt2vecmodels.Interpretation{
		Source: sourceFromInputElements(sources),
	}

	return vec, additional, nil
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, overrides map[string]string,
	cfg moduletools.ClassConfig,
) ([]float32, []txt2vecmodels.InterpretationSource, error) {
	icheck := NewIndexChecker(cfg)
	corpi := v.objectVectorizer.Texts(ctx, object, icheck)

	vector, ie, err := v.client.VectorForCorpi(ctx, []string{corpi}, overrides)
	if err != nil {
		switch {
		case errors.As(err, &ErrNoUsableWords{}):
			return nil, nil, fmt.Errorf("the object is invalid, as weaviate could not extract "+
				"any contextionary-valid words from it. This is the case when you have "+
				"set the options 'vectorizeClassName: false' and 'vectorizePropertyName: false' in this class' schema definition "+
				"and not a single property's value "+
				"contains at least one contextionary-valid word. To fix this, you have several "+
				"options:\n\n1.) Make sure that the schema class name or the set properties are "+
				"a contextionary-valid term and include them in vectorization using the "+
				"'vectorizeClassName' or 'vectorizePropertyName' setting. In this case the vector position "+
				"will be composed of both the class/property names and the values for those fields. "+
				"Even if no property values are contextionary-valid, the overall word corpus is still valid "+
				"due to the contextionary-valid class/property names."+
				"\n\n2.) Alternatively, if you do not want to include schema class/property names "+
				"in vectorization, you must make sure that at least one text/string property contains "+
				"at least one contextionary-valid word."+
				"\n\n3.) If the word corpus weaviate extracted from your object "+
				"(see below) does contain enough meaning to build a vector position, but the contextionary "+
				"did not recognize the words, you can extend the contextionary using the "+
				"REST API. This is the case	when you use mostly industry-specific terms which are "+
				"not known to the common language contextionary. Once extended, simply reimport this object."+
				"\n\nThe following words were extracted from your object: %v"+
				"\n\nTo learn more about the contextionary and how it behaves, check out: https://www.semi.technology/documentation/weaviate/current/contextionary.html"+
				"\n\nOriginal error: %v", corpi, err)
		default:
			return nil, nil, fmt.Errorf("vectorizing object with corpus '%+v': %w", corpi, err)
		}
	}

	return vector, ie, nil
}

// Corpi takes any list of strings and builds a common vector for all of them
func (v *Vectorizer) Corpi(ctx context.Context, corpi []string,
) ([]float32, error) {
	// can be written to concurrently if multiple named vectors are used
	corpiTmp := make([]string, len(corpi))
	for i, corpus := range corpi {
		corpiTmp[i] = camelCaseToLower(corpus)
	}

	vector, _, err := v.client.VectorForCorpi(ctx, corpiTmp, nil)
	if err != nil {
		return nil, fmt.Errorf("vectorizing corpus '%+v': %w", corpiTmp, err)
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

func sourceFromInputElements(in []txt2vecmodels.InterpretationSource) []*txt2vecmodels.InterpretationSource {
	out := make([]*txt2vecmodels.InterpretationSource, len(in))
	for i, elem := range in {
		out[i] = &txt2vecmodels.InterpretationSource{
			Concept:    elem.Concept,
			Occurrence: elem.Occurrence,
			Weight:     float64(elem.Weight),
		}
	}

	return out
}
