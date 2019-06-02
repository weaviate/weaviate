package vectorizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
)

// Vectorizer turns things and actions into vectors
type Vectorizer struct {
	client client
}

type client interface {
	VectorForCorpi(ctx context.Context, corpi []string) ([]float32, error)
}

// New from c11y client
func New(client client) *Vectorizer {
	return &Vectorizer{client}
}

// Thing concept to vector
func (v *Vectorizer) Thing(ctx context.Context, concept *models.Thing) ([]float32, error) {

	return v.concept(ctx, concept.Class, concept.Schema)
}

// Action concept to vector
func (v *Vectorizer) Action(ctx context.Context, concept *models.Action) ([]float32, error) {

	return v.concept(ctx, concept.Class, concept.Schema)
}

func (v *Vectorizer) concept(ctx context.Context, className string,
	schema interface{}) ([]float32, error) {
	var corpi []string
	corpi = append(corpi, strings.ToLower(className))

	if schema != nil {
		for prop, value := range schema.(map[string]interface{}) {
			valueString, ok := value.(string)
			if ok {
				// use prop and value
				corpi = append(corpi, strings.ToLower(
					fmt.Sprintf("%s %s", prop, valueString)))
			} else {
				// use only prop name
				corpi = append(corpi, strings.ToLower(
					fmt.Sprintf("%s", prop)))
			}
		}
	}

	vector, err := v.client.VectorForCorpi(ctx, corpi)
	if err != nil {
		return nil, fmt.Errorf("vectorizing thing: %v", err)
	}

	return vector, nil
}
