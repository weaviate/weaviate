package vectorizer

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2vec-voyageai/ent"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
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
	Vectorize(ctx context.Context,
		texts, images []string, config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

type ClassSettings interface {
	ImageField(property string) bool
	ImageFieldsWeights() ([]float32, error)
	TextField(property string) bool
	TextFieldsWeights() ([]float32, error)
	BaseURL() string
	Model() string
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	comp moduletools.VectorizablePropsComparator, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object.ID, comp, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, id strfmt.UUID,
	comp moduletools.VectorizablePropsComparator, cfg moduletools.ClassConfig,
) ([]float32, error) {
	ichek := NewClassSettings(cfg)
	prevVector := comp.PrevVector()
	if cfg.TargetVector() != "" {
		prevVector = comp.PrevVectorForName(cfg.TargetVector())
	}

	vectorize := prevVector == nil

	// vectorize image and text
	texts := []string{}
	images := []string{}

	it := comp.PropsIterator()
	for propName, propValue, ok := it.Next(); ok; propName, propValue, ok = it.Next() {
		switch typed := propValue.(type) {
		case string:
			if ichek.ImageField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				images = append(images, typed)
			}
			if ichek.TextField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				texts = append(texts, typed)
			}

		case []string:
			if ichek.TextField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
				texts = append(texts, typed...)
			}

		case nil:
			if ichek.ImageField(propName) || ichek.TextField(propName) {
				vectorize = vectorize || comp.IsChanged(propName)
			}
		}
	}

	// no property was changed, old vector can be used
	if !vectorize {
		return prevVector, nil
	}

	vectors := [][]float32{}
	if len(texts) > 0 || len(images) > 0 {
		res, err := v.client.Vectorize(ctx, texts, images, v.getVectorizationConfig(cfg))
		if err != nil {
			return nil, errors.Wrap(err, "remote client vectorize")
		}
		vectors = append(vectors, res.TextVectors...)
		vectors = append(vectors, res.ImageVectors...)
	}

	weights, err := v.getWeights(ichek)
	if err != nil {
		return nil, err
	}

	return libvectorizer.CombineVectorsWithWeights(vectors, weights), nil
}

func (v *Vectorizer) getWeights(ichek ClassSettings) ([]float32, error) {
	weights := []float32{}
	textFieldsWeights, err := ichek.TextFieldsWeights()
	if err != nil {
		return nil, err
	}
	imageFieldsWeights, err := ichek.ImageFieldsWeights()
	if err != nil {
		return nil, err
	}

	weights = append(weights, textFieldsWeights...)
	weights = append(weights, imageFieldsWeights...)

	normalizedWeights := v.normalizeWeights(weights)

	return normalizedWeights, nil
}

func (v *Vectorizer) normalizeWeights(weights []float32) []float32 {
	if len(weights) > 0 {
		var denominator float32
		for i := range weights {
			denominator += weights[i]
		}
		normalizer := 1 / denominator
		normalized := make([]float32, len(weights))
		for i := range weights {
			normalized[i] = weights[i] * normalizer
		}
		return normalized
	}
	return nil
}

func (v *Vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	settings := NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Model:   settings.Model(),
		BaseURL: settings.BaseURL(),
	}
}
