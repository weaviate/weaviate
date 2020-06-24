package projector

import (
	"fmt"

	"github.com/etiennedi/go-tsne/tsne"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"gonum.org/v1/gonum/mat"
)

func New() *FeatureProjector {
	return &FeatureProjector{}
}

type FeatureProjector struct{}

func (f *FeatureProjector) Reduce(in []search.Result) ([]search.Result, error) {
	if in == nil || len(in) == 0 {
		return nil, nil
	}

	dims := len(in[0].Vector)

	// concat all vectors to build gonum dense matrix
	mergedVectors := make([]float64, len(in)*dims)
	for i, obj := range in {
		if l := len(obj.Vector); l != dims {
			return nil, fmt.Errorf("inconsistent vector lengths found: %d and %d", dims, l)
		}

		for j, dim := range obj.Vector {
			mergedVectors[i*dims+j] = float64(dim)
		}
	}

	matrix := mat.NewDense(len(in), dims, mergedVectors)
	perplexity := float64(len(in)) / 2
	t := tsne.NewTSNE(2, perplexity, 100, 100, true)
	t.EmbedData(matrix, nil)
	rows, cols := t.Y.Dims()
	if rows != len(in) {
		return nil, fmt.Errorf("incorrect matrix dimensions after t-SNE len %d != %d", len(in), rows)
	}

	for i := 0; i < rows; i++ {
		vector := make([]float32, cols)
		for j := range vector {
			vector[j] = float32(t.Y.At(i, j))
		}
		up := in[i].UnderscoreProperties
		if up == nil {
			up = &models.UnderscoreProperties{}
		}

		up.FeatureProjection = &models.FeatureProjection{
			Vector: vector,
		}

		in[i].UnderscoreProperties = up
	}

	return in, nil
}
