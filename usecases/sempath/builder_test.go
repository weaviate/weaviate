package sempath

import (
	"context"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/stretchr/testify/require"
)

func TestSemanticPathBuilder(t *testing.T) {
	c11y := &fakeC11y{}
	b := New(c11y)

	b.fixedSeed = 1000 // control randomness in unit test

	input := []search.Result{
		search.Result{
			ID:        "7fe919ed-2ef6-4087-856c-a307046bf895",
			Kind:      kind.Thing,
			ClassName: "Foo",
			Vector:    []float32{1, 0.1},
		},
	}
	searchVector := []float32{0.3, 0.3}

	c11y.neighbors = []*models.NearestNeighbors{
		&models.NearestNeighbors{
			Neighbors: []*models.NearestNeighbor{
				&models.NearestNeighbor{
					Concept: "good1",
					Vector:  []float32{0.5, 0.1},
				},
				&models.NearestNeighbor{
					Concept: "good2",
					Vector:  []float32{0.7, 0.2},
				},
				&models.NearestNeighbor{
					Concept: "good3",
					Vector:  []float32{0.9, 0.1},
				},
				&models.NearestNeighbor{
					Concept: "good4",
					Vector:  []float32{0.55, 0.1},
				},
				&models.NearestNeighbor{
					Concept: "good5",
					Vector:  []float32{0.77, 0.2},
				},
				&models.NearestNeighbor{
					Concept: "good6",
					Vector:  []float32{0.99, 0.1},
				},
				&models.NearestNeighbor{
					Concept: "bad1",
					Vector:  []float32{-0.1, -3},
				},
				&models.NearestNeighbor{
					Concept: "bad2",
					Vector:  []float32{-0.15, -2.75},
				},
				&models.NearestNeighbor{
					Concept: "bad3",
					Vector:  []float32{-0.22, -2.35},
				},
				&models.NearestNeighbor{
					Concept: "bad4",
					Vector:  []float32{0.1, -3.3},
				},
				&models.NearestNeighbor{
					Concept: "bad5",
					Vector:  []float32{0.15, -2.5},
				},
				&models.NearestNeighbor{
					Concept: "bad6",
					Vector:  []float32{-0.4, -2.25},
				},
			},
		},
	}

	res, err := b.CalculatePath(input, &Params{SearchVector: searchVector})
	require.Nil(t, err)

	spew.Dump(res)
	t.Fail()
}

type fakeC11y struct {
	neighbors []*models.NearestNeighbors
}

func (f *fakeC11y) MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([]*models.NearestNeighbors, error) {
	return f.neighbors, nil
}
