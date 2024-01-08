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

package sempath

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/danaugrs/go-tsne/tsne"
	"github.com/pkg/errors"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	txt2vecmodels "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
	"gonum.org/v1/gonum/mat"
)

func New(c11y Remote) *PathBuilder {
	return &PathBuilder{
		fixedSeed: time.Now().UnixNano(),
		c11y:      c11y,
	}
}

type PathBuilder struct {
	fixedSeed int64
	c11y      Remote
}

type Remote interface {
	MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([]*txt2vecmodels.NearestNeighbors, error)
}

func (pb *PathBuilder) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (pb *PathBuilder) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return pb.CalculatePath(in, parameters)
	}
	return nil, errors.New("unknown params")
}

func (pb *PathBuilder) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return &Params{}
}

func (pb *PathBuilder) CalculatePath(in []search.Result, params *Params) ([]search.Result, error) {
	if len(in) == 0 {
		return nil, nil
	}

	if params == nil {
		return nil, fmt.Errorf("no params provided")
	}

	dims := len(in[0].Vector)
	if err := params.SetDefaultsAndValidate(len(in), dims); err != nil {
		return nil, errors.Wrap(err, "invalid params")
	}

	searchNeighbors, err := pb.addSearchNeighbors(params)
	if err != nil {
		return nil, err
	}

	for i, obj := range in {
		path, err := pb.calculatePathPerObject(obj, in, params, searchNeighbors)
		if err != nil {
			return nil, fmt.Errorf("object %d: %v", i, err)
		}

		if in[i].AdditionalProperties == nil {
			in[i].AdditionalProperties = models.AdditionalProperties{}
		}

		in[i].AdditionalProperties["semanticPath"] = path
	}

	return in, nil
}

func (pb *PathBuilder) calculatePathPerObject(obj search.Result, allObjects []search.Result, params *Params,
	searchNeighbors []*txt2vecmodels.NearestNeighbor,
) (*txt2vecmodels.SemanticPath, error) {
	dims := len(obj.Vector)
	matrix, neighbors, err := pb.vectorsToMatrix(obj, allObjects, dims, params, searchNeighbors)
	if err != nil {
		return nil, err
	}

	inputRows := matrix.RawMatrix().Rows
	t := tsne.NewTSNE(2, float64(inputRows/2), 100, 100, false)
	res := t.EmbedData(matrix, nil)
	rows, cols := res.Dims()
	if rows != inputRows {
		return nil, fmt.Errorf("have different output results than input %d != %d", inputRows, rows)
	}

	// create an explicit copy of the neighbors, so we don't mutate them.
	// Otherwise the 2nd round will have been influenced by the first
	projectedNeighbors := copyNeighbors(neighbors)
	var projectedSearchVector []float32
	var projectedTargetVector []float32
	for i := 0; i < rows; i++ {
		vector := make([]float32, cols)
		for j := range vector {
			vector[j] = float32(res.At(i, j))
		}
		if i == 0 { // the input object
			projectedTargetVector = vector
		} else if i < 1+len(neighbors) {
			// these must be neighbor props
			projectedNeighbors[i-1].Vector = vector
		} else {
			// is now the very last element which is the search vector
			projectedSearchVector = vector
		}
	}

	path := pb.buildPath(projectedNeighbors, projectedSearchVector, projectedTargetVector)
	return pb.addDistancesToPath(path, neighbors, params.SearchVector, obj.Vector)
}

func (pb *PathBuilder) addSearchNeighbors(params *Params) ([]*txt2vecmodels.NearestNeighbor, error) {
	nn, err := pb.c11y.MultiNearestWordsByVector(context.TODO(), [][]float32{params.SearchVector}, 36, 50)
	if err != nil {
		return nil, err
	}

	return nn[0].Neighbors, nil
}

// TODO: document behavior if it actually stays like this
func (pb *PathBuilder) vectorsToMatrix(obj search.Result, allObjects []search.Result, dims int,
	params *Params, searchNeighbors []*txt2vecmodels.NearestNeighbor,
) (*mat.Dense, []*txt2vecmodels.NearestNeighbor, error) {
	items := 1 // the initial object
	var neighbors []*txt2vecmodels.NearestNeighbor
	neighbors = pb.extractNeighbors(allObjects)
	neighbors = append(neighbors, searchNeighbors...)
	neighbors = pb.removeDuplicateNeighborsAndDollarNeighbors(neighbors)
	items += len(neighbors) + 1 // The +1 is for the search vector which we append last

	// concat all vectors to build gonum dense matrix
	mergedVectors := make([]float64, items*dims)
	if l := len(obj.Vector); l != dims {
		return nil, nil, fmt.Errorf("object: inconsistent vector lengths found: dimensions=%d and object=%d", dims, l)
	}

	for j, dim := range obj.Vector {
		mergedVectors[j] = float64(dim)
	}

	withoutNeighbors := 1 * dims
	for i, neighbor := range neighbors {
		neighborVector := neighbor.Vector

		if l := len(neighborVector); l != dims {
			return nil, nil, fmt.Errorf("neighbor: inconsistent vector lengths found: dimensions=%d and object=%d", dims, l)
		}

		for j, dim := range neighborVector {
			mergedVectors[withoutNeighbors+i*dims+j] = float64(dim)
		}
	}

	for i, dim := range params.SearchVector {
		mergedVectors[len(mergedVectors)-dims+i] = float64(dim)
	}

	return mat.NewDense(items, dims, mergedVectors), neighbors, nil
}

func (pb *PathBuilder) extractNeighbors(in []search.Result) []*txt2vecmodels.NearestNeighbor {
	var out []*txt2vecmodels.NearestNeighbor

	for _, obj := range in {
		if obj.AdditionalProperties == nil || obj.AdditionalProperties["nearestNeighbors"] == nil {
			continue
		}

		if neighbors, ok := obj.AdditionalProperties["nearestNeighbors"]; ok {
			if nearestNeighbors, ok := neighbors.(*txt2vecmodels.NearestNeighbors); ok {
				out = append(out, nearestNeighbors.Neighbors...)
			}
		}
	}

	return out
}

func (pb *PathBuilder) removeDuplicateNeighborsAndDollarNeighbors(in []*txt2vecmodels.NearestNeighbor) []*txt2vecmodels.NearestNeighbor {
	seen := map[string]struct{}{}
	out := make([]*txt2vecmodels.NearestNeighbor, len(in))

	i := 0
	for _, candidate := range in {
		if _, ok := seen[candidate.Concept]; ok {
			continue
		}

		if candidate.Concept[0] == '$' {
			continue
		}

		out[i] = candidate
		i++
		seen[candidate.Concept] = struct{}{}
	}

	return out[:i]
}

func (pb *PathBuilder) buildPath(neighbors []*txt2vecmodels.NearestNeighbor, searchVector []float32,
	target []float32,
) *txt2vecmodels.SemanticPath {
	var path []*txt2vecmodels.SemanticPathElement

	minDist := float32(math.MaxFloat32)

	current := searchVector // initial search point

	for {
		nn := pb.nearestNeighbors(current, neighbors, 10)
		nn = pb.discardFurtherThan(nn, minDist, target)
		if len(nn) == 0 {
			break
		}
		nn = pb.nearestNeighbors(current, nn, 1)
		current = nn[0].Vector
		minDist = pb.distance(current, target)

		path = append(path, &txt2vecmodels.SemanticPathElement{
			Concept: nn[0].Concept,
		})
	}

	return &txt2vecmodels.SemanticPath{
		Path: path,
	}
}

func (pb *PathBuilder) nearestNeighbors(search []float32, candidates []*txt2vecmodels.NearestNeighbor, length int) []*txt2vecmodels.NearestNeighbor {
	sort.Slice(candidates, func(a, b int) bool {
		return pb.distance(candidates[a].Vector, search) < pb.distance(candidates[b].Vector, search)
	})
	return candidates[:length]
}

func (pb *PathBuilder) distance(a, b []float32) float32 {
	var sums float32
	for i := range a {
		sums += (a[i] - b[i]) * (a[i] - b[i])
	}

	return float32(math.Sqrt(float64(sums)))
}

func (pb *PathBuilder) discardFurtherThan(candidates []*txt2vecmodels.NearestNeighbor, threshold float32, target []float32) []*txt2vecmodels.NearestNeighbor {
	out := make([]*txt2vecmodels.NearestNeighbor, len(candidates))
	i := 0
	for _, c := range candidates {
		if pb.distance(c.Vector, target) >= threshold {
			continue
		}

		out[i] = c
		i++
	}

	return out[:i]
}

// create an explicit deep copy that does not keep any references
func copyNeighbors(in []*txt2vecmodels.NearestNeighbor) []*txt2vecmodels.NearestNeighbor {
	out := make([]*txt2vecmodels.NearestNeighbor, len(in))
	for i, n := range in {
		out[i] = &txt2vecmodels.NearestNeighbor{
			Concept:  n.Concept,
			Distance: n.Distance,
			Vector:   n.Vector,
		}
	}

	return out
}

func (pb *PathBuilder) addDistancesToPath(path *txt2vecmodels.SemanticPath, neighbors []*txt2vecmodels.NearestNeighbor,
	searchVector, targetVector []float32,
) (*txt2vecmodels.SemanticPath, error) {
	for i, elem := range path.Path {
		vec, ok := neighborVecByConcept(neighbors, elem.Concept)
		if !ok {
			return nil, fmt.Errorf("no vector present for concept: %s", elem.Concept)
		}

		if i != 0 {
			// include previous
			previousVec, ok := neighborVecByConcept(neighbors, path.Path[i-1].Concept)
			if !ok {
				return nil, fmt.Errorf("no vector present for previous concept: %s", path.Path[i-1].Concept)
			}

			d, err := cosineDist(vec, previousVec)
			if err != nil {
				return nil, errors.Wrap(err, "calculate distance between current path and previous element")
			}

			path.Path[i].DistanceToPrevious = &d
		}

		// target
		d, err := cosineDist(vec, targetVector)
		if err != nil {
			return nil, errors.Wrap(err, "calculate distance between current path and result element")
		}
		path.Path[i].DistanceToResult = d

		// query
		d, err = cosineDist(vec, searchVector)
		if err != nil {
			return nil, errors.Wrap(err, "calculate distance between current path and query element")
		}
		path.Path[i].DistanceToQuery = d

		if i != len(path.Path)-1 {
			// include next
			nextVec, ok := neighborVecByConcept(neighbors, path.Path[i+1].Concept)
			if !ok {
				return nil, fmt.Errorf("no vector present for next concept: %s", path.Path[i+1].Concept)
			}

			d, err := cosineDist(vec, nextVec)
			if err != nil {
				return nil, errors.Wrap(err, "calculate distance between current path and next element")
			}

			path.Path[i].DistanceToNext = &d
		}
	}

	return path, nil
}

func neighborVecByConcept(neighbors []*txt2vecmodels.NearestNeighbor, concept string) ([]float32, bool) {
	for _, n := range neighbors {
		if n.Concept == concept {
			return n.Vector, true
		}
	}

	return nil, false
}

func cosineSim(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vectors have different dimensions")
	}

	var (
		sumProduct float64
		sumASquare float64
		sumBSquare float64
	)

	for i := range a {
		sumProduct += float64(a[i] * b[i])
		sumASquare += float64(a[i] * a[i])
		sumBSquare += float64(b[i] * b[i])
	}

	return float32(sumProduct / (math.Sqrt(sumASquare) * math.Sqrt(sumBSquare))), nil
}

func cosineDist(a, b []float32) (float32, error) {
	sim, err := cosineSim(a, b)
	if err != nil {
		return 0, err
	}

	return 1 - sim, nil
}
