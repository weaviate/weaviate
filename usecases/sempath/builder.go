//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package sempath

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/etiennedi/go-tsne/tsne"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"gonum.org/v1/gonum/mat"
)

func New(c11y c11y) *PathBuilder {
	return &PathBuilder{
		fixedSeed: time.Now().UnixNano(),
		c11y:      c11y,
	}
}

type PathBuilder struct {
	fixedSeed int64
	c11y      c11y
}

type c11y interface {
	MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([]*models.NearestNeighbors, error)
}

func (f *PathBuilder) CalculatePath(in []search.Result, params *Params) ([]search.Result, error) {
	if in == nil || len(in) == 0 {
		return nil, nil
	}

	if params == nil {
		return nil, fmt.Errorf("no params provided")
	}

	dims := len(in[0].Vector)
	if err := params.SetDefaultsAndValidate(len(in), dims); err != nil {
		return nil, errors.Wrap(err, "invalid params")
	}

	searchNeighbors, err := f.addSearchNeighbors(params)
	if err != nil {
		return nil, err
	}

	for i, obj := range in {
		path, err := f.calculatePathPerObject(obj, in, params, searchNeighbors)
		if err != nil {
			return nil, fmt.Errorf("object %d: %v", i, err)
		}

		if in[i].UnderscoreProperties == nil {
			in[i].UnderscoreProperties = &models.UnderscoreProperties{}
		}

		in[i].UnderscoreProperties.SemanticPath = path
	}

	return in, nil
}

func (f *PathBuilder) calculatePathPerObject(obj search.Result, allObjects []search.Result, params *Params,
	searchNeighbors []*models.NearestNeighbor) (*models.SemanticPath, error) {
	dims := len(obj.Vector)
	matrix, neighbors, err := f.vectorsToMatrix(obj, allObjects, dims, params, searchNeighbors)
	if err != nil {
		return nil, err
	}

	rand.Seed(f.fixedSeed) // TODO: don't use global random function
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

	path := f.buildPath(projectedNeighbors, projectedSearchVector, projectedTargetVector)
	return f.addDistancesToPath(path, neighbors, params.SearchVector, obj.Vector)
}

func (f *PathBuilder) addSearchNeighbors(params *Params) ([]*models.NearestNeighbor, error) {
	nn, err := f.c11y.MultiNearestWordsByVector(context.TODO(), [][]float32{params.SearchVector}, 36, 50)
	if err != nil {
		return nil, err
	}

	return nn[0].Neighbors, nil
}

// TODO: document behavior if it actually stays like this
func (f *PathBuilder) vectorsToMatrix(obj search.Result, allObjects []search.Result, dims int,
	params *Params, searchNeighbors []*models.NearestNeighbor) (*mat.Dense, []*models.NearestNeighbor, error) {

	items := 1 // the initial object
	var neighbors []*models.NearestNeighbor
	neighbors = f.extractNeighbors(allObjects)
	neighbors = append(neighbors, searchNeighbors...)
	neighbors = f.removeDuplicateNeighborsAndDollarNeighbors(neighbors)
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

func (f *PathBuilder) extractNeighbors(in []search.Result) []*models.NearestNeighbor {
	var out []*models.NearestNeighbor

	for _, obj := range in {
		if obj.UnderscoreProperties == nil || obj.UnderscoreProperties.NearestNeighbors == nil {
			continue
		}

		out = append(out, obj.UnderscoreProperties.NearestNeighbors.Neighbors...)
	}

	return out
}

func (f *PathBuilder) removeDuplicateNeighborsAndDollarNeighbors(in []*models.NearestNeighbor) []*models.NearestNeighbor {
	seen := map[string]struct{}{}
	out := make([]*models.NearestNeighbor, len(in))

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

type Params struct {
	SearchVector []float32
}

func (p *Params) SetDefaultsAndValidate(inputSize, dims int) error {
	return p.validate(inputSize, dims)
}

func (p *Params) validate(inputSize, dims int) error {
	ec := &errorCompounder{}
	if p.SearchVector == nil || len(p.SearchVector) == 0 {
		ec.addf("no valid search vector present, got: %v", p.SearchVector)
	}

	return ec.toError()
}

type errorCompounder struct {
	errors []error
}

func (ec *errorCompounder) addf(msg string, args ...interface{}) {
	ec.errors = append(ec.errors, fmt.Errorf(msg, args...))
}

func (ec *errorCompounder) add(err error) {
	if err != nil {
		ec.errors = append(ec.errors, err)
	}
}

func (ec *errorCompounder) toError() error {
	if len(ec.errors) == 0 {
		return nil
	}

	var msg strings.Builder
	for i, err := range ec.errors {
		if i != 0 {
			msg.WriteString(", ")
		}

		msg.WriteString(err.Error())
	}

	return errors.New(msg.String())
}

func (f *PathBuilder) buildPath(neighbors []*models.NearestNeighbor, searchVector []float32,
	target []float32) *models.SemanticPath {
	var path []*models.SemanticPathElement

	var minDist = float32(math.MaxFloat32)

	current := searchVector // initial search point

	for {
		nn := f.nearestNeighbors(current, neighbors, 10)
		nn = f.discardFurtherThan(nn, minDist, target)
		if len(nn) == 0 {
			break
		}
		nn = f.nearestNeighbors(current, nn, 1)
		current = nn[0].Vector
		minDist = f.distance(current, target)

		path = append(path, &models.SemanticPathElement{
			Concept: nn[0].Concept,
		})
	}

	return &models.SemanticPath{
		Path: path,
	}
}

func (f *PathBuilder) nearestNeighbors(search []float32, candidates []*models.NearestNeighbor, length int) []*models.NearestNeighbor {

	sort.Slice(candidates, func(a, b int) bool {
		return f.distance(candidates[a].Vector, search) < f.distance(candidates[b].Vector, search)
	})
	return candidates[:length]
}

func (f *PathBuilder) distance(a, b []float32) float32 {
	var sums float32
	for i := range a {
		sums += (a[i] - b[i]) * (a[i] - b[i])
	}

	return float32(math.Sqrt(float64(sums)))
}

func (f *PathBuilder) discardFurtherThan(candidates []*models.NearestNeighbor, threshold float32, target []float32) []*models.NearestNeighbor {
	out := make([]*models.NearestNeighbor, len(candidates))
	i := 0
	for _, c := range candidates {
		if f.distance(c.Vector, target) >= threshold {
			continue
		}

		out[i] = c
		i++
	}

	return out[:i]
}

// craete an explicit deep copy that does not keep any references
func copyNeighbors(in []*models.NearestNeighbor) []*models.NearestNeighbor {
	out := make([]*models.NearestNeighbor, len(in))
	for i, n := range in {
		out[i] = &models.NearestNeighbor{
			Concept:  n.Concept,
			Distance: n.Distance,
			Vector:   n.Vector,
		}
	}

	return out
}

func (f *PathBuilder) addDistancesToPath(path *models.SemanticPath, neighbors []*models.NearestNeighbor,
	searchVector, targetVector []float32) (*models.SemanticPath, error) {

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

func neighborVecByConcept(neighbors []*models.NearestNeighbor, concept string) ([]float32, bool) {
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
