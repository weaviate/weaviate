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

package projector

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/etiennedi/go-tsne/tsne"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"gonum.org/v1/gonum/mat"
)

func New() *FeatureProjector {
	return &FeatureProjector{
		fixedSeed: time.Now().UnixNano(),
	}
}

type FeatureProjector struct {
	fixedSeed int64
}

func (f *FeatureProjector) Reduce(in []search.Result, params *Params) ([]search.Result, error) {
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
	rand.Seed(f.fixedSeed) // TODO: don't use global random function
	t := tsne.NewTSNE(*params.Dimensions, float64(*params.Perplexity),
		float64(*params.LearningRate), *params.Iterations, false)
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

type Params struct {
	Enabled      bool
	Algorithm    *string // optional parameter
	Dimensions   *int    // optional parameter
	Perplexity   *int    // optional parameter
	Iterations   *int    // optional parameter
	LearningRate *int    // optional parameter
}

func (p *Params) SetDefaultsAndValidate(inputSize, dims int) error {
	p.setDefaults(inputSize, dims)
	return p.validate(inputSize, dims)
}

func (p *Params) setDefaults(inputSize, dims int) {
	perplexity := min(inputSize-1, 5)
	p.Algorithm = p.optionalString(p.Algorithm, "tsne")
	p.Dimensions = p.optionalInt(p.Dimensions, 2)
	p.Perplexity = p.optionalInt(p.Perplexity, perplexity)
	p.Iterations = p.optionalInt(p.Iterations, 100)
	p.LearningRate = p.optionalInt(p.LearningRate, 25)
}

func (p *Params) validate(inputSize, dims int) error {
	ec := &errorCompounder{}
	if *p.Algorithm != "tsne" {
		ec.addf("algorithm %s is not supported: must be one of: tsne", *p.Algorithm)
	}

	if *p.Perplexity >= inputSize {
		ec.addf("perplexity must be smaller than amount of items: %d >= %d", *p.Perplexity, inputSize)
	}

	if *p.Iterations < 1 {
		ec.addf("iterations must be at least 1, got: %d", *p.Iterations)
	}

	if *p.LearningRate < 1 {
		ec.addf("learningRate must be at least 1, got: %d", *p.LearningRate)
	}

	if *p.Dimensions < 1 {
		ec.addf("dimensions must be at least 1, got: %d", *p.Dimensions)
	}

	if *p.Dimensions >= dims {
		ec.addf("dimensions must be smaller than source dimensions: %d >= %d", *p.Dimensions, dims)
	}

	return ec.toError()
}

func (p Params) optionalString(in *string, defaultValue string) *string {
	if in == nil {
		return &defaultValue
	}

	return in
}

func (p Params) optionalInt(in *int, defaultValue int) *int {
	if in == nil {
		return &defaultValue
	}

	return in
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
