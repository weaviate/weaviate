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

package projector

import "github.com/weaviate/weaviate/entities/errorcompounder"

type Params struct {
	Enabled          bool
	Algorithm        *string // optional parameter
	Dimensions       *int    // optional parameter
	Perplexity       *int    // optional parameter
	Iterations       *int    // optional parameter
	LearningRate     *int    // optional parameter
	IncludeNeighbors bool
}

func (p *Params) SetDefaultsAndValidate(inputSize, dims int) error {
	p.setDefaults(inputSize, dims)
	return p.validate(inputSize, dims)
}

func (p *Params) setDefaults(inputSize, dims int) {
	perplexity := p.min(inputSize-1, 5)
	p.Algorithm = p.optionalString(p.Algorithm, "tsne")
	p.Dimensions = p.optionalInt(p.Dimensions, 2)
	p.Perplexity = p.optionalInt(p.Perplexity, perplexity)
	p.Iterations = p.optionalInt(p.Iterations, 100)
	p.LearningRate = p.optionalInt(p.LearningRate, 25)
}

func (p *Params) validate(inputSize, dims int) error {
	ec := &errorcompounder.ErrorCompounder{}
	if *p.Algorithm != "tsne" {
		ec.Addf("algorithm %s is not supported: must be one of: tsne", *p.Algorithm)
	}

	if *p.Perplexity >= inputSize {
		ec.Addf("perplexity must be smaller than amount of items: %d >= %d", *p.Perplexity, inputSize)
	}

	if *p.Iterations < 1 {
		ec.Addf("iterations must be at least 1, got: %d", *p.Iterations)
	}

	if *p.LearningRate < 1 {
		ec.Addf("learningRate must be at least 1, got: %d", *p.LearningRate)
	}

	if *p.Dimensions < 1 {
		ec.Addf("dimensions must be at least 1, got: %d", *p.Dimensions)
	}

	if *p.Dimensions >= dims {
		ec.Addf("dimensions must be smaller than source dimensions: %d >= %d", *p.Dimensions, dims)
	}

	return ec.ToError()
}

func (p *Params) min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
