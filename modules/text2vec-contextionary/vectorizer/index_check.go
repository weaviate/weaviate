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
	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
)

type indexChecker struct {
	cfg moduletools.ClassConfig
}

func NewIndexChecker(cfg moduletools.ClassConfig) *indexChecker {
	return &indexChecker{cfg: cfg}
}

func (ic *indexChecker) PropertyIndexed(propName string) bool {
	vcn, ok := ic.cfg.Property(propName)["skip"]
	if !ok {
		return DefaultPropertyIndexed
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultPropertyIndexed
	}

	return !asBool
}

func (ic *indexChecker) VectorizePropertyName(propName string) bool {
	vcn, ok := ic.cfg.Property(propName)["vectorizePropertyName"]
	if !ok {
		return DefaultVectorizePropertyName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizePropertyName
	}

	return asBool
}

func (ic *indexChecker) VectorizeClassName() bool {
	vcn, ok := ic.cfg.Class()["vectorizeClassName"]
	if !ok {
		return DefaultVectorizeClassName
	}

	asBool, ok := vcn.(bool)
	if !ok {
		return DefaultVectorizeClassName
	}

	return asBool
}
