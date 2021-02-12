package vectorizer

import (
	"github.com/semi-technologies/weaviate/entities/moduletools"
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
