package vectorizer

import "github.com/semi-technologies/weaviate/entities/modulecapabilities"

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
)

type IndexChecker struct {
	cfg modulecapabilities.ClassConfig
}

func NewIndexChecker(cfg modulecapabilities.ClassConfig) *IndexChecker {
	return &IndexChecker{cfg: cfg}
}

func (ic *IndexChecker) PropertyIndexed(propName string) bool {
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

func (ic *IndexChecker) VectorizePropertyName(propName string) bool {
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

func (ic *IndexChecker) VectorizeClassName() bool {
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
