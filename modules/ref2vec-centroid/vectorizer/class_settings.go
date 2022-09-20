package vectorizer

import "github.com/semi-technologies/weaviate/entities/moduletools"

const (
	CalculationMethodField   = "method"
	ReferencePropertiesField = "referenceProperties"
)

func DefaultConfig() map[string]interface{} {
	return map[string]interface{}{
		CalculationMethodField: MethodDefault,
	}
}

type ClassSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *ClassSettings {
	return &ClassSettings{cfg: cfg}
}

func (cs *ClassSettings) ReferenceProperties() map[string]struct{} {
	refProps := map[string]struct{}{}
	props := cs.cfg.Class()

	iRefProps := props[ReferencePropertiesField].([]interface{})
	for _, iProp := range iRefProps {
		refProps[iProp.(string)] = struct{}{}
	}

	return refProps
}

func (cs *ClassSettings) CalculationMethod() string {
	props := cs.cfg.Class()
	calcMethod := props[CalculationMethodField].(string)
	return calcMethod
}
