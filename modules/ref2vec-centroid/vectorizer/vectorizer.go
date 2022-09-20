package vectorizer

import "fmt"

const (
	MethodMean    = "mean"
	MethodDefault = MethodMean
)

type CalcFunc func(vecs ...[]float32) ([]float32, error)

type Vectorizer struct {
	calcFunc CalcFunc
}

func New(method string) *Vectorizer {
	v := &Vectorizer{}
	switch method {
	case MethodMean:
		v.calcFunc = CalculateMean
	default:
		v.calcFunc = CalculateMean
	}

	return v
}

func (v *Vectorizer) CalculateVector(vecs ...[]float32) ([]float32, error) {
	if v.calcFunc == nil {
		return nil, fmt.Errorf("vectorizer calcFunc not set")
	}
	return v.calcFunc(vecs...)
}
