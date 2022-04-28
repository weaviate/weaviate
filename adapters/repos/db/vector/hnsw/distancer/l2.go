package distancer

import "github.com/pkg/errors"

var l2SquaredImpl func(a, b []float32) float32 = func(a, b []float32) float32 {
	var sum float32

	for i := range a {
		diff := a[i] - b[i]

		sum += diff * diff
	}

	return sum
}

type L2Squared struct {
	a []float32
}

func (l L2Squared) Distance(b []float32) (float32, bool, error) {
	if len(l.a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(l.a), len(b))
	}

	return l2SquaredImpl(l.a, b), true, nil
}

type L2SquaredProvider struct{}

func NewL2SquaredProvider() L2SquaredProvider {
	return L2SquaredProvider{}
}

func (l L2SquaredProvider) SingleDist(a, b []float32) (float32, bool, error) {
	if len(a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(a), len(b))
	}

	return l2SquaredImpl(a, b), true, nil
}

func (l L2SquaredProvider) Type() string {
	return "l2-squared"
}

func (l L2SquaredProvider) New(a []float32) Distancer {
	return &L2Squared{a: a}
}
