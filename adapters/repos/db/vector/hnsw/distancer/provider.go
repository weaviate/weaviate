package distancer

type Provider interface {
	New(vec []float32) Distancer
}

type Distancer interface {
	Distance(vec []float32) (float32, bool, error)
}
