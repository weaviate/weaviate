package vectorizer

// CombineVectors combines all of the vector into sum of their parts
func (v *Vectorizer) CombineVectors(vectors [][]float32) []float32 {
	maxVectorLength := 0
	for i := range vectors {
		if len(vectors[i]) > maxVectorLength {
			maxVectorLength = len(vectors[i])
		}
	}
	sums := make([]float32, maxVectorLength)
	dividers := make([]float32, maxVectorLength)
	for _, vector := range vectors {
		for i := 0; i < len(vector); i++ {
			sums[i] += vector[i]
			dividers[i]++
		}
	}
	combinedVector := make([]float32, len(sums))
	for i := 0; i < len(sums); i++ {
		combinedVector[i] = sums[i] / dividers[i]
	}

	return combinedVector
}
