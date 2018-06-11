package vector

func ComputeCentroid(vectors []Vector) Vector {
	var weights []float32 = make([]float32, len(vectors))

	for i := 0; i < len(vectors); i++ {
		weights[i] = 1.0
	}

	return ComputeWeightedCentroid(vectors, weights)
}

func ComputeWeightedCentroid(vectors []Vector, weights []float32) Vector {
	return Vector{}
}
