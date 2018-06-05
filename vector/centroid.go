func ComputeCentroid(vectors []Vector) Vector {
  weights []float := make([]float32, len(vectors))
  for i := range len(vectors) {
    weights[i] = 1.0
  }

  return ComputeWeightedCentroid(vectors, weights)
}

func ComputeWeightedCentroid(vectors []Vector, weights []float32) Vector {
  return Vector { vector: [], }
}
