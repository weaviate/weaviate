package utils

func TensorShapeEqual[T int32 | int64](s1 []T, s2 []T) bool {
	if len(s1) != 2 {
		return false
	}
	batchDim, outputDim := s1[0], s1[1]
	expectedBatchDim, expectedOutputDim := s2[0], s2[1]
	if batchDim != expectedBatchDim {
		return false
	}
	return outputDim == expectedOutputDim
}
