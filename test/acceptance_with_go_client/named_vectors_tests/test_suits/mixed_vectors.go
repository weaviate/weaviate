package test_suits

import "testing"

func AllMixedVectorsTests(endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("test", testMixedVectorsCreateSchema(endpoint))
	}
}
