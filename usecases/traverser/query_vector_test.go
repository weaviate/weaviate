package traverser

import (
	"testing"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func TestExtractQueryVectorFromParams(t *testing.T) {
	// Test with NearVector parameters - []float32 type
	t.Run("nearVector with float32 slice", func(t *testing.T) {
		params := dto.GetParams{
			NearVector: &searchparams.NearVector{
				Vectors: []models.Vector{
					[]float32{0.1, 0.2, 0.3, 0.4},
				},
			},
		}

		result := ExtractQueryVectorFromParams(params)
		
		if result == nil {
			t.Error("Expected non-nil result for NearVector params")
			return
		}

		expected := []float32{0.1, 0.2, 0.3, 0.4}
		if len(result) != len(expected) {
			t.Errorf("Expected vector length %d, got %d", len(expected), len(result))
			return
		}

		for i, v := range expected {
			if result[i] != v {
				t.Errorf("Expected value %f at index %d, got %f", v, i, result[i])
			}
		}
	})

	// Test with interface{} slice containing float64 values
	t.Run("nearVector with interface slice", func(t *testing.T) {
		params := dto.GetParams{
			NearVector: &searchparams.NearVector{
				Vectors: []models.Vector{
					[]interface{}{0.1, 0.2, 0.3, 0.4},
				},
			},
		}

		result := ExtractQueryVectorFromParams(params)
		
		if result == nil {
			t.Error("Expected non-nil result for interface{} vector")
			return
		}

		expected := []float32{0.1, 0.2, 0.3, 0.4}
		if len(result) != len(expected) {
			t.Errorf("Expected vector length %d, got %d", len(expected), len(result))
			return
		}

		for i, v := range expected {
			if result[i] != v {
				t.Errorf("Expected value %f at index %d, got %f", v, i, result[i])
			}
		}
	})
}

func TestExtractQueryVectorFromParamsEmpty(t *testing.T) {
	// Test with empty parameters
	params := dto.GetParams{}

	result := ExtractQueryVectorFromParams(params)
	
	if result != nil {
		t.Error("Expected nil result for empty params")
	}
}

func TestExtractQueryVectorFromParamsNearObject(t *testing.T) {
	// Test with NearObject - should return nil as we only support NearVector for now
	params := dto.GetParams{
		NearObject: &searchparams.NearObject{
			ID: "some-id",
		},
	}

	result := ExtractQueryVectorFromParams(params)
	
	if result != nil {
		t.Error("Expected nil result for NearObject params (not implemented)")
	}
}
