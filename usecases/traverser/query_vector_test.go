package traverser

import (
	"testing"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func TestExtractQueryVectorFromParams(t *testing.T) {
	tests := []struct {
		name     string
		params   dto.GetParams
		expected []float32
		wantNil  bool
	}{
		{
			name: "nearVector with float32 slice",
			params: dto.GetParams{
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3, 0.4}},
				},
			},
			expected: []float32{0.1, 0.2, 0.3, 0.4},
		},
		{
			name: "nearVector with interface slice",
			params: dto.GetParams{
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{[]interface{}{0.1, 0.2, 0.3, 0.4}},
				},
			},
			expected: []float32{0.1, 0.2, 0.3, 0.4},
		},
		{
			name:    "empty parameters",
			params:  dto.GetParams{},
			wantNil: true,
		},
		{
			name: "nearObject parameters",
			params: dto.GetParams{
				NearObject: &searchparams.NearObject{ID: "some-id"},
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractQueryVectorFromParams(tt.params)
			
			if tt.wantNil {
				if result != nil {
					t.Errorf("Expected nil result, got %v", result)
				}
				return
			}

			if result == nil {
				t.Error("Expected non-nil result")
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("Expected vector length %d, got %d", len(tt.expected), len(result))
				return
			}

			for i, v := range tt.expected {
				if result[i] != v {
					t.Errorf("Expected value %f at index %d, got %f", v, i, result[i])
				}
			}
		})
	}
}
