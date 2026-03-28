package traverser

import (
	"context"
	"reflect"
	"testing"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func TestGetClass_QueryVectorExtraction(t *testing.T) {
	// Test that vectorFromParams correctly extracts the query vector from nearVector
	// into the provided Context via dto.QueryVectorKey.
	e := &nearParamsVector{
		modulesProvider: &fakeModulesProvider{},
		search:          &fakeNearParamsSearcher{returnVec: true},
	}

	var extractedVector []float32
	ctx := context.WithValue(context.Background(), dto.QueryVectorKey, &extractedVector)

	nearVector := &searchparams.NearVector{
		Vectors: []models.Vector{[]float32{7.1, 8.2, 9.3}},
	}

	_, err := e.vectorFromParams(ctx, nearVector, nil, nil, "TestClass", "", "", 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []float32{7.1, 8.2, 9.3}
	if !reflect.DeepEqual(extractedVector, expected) {
		t.Errorf("expected extracted query vector %v, got %v", expected, extractedVector)
	}
}

func TestGetClass_QueryVectorExtraction_Modules(t *testing.T) {
	// Test extraction from module parameters (e.g. nearText)
	e := &nearParamsVector{
		modulesProvider: &fakeModulesProvider{},
		search:          &fakeNearParamsSearcher{returnVec: true},
	}

	var extractedVector []float32
	ctx := context.WithValue(context.Background(), dto.QueryVectorKey, &extractedVector)

	moduleParams := map[string]interface{}{
		"nearCustomText": &nearCustomTextParams{
			Values: []string{"test query"},
		},
	}

	// fakeModulesProvider maps nearCustomText to [1, 2, 3]
	_, err := e.vectorFromParams(ctx, nil, nil, moduleParams, "TestClass", "", "", 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := []float32{1.0, 2.0, 3.0}
	if !reflect.DeepEqual(extractedVector, expected) {
		t.Errorf("expected extracted query vector %v, got %v", expected, extractedVector)
	}
}
