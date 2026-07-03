//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// BenchmarkNestedMetaEntries2_Flat measures the full production write-path
// pipeline for a flat document (10 scalar text properties + 1 text array with
// 5 elements): Analyzer.Object → nestedMetaEntries. The analyzer and prop
// slice are built once before the timer so only the per-call cost is measured.
func BenchmarkNestedMetaEntries2_Flat(b *testing.B) {
	prop := &models.Property{
		Name:     "obj",
		DataType: []string{string(schema.DataTypeObject)},
		NestedProperties: []*models.NestedProperty{
			{Name: "f1", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f2", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f3", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f4", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f5", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f6", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f7", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f8", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f9", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "f10", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
			{Name: "tags", DataType: []string{string(schema.DataTypeTextArray)}, Tokenization: models.NestedPropertyTokenizationWord},
		},
	}
	value := map[string]any{
		"f1":   "v1",
		"f2":   "v2",
		"f3":   "v3",
		"f4":   "v4",
		"f5":   "v5",
		"f6":   "v6",
		"f7":   "v7",
		"f8":   "v8",
		"f9":   "v9",
		"f10":  "v10",
		"tags": []any{"a", "b", "c", "d", "e"},
	}

	a := inverted.NewAnalyzer(nil, "")
	propSlice := []*models.Property{prop}
	input := map[string]any{prop.Name: value}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, nestedProps, err := a.Object(input, propSlice, "00000000-0000-0000-0000-000000000001")
		if err != nil {
			b.Fatal(err)
		}
		if len(nestedProps) != 1 {
			b.Fatalf("expected 1 nested prop, got %d", len(nestedProps))
		}
		_ = nestedMetaEntries(nestedProps[0], 1)
	}
}

// BenchmarkNestedMetaEntries2_Deep measures the full production write-path
// pipeline for a deep 3-level nested document (10 countries × 5 garages ×
// 20 cars): Analyzer.Object → nestedMetaEntries. Document is large enough
// that per-element allocation costs produce a clearly visible allocs/op signal.
func BenchmarkNestedMetaEntries2_Deep(b *testing.B) {
	prop := &models.Property{
		Name:     "countries",
		DataType: []string{string(schema.DataTypeObjectArray)},
		NestedProperties: []*models.NestedProperty{
			{
				Name:     "garages",
				DataType: []string{string(schema.DataTypeObjectArray)},
				NestedProperties: []*models.NestedProperty{
					{
						Name:     "cars",
						DataType: []string{string(schema.DataTypeObjectArray)},
						NestedProperties: []*models.NestedProperty{
							{Name: "make", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
							{Name: "model", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
							{Name: "color", DataType: []string{string(schema.DataTypeText)}, Tokenization: models.NestedPropertyTokenizationWord},
							{Name: "features", DataType: []string{string(schema.DataTypeTextArray)}, Tokenization: models.NestedPropertyTokenizationWord},
						},
					},
				},
			},
		},
	}

	car := map[string]any{
		"make":     "Toyota",
		"model":    "Corolla",
		"color":    "white",
		"features": []any{"abs", "airbag", "esp"},
	}
	cars := make([]any, 20)
	for i := range cars {
		cars[i] = car
	}
	garage := map[string]any{"cars": cars}
	garages := make([]any, 5)
	for i := range garages {
		garages[i] = garage
	}
	country := map[string]any{"garages": garages}
	countries := make([]any, 10)
	for i := range countries {
		countries[i] = country
	}

	a := inverted.NewAnalyzer(nil, "")
	propSlice := []*models.Property{prop}
	input := map[string]any{prop.Name: countries}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, nestedProps, err := a.Object(input, propSlice, "00000000-0000-0000-0000-000000000001")
		if err != nil {
			b.Fatal(err)
		}
		if len(nestedProps) != 1 {
			b.Fatalf("expected 1 nested prop, got %d", len(nestedProps))
		}
		_ = nestedMetaEntries(nestedProps[0], 1)
	}
}
