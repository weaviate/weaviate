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
	nested2 "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested2"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// BenchmarkNestedMetaEntries2_Flat measures the write-path pipeline for a flat
// document (10 scalar properties + 1 scalar array with 5 elements):
// AssignPositionsFromSchema → nestedMetaEntries2. The schema is built once
// before the timer so only the per-call cost is measured.
//
// Note: the intended pipeline is analyzeNestedProp2 → nestedMetaEntries2, but
// analyzeNestedProp2 is unexported on *inverted.Analyzer and cannot be called
// from this package. AssignPositionsFromSchema is the allocation-heavy inner
// step that analyzeNestedProp2 delegates to, so this approximation captures
// the same yield-closure heap-allocation signal.
func BenchmarkNestedMetaEntries2_Flat(b *testing.B) {
	prop := &models.Property{
		Name:     "obj",
		DataType: []string{string(schema.DataTypeObject)},
		NestedProperties: []*models.NestedProperty{
			{Name: "f1", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f2", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f3", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f4", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f5", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f6", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f7", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f8", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f9", DataType: []string{string(schema.DataTypeText)}},
			{Name: "f10", DataType: []string{string(schema.DataTypeText)}},
			{Name: "tags", DataType: []string{string(schema.DataTypeTextArray)}},
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

	ls, err := nested2.BuildSchema(prop)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := nested2.AssignPositionsFromSchema(ls, prop, value)
		if err != nil {
			b.Fatal(err)
		}
		np := inverted.NewNestedProperty2ForTest("obj", result, nil)
		_ = nestedMetaEntries2(*np, 1)
	}
}

// BenchmarkNestedMetaEntries2_Deep measures the write-path pipeline for a deep
// 3-level nested document (10 countries × 5 garages × 20 cars): same pipeline
// as BenchmarkNestedMetaEntries2_Flat (AssignPositionsFromSchema → nestedMetaEntries2).
// analyzeNestedProp2 is unexported; see BenchmarkNestedMetaEntries2_Flat for the
// deviation rationale. Document is large enough that per-element closure
// heap-allocations produce a clearly visible allocs/op increase.
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
							{Name: "make", DataType: []string{string(schema.DataTypeText)}},
							{Name: "model", DataType: []string{string(schema.DataTypeText)}},
							{Name: "color", DataType: []string{string(schema.DataTypeText)}},
							{Name: "features", DataType: []string{string(schema.DataTypeTextArray)}},
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

	ls, err := nested2.BuildSchema(prop)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := nested2.AssignPositionsFromSchema(ls, prop, countries)
		if err != nil {
			b.Fatal(err)
		}
		np := inverted.NewNestedProperty2ForTest("countries", result, nil)
		_ = nestedMetaEntries2(*np, 1)
	}
}
