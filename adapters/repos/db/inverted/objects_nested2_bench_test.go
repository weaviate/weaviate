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

package inverted

import (
	"testing"

	nested2 "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested2"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// BenchmarkAnalyzeNestedProp2_Flat measures the full analyzeNestedProp2
// pipeline for a flat document (10 scalar text properties + 1 text array with
// 5 elements). LevelSchema is built once before the timer; only the per-call
// cost of analyze + value tokenization is measured.
//
// This is the site where the analyze-step alloc profile (configs map,
// Values slice, structural allocs) can be measured end-to-end.
func BenchmarkAnalyzeNestedProp2_Flat(b *testing.B) {
	prop := &models.Property{
		Name:     "obj",
		DataType: schema.DataTypeObject.PropString(),
		NestedProperties: []*models.NestedProperty{
			{Name: "f1", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f2", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f3", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f4", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f5", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f6", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f7", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f8", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f9", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "f10", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
			{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
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
	a := NewAnalyzer(nil, "Bench")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		np, err := a.analyzeNestedProp2(ls, prop, value)
		if err != nil {
			b.Fatal(err)
		}
		_ = np
	}
}

// BenchmarkAnalyzeNestedProp2_Deep measures the full analyzeNestedProp2
// pipeline for a deep 3-level nested document (10 countries × 5 garages ×
// 20 cars, each car with make/model/color text scalars and a 3-element
// features text array). LevelSchema is built once before the timer.
//
// With 1000 cars × 4 leaf properties the configs map and Values slice
// construction dominate; the allocs/op here is the baseline above which
// nestedMetaEntries2 adds its entry-building cost.
func BenchmarkAnalyzeNestedProp2_Deep(b *testing.B) {
	prop := &models.Property{
		Name:     "countries",
		DataType: schema.DataTypeObjectArray.PropString(),
		NestedProperties: []*models.NestedProperty{
			{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:     "cars",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
							{Name: "model", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
							{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
							{Name: "features", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
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
	a := NewAnalyzer(nil, "Bench")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		np, err := a.analyzeNestedProp2(ls, prop, countries)
		if err != nil {
			b.Fatal(err)
		}
		_ = np
	}
}
