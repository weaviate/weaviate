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

package nested2

import (
	"testing"
)

// BenchmarkAssignPositions_Flat measures AssignPositions on a flat object:
// 10 plain scalar properties and one scalar array with 5 elements.
// This is the baseline for a single-level, non-nested document.
func BenchmarkAssignPositions_Flat(b *testing.B) {
	prop := topLevelObject("obj",
		textProp("f1"),
		textProp("f2"),
		textProp("f3"),
		textProp("f4"),
		textProp("f5"),
		textProp("f6"),
		textProp("f7"),
		textProp("f8"),
		textProp("f9"),
		textProp("f10"),
		textArrayProp("tags"),
	)
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

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = AssignPositions(prop, value)
	}
}

// BenchmarkAssignPositions_Deep measures AssignPositions on a 3-level nested
// array: countries[10] × garages[5] × cars[20]. Each car has 3 plain scalar
// properties (make, model, color) and one scalar array (features, 3 elements).
// This is the load-bearing benchmark for multi-level documents; the dominant
// cost is per-element path string allocation, which later steps optimize away.
func BenchmarkAssignPositions_Deep(b *testing.B) {
	prop := topLevelObjectArray("countries",
		objectArrayProp("garages",
			objectArrayProp("cars",
				textProp("make"),
				textProp("model"),
				textProp("color"),
				textArrayProp("features"),
			),
		),
	)

	// Build 10 × 5 × 20 fixture once before starting the timer.
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

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = AssignPositions(prop, countries)
	}
}

// BenchmarkAssignPositions_Flat_Cached measures AssignPositionsFromSchema on the
// flat fixture. The schema is built once with BuildSchema before the timer starts,
// so the benchmark measures only the per-call cost of walking the object against a
// cached schema — the OPT-A improvement path.
func BenchmarkAssignPositions_Flat_Cached(b *testing.B) {
	prop := topLevelObject("obj",
		textProp("f1"),
		textProp("f2"),
		textProp("f3"),
		textProp("f4"),
		textProp("f5"),
		textProp("f6"),
		textProp("f7"),
		textProp("f8"),
		textProp("f9"),
		textProp("f10"),
		textArrayProp("tags"),
	)
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

	ls, err := BuildSchema(prop)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = AssignPositionsFromSchema(ls, prop, value)
	}
}

// BenchmarkAssignPositions_Deep_Cached measures AssignPositionsFromSchema on the
// deep 3-level nested fixture. The schema is built once with BuildSchema before the
// timer starts, so the benchmark measures only the per-call cost — the OPT-A
// improvement path that the uncached benchmark cannot isolate.
func BenchmarkAssignPositions_Deep_Cached(b *testing.B) {
	prop := topLevelObjectArray("countries",
		objectArrayProp("garages",
			objectArrayProp("cars",
				textProp("make"),
				textProp("model"),
				textProp("color"),
				textArrayProp("features"),
			),
		),
	)

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

	ls, err := BuildSchema(prop)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = AssignPositionsFromSchema(ls, prop, countries)
	}
}
