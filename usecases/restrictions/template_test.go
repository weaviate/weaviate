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

package restrictions

import "testing"

func TestRenderTemplate(t *testing.T) {
	tests := []struct {
		name        string
		template    string
		restriction RestrictionName
		value       string
		allowed     []string
		want        string
	}{
		{
			name:        "empty template falls back to default",
			template:    "",
			restriction: RestrictionVectorIndexType,
			value:       "flat",
			allowed:     []string{"hnsw", "hfresh"},
			want:        "flat is not allowed for vector_index_type. Allowed values: hfresh, hnsw.",
		},
		{
			name:        "operator template with upgrade copy",
			template:    "Invalid config: {value} is not allowed on this tier, please upgrade (allowed: {allowed}).",
			restriction: RestrictionVectorIndexType,
			value:       "hnsw",
			allowed:     []string{"hfresh"},
			want:        "Invalid config: hnsw is not allowed on this tier, please upgrade (allowed: hfresh).",
		},
		{
			name:        "allowed list is alphabetically sorted regardless of input order",
			template:    "{allowed}",
			restriction: RestrictionCompression,
			value:       "pq",
			allowed:     []string{"rq-8", "bq", "none", "pq"},
			want:        "bq, none, pq, rq-8",
		},
		{
			name:        "empty allowed renders as empty placeholder",
			template:    "{value} ({allowed})",
			restriction: RestrictionCompression,
			value:       "pq",
			allowed:     nil,
			want:        "pq ()",
		},
		{
			name:        "missing placeholders pass through unchanged",
			template:    "static message",
			restriction: RestrictionCompression,
			value:       "pq",
			allowed:     []string{"rq-8"},
			want:        "static message",
		},
		{
			name:        "duplicated placeholders all render",
			template:    "{value}/{value} for {restriction}",
			restriction: RestrictionVectorIndexType,
			value:       "flat",
			allowed:     []string{"hnsw"},
			want:        "flat/flat for vector_index_type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RenderTemplate(tt.template, tt.restriction, tt.value, tt.allowed); got != tt.want {
				t.Errorf("RenderTemplate() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRenderTemplate_DoesNotMutateInput(t *testing.T) {
	allowed := []string{"rq-8", "bq", "none"}
	original := append([]string(nil), allowed...)
	_ = RenderTemplate("{allowed}", RestrictionCompression, "pq", allowed)
	for i := range allowed {
		if allowed[i] != original[i] {
			t.Fatalf("RenderTemplate mutated input slice: got %v, want %v", allowed, original)
		}
	}
}
