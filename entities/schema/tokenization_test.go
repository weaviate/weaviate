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

package schema

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
)

func TestIsValidTokenization(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{"word", models.PropertyTokenizationWord, true},
		{"lowercase", models.PropertyTokenizationLowercase, true},
		{"whitespace", models.PropertyTokenizationWhitespace, true},
		{"field", models.PropertyTokenizationField, true},
		{"trigram", models.PropertyTokenizationTrigram, true},
		{"gse", models.PropertyTokenizationGse, true},
		{"kagome_kr", models.PropertyTokenizationKagomeKr, true},
		{"kagome_ja", models.PropertyTokenizationKagomeJa, true},
		{"gse_ch", models.PropertyTokenizationGseCh, true},
		{"empty", "", false},
		{"unknown", "wat", false},
		{"case-sensitive", "WORD", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsValidTokenization(tc.value); got != tc.want {
				t.Fatalf("IsValidTokenization(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

func TestAllTokenizationsContainsAllModelConstants(t *testing.T) {
	// All 9 tokenization constants exported by models must appear in
	// AllTokenizations. This guards against drift when a new constant
	// is added to entities/models/property.go.
	want := []string{
		models.PropertyTokenizationWord,
		models.PropertyTokenizationLowercase,
		models.PropertyTokenizationWhitespace,
		models.PropertyTokenizationField,
		models.PropertyTokenizationTrigram,
		models.PropertyTokenizationGse,
		models.PropertyTokenizationKagomeKr,
		models.PropertyTokenizationKagomeJa,
		models.PropertyTokenizationGseCh,
	}
	if len(AllTokenizations) != len(want) {
		t.Fatalf("AllTokenizations length = %d, want %d", len(AllTokenizations), len(want))
	}
	have := make(map[string]struct{}, len(AllTokenizations))
	for _, v := range AllTokenizations {
		have[v] = struct{}{}
	}
	for _, w := range want {
		if _, ok := have[w]; !ok {
			t.Errorf("AllTokenizations missing %q", w)
		}
	}
}
