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

import "github.com/weaviate/weaviate/entities/models"

// AllTokenizations is the canonical list of tokenization names that
// Weaviate recognises for text / text[] properties. This is the union
// of every value the schema layer accepts; whether a given tokenizer is
// actually usable at runtime can additionally depend on env-gating
// (ENABLE_TOKENIZER_*), which is enforced separately by the schema
// validator and the tokenizer package.
//
// This list MUST stay in sync with the switch in
// (*Handler).validatePropertyTokenization (usecases/schema/class.go).
// Use IsValidTokenization for membership checks instead of duplicating
// the list at call sites.
var AllTokenizations = []string{
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

// validTokenizationsSet is a set built from AllTokenizations for O(1)
// membership lookups in IsValidTokenization.
var validTokenizationsSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(AllTokenizations))
	for _, t := range AllTokenizations {
		m[t] = struct{}{}
	}
	return m
}()

// IsValidTokenization reports whether t is one of the canonical
// tokenization values defined in AllTokenizations. It performs a pure
// name-based check and does NOT consider env-gating (e.g. whether the
// gse_ch tokenizer is enabled at runtime). Use this for input
// validation; rely on the schema validator for full semantic checks.
func IsValidTokenization(t string) bool {
	_, ok := validTokenizationsSet[t]
	return ok
}
