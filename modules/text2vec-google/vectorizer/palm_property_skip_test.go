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

package vectorizer

import (
	"context"
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	jinaent "github.com/weaviate/weaviate/modules/text2multivec-jinaai/ent"
	"github.com/weaviate/weaviate/usecases/modules"
)

// palmAltNames mirrors what the registered module advertises, so the test
// exercises the same fallback the provider configures in production.
var palmAltNames = []string{"text2vec-palm"}

func TestPalmPropertyLevelSettingsAreHonoured(t *testing.T) {
	obj := &models.Object{Properties: map[string]interface{}{
		"title":        "Hello",
		"internalNote": "do not embed this",
	}}

	tests := []struct {
		name        string
		storedKey   string
		namedVector bool
	}{
		{name: "canonical key, class vectorizer", storedKey: "text2vec-google"},
		{name: "canonical key, named vector", storedKey: "text2vec-google", namedVector: true},
		{name: "legacy alias, class vectorizer", storedKey: "text2vec-palm"},
		{name: "legacy alias, named vector", storedKey: "text2vec-palm", namedVector: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			titleCfg := map[string]interface{}{
				tc.storedKey: map[string]interface{}{"vectorizePropertyName": true},
			}
			noteCfg := map[string]interface{}{
				tc.storedKey: map[string]interface{}{"skip": true},
			}
			// Contradictory settings under the legacy key prove the canonical
			// name wins for both flags. The alias rows stay legacy-only, which is
			// what a collection created before the rename actually looks like.
			if tc.storedKey == "text2vec-google" {
				titleCfg["text2vec-palm"] = map[string]interface{}{"vectorizePropertyName": false}
				noteCfg["text2vec-palm"] = map[string]interface{}{"skip": false}
			}

			class := &models.Class{
				Class: "Doc",
				Properties: []*models.Property{
					{
						Name: "title", DataType: []string{"text"},
						ModuleConfig: titleCfg,
					},
					{
						Name: "internalNote", DataType: []string{"text"},
						ModuleConfig: noteCfg,
					},
				},
			}

			vectorizerCfg := map[string]interface{}{
				tc.storedKey: map[string]interface{}{"vectorizeClassName": false},
			}
			target := ""
			if tc.namedVector {
				target = "default"
				class.VectorConfig = map[string]models.VectorConfig{"default": {Vectorizer: vectorizerCfg}}
			} else {
				class.ModuleConfig = vectorizerCfg
			}

			// The provider always asks for the canonical module name.
			cfg := modules.NewClassBasedModuleConfig(class, "text2vec-google", "", target, nil).
				WithAltNames(palmAltNames)

			client := &fakeClient{}
			if _, _, err := New(client).Object(context.Background(), obj, cfg); err != nil {
				t.Fatal(err)
			}

			// skip on internalNote and vectorizePropertyName on title must both
			// come from the key the class actually stored its settings under.
			if len(client.lastInput) != 1 || client.lastInput[0] != "title Hello" {
				t.Errorf("got %q, want %q", client.lastInput, "title Hello")
			}
		})
	}
}

// Settings stored only under a legacy key must not leak into a class that also
// has canonical settings for that property.
func TestPropertySettingsCanonicalKeyWinsOverAlias(t *testing.T) {
	class := &models.Class{
		Class: "Doc",
		Properties: []*models.Property{
			{
				Name: "title", DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					"text2vec-google": map[string]interface{}{"skip": false},
					"text2vec-palm":   map[string]interface{}{"skip": true},
				},
			},
			{
				Name: "internalNote", DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					"text2vec-palm": map[string]interface{}{"skip": true},
				},
			},
		},
	}

	cfg := modules.NewClassBasedModuleConfig(class, "text2vec-google", "", "", nil).
		WithAltNames(palmAltNames)

	if got := cfg.Property("title")["skip"]; got != false {
		t.Errorf("canonical skip must win, got %v", got)
	}
	if got := cfg.Property("internalNote")["skip"]; got != true {
		t.Errorf("alias skip must be used when canonical is absent, got %v", got)
	}
}

// Without registered alt names the lookup stays canonical-only, so an unrelated
// module's key is never read by accident.
func TestPropertyLookupWithoutAltNamesStaysCanonical(t *testing.T) {
	class := &models.Class{
		Class: "Doc",
		Properties: []*models.Property{
			{
				Name: "title", DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					"text2vec-palm": map[string]interface{}{"skip": true},
				},
			},
		},
	}

	cfg := modules.NewClassBasedModuleConfig(class, "text2vec-google", "", "", nil)
	if _, ok := cfg.Property("title")["skip"]; ok {
		t.Error("expected no settings without registered alt names")
	}
}

// The same fallback has to work for every module that still declares a legacy
// name, not just text2vec-google: a text2colbert-jinaai collection stores its
// property settings under the old key, and a missed lookup would send a
// skip: true property to the vectorizer.
func TestJinaLegacyAliasHonoursPropertySkip(t *testing.T) {
	class := &models.Class{
		Class: "Doc",
		Properties: []*models.Property{
			{
				Name: "internalNote", DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					"text2colbert-jinaai": map[string]interface{}{"skip": true},
				},
			},
			{Name: "title", DataType: []string{"text"}},
		},
	}

	cfg := modules.NewClassBasedModuleConfig(class, "text2multivec-jinaai", "", "", nil).
		WithAltNames([]string{"text2colbert-jinaai"})

	settings := jinaent.NewClassSettings(cfg)
	if settings.PropertyIndexed("internalNote") {
		t.Error("property stored under the legacy jina key must not be vectorized")
	}
	if !settings.PropertyIndexed("title") {
		t.Error("property without settings must stay indexed")
	}
}
