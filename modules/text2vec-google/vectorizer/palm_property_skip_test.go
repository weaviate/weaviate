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
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestPalmPropertyLevelSkipIsIgnored(t *testing.T) {
	obj := &models.Object{Properties: map[string]interface{}{
		"title":        "Hello",
		"internalNote": "do not embed this",
	}}
	vectorizerCfg := func(key string) map[string]interface{} {
		return map[string]interface{}{key: map[string]interface{}{"vectorizeClassName": false}}
	}

	for _, key := range []string{"text2vec-google", "text2vec-palm"} {
		for _, namedVector := range []bool{false, true} {
			titleCfg := map[string]interface{}{
				key: map[string]interface{}{"vectorizePropertyName": true},
			}
			noteCfg := map[string]interface{}{
				key: map[string]interface{}{"skip": true},
			}
			// Google rows also store the opposite flags under the legacy key.
			// The canonical name must win for both skip and vectorizePropertyName.
			// Palm rows stay legacy-only.
			if key == "text2vec-google" {
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
			target := ""
			if namedVector {
				target = "default"
				class.VectorConfig = map[string]models.VectorConfig{"default": {Vectorizer: vectorizerCfg(key)}}
			} else {
				class.ModuleConfig = vectorizerCfg(key)
			}
			// the provider passes the canonical module name for either key
			cfg := modules.NewClassBasedModuleConfig(class, "text2vec-google", "", target, nil)

			client := &fakeClient{}
			if _, _, err := New(client).Object(context.Background(), obj, cfg); err != nil {
				t.Fatal(err)
			}
			t.Logf("key=%-16s namedVector=%-5t text sent: %q", key, namedVector, client.lastInput)
			if len(client.lastInput) != 1 || client.lastInput[0] != "title Hello" {
				t.Errorf("key=%s namedVector=%t: got %q, want %q", key, namedVector, client.lastInput, "title Hello")
			}
		}
	}
}
