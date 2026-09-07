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

package config

import (
	"testing"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/rerankertest"
)

func Test_classSettings_Validate(t *testing.T) {
	rerankertest.RunValidateTest(t, []rerankertest.ValidateTestCase{
		{
			Name: "default settings",
			Cfg: rerankertest.FakeClassConfig{
				ClassConfig: map[string]interface{}{},
			},
			WantModel:   "rerank-lite-1",
			WantBaseURL: "https://api.voyageai.com/v1",
		},
		{
			Name: "custom settings",
			Cfg: rerankertest.FakeClassConfig{
				ClassConfig: map[string]interface{}{
					"model":   "rerank-lite-1",
					"baseURL": "http://base-url.com",
				},
			},
			WantModel:   "rerank-lite-1",
			WantBaseURL: "http://base-url.com",
		},
	}, func(cfg moduletools.ClassConfig) rerankertest.SettingsUnderTest {
		return NewClassSettings(cfg)
	})
}

func Test_classSettings_ValidateBaseURL(t *testing.T) {
	rerankertest.RunSSRFValidationTest(t, DefaultBaseURL, func(baseURL string) rerankertest.SettingsUnderTest {
		return NewClassSettings(rerankertest.FakeClassConfig{
			ClassConfig: map[string]interface{}{
				"model":   DefaultVoyageAIModel,
				"baseURL": baseURL,
			},
		})
	})
}
