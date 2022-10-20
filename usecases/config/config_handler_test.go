//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	t.Run("invalid DefaultVectorDistanceMetric", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule:     "text2vec-contextionary",
			DefaultVectorDistanceMetric: "euclidean",
		}
		err := config.Validate(moduleProvider)
		assert.EqualError(
			t,
			err,
			"default vector distance metric: must be one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]",
		)
	})

	t.Run("invalid DefaultVectorizerModule", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule:     "contextionary",
			DefaultVectorDistanceMetric: "cosine",
		}
		err := config.Validate(moduleProvider)
		assert.EqualError(
			t,
			err,
			"default vectorizer module: invalid vectorizer \"contextionary\"",
		)
	})

	t.Run("all valid configurations", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule:     "text2vec-contextionary",
			DefaultVectorDistanceMetric: "l2-squared",
		}
		err := config.Validate(moduleProvider)
		assert.Nil(t, err, "should not error")
	})

	t.Run("without DefaultVectorDistanceMetric", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule: "text2vec-contextionary",
		}
		err := config.Validate(moduleProvider)
		assert.Nil(t, err, "should not error")
	})

	t.Run("with none DefaultVectorizerModule", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule: "none",
		}
		err := config.Validate(moduleProvider)
		assert.Nil(t, err, "should not error")
	})
}
