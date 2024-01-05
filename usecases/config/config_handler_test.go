//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	t.Run("parse config.yaml file", func(t *testing.T) {
		configFileName := "config.yaml"
		configYaml := `authentication:
  apikey:
    enabled: true
    allowed_keys:
      - api-key-1
    users:
      - readonly@weaviate.io`

		filepath := fmt.Sprintf("%s/%s", t.TempDir(), configFileName)
		f, err := os.Create(filepath)
		require.Nil(t, err)
		defer f.Close()
		_, err2 := f.WriteString(configYaml)
		require.Nil(t, err2)

		file, err := os.ReadFile(filepath)
		require.Nil(t, err)
		weaviateConfig := &WeaviateConfig{}
		config, err := weaviateConfig.parseConfigFile(file, configFileName)
		require.Nil(t, err)

		assert.True(t, config.Authentication.APIKey.Enabled)
		assert.ElementsMatch(t, []string{"api-key-1"}, config.Authentication.APIKey.AllowedKeys)
		assert.ElementsMatch(t, []string{"readonly@weaviate.io"}, config.Authentication.APIKey.Users)
	})

	t.Run("parse config.yaml file with admin_list and read_only_users", func(t *testing.T) {
		configFileName := "config.yaml"
		configYaml := `authorization:
  admin_list:
    enabled: true
    users:
      - userA
    read_only_users:
      - userA@read.only
      - userB@read.only`

		filepath := fmt.Sprintf("%s/%s", t.TempDir(), configFileName)
		f, err := os.Create(filepath)
		require.Nil(t, err)
		defer f.Close()
		_, err2 := f.WriteString(configYaml)
		require.Nil(t, err2)

		file, err := os.ReadFile(filepath)
		require.Nil(t, err)
		weaviateConfig := &WeaviateConfig{}
		config, err := weaviateConfig.parseConfigFile(file, configFileName)
		require.Nil(t, err)

		assert.True(t, config.Authorization.AdminList.Enabled)
		assert.ElementsMatch(t, []string{"userA"}, config.Authorization.AdminList.Users)
		assert.ElementsMatch(t, []string{"userA@read.only", "userB@read.only"}, config.Authorization.AdminList.ReadOnlyUsers)
	})

	t.Run("parse config.yaml file multiple keys and users", func(t *testing.T) {
		configFileName := "config.yaml"
		configYaml := `authentication:
  apikey:
    enabled: true
    allowed_keys:
      - api-key-1
      - api-key-2
      - api-key-3
    users:
      - user1@weaviate.io
      - user2@weaviate.io`

		filepath := fmt.Sprintf("%s/%s", t.TempDir(), configFileName)
		f, err := os.Create(filepath)
		require.Nil(t, err)
		defer f.Close()
		_, err2 := f.WriteString(configYaml)
		require.Nil(t, err2)

		file, err := os.ReadFile(filepath)
		require.Nil(t, err)
		weaviateConfig := &WeaviateConfig{}
		config, err := weaviateConfig.parseConfigFile(file, configFileName)
		require.Nil(t, err)

		assert.True(t, config.Authentication.APIKey.Enabled)
		assert.ElementsMatch(t, []string{"api-key-1", "api-key-2", "api-key-3"}, config.Authentication.APIKey.AllowedKeys)
		assert.ElementsMatch(t, []string{"user1@weaviate.io", "user2@weaviate.io"}, config.Authentication.APIKey.Users)
	})
}
