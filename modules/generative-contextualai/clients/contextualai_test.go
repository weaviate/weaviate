//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"net/http"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestClient_GetParameters(t *testing.T) {
	mockConfig := &mockClassConfig{
		classConfig: map[string]interface{}{
			"model":       "v1",
			"temperature": 0.5,
		},
	}

	client := &contextualai{
		apiKey:     "test-key",
		httpClient: &http.Client{Timeout: 30 * time.Second},
		logger:     logrus.New(),
	}

	params := client.getParameters(mockConfig, nil)

	assert.Equal(t, "v1", params.Model)
	assert.Equal(t, 0.5, *params.Temperature)
}

func TestClient_MetaInfo(t *testing.T) {
	client := &contextualai{}

	meta, err := client.MetaInfo()
	require.NoError(t, err)

	assert.Equal(t, "Generative Search - Contextual AI", meta["name"])
	assert.Equal(t, "https://contextual.ai/generate/", meta["documentationHref"])
}

// mockClassConfig implements moduletools.ClassConfig for testing
type mockClassConfig struct {
	classConfig map[string]interface{}
}

func (m *mockClassConfig) Class() map[string]interface{} {
	return m.classConfig
}

func (m *mockClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return m.classConfig
}

func (m *mockClassConfig) Property(propName string) map[string]interface{} {
	return map[string]interface{}{}
}

func (m *mockClassConfig) Tenant() string {
	return ""
}

func (m *mockClassConfig) TargetVector() string {
	return ""
}

func (m *mockClassConfig) Config() *config.Config {
	return nil
}

func (m *mockClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
