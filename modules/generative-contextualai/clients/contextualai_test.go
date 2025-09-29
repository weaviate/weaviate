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
)

func TestClientGetParameters(t *testing.T) {
	fakeConfig := fakeClassConfig{
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

	params := client.getParameters(fakeConfig, nil)

	assert.Equal(t, "v1", params.Model)
	assert.Equal(t, 0.5, *params.Temperature)
}

func TestClientMetaInfo(t *testing.T) {
	client := &contextualai{}

	meta, err := client.MetaInfo()
	require.NoError(t, err)

	assert.Equal(t, "Generative Search - Contextual AI", meta["name"])
	assert.Equal(t, "https://contextual.ai/generate/", meta["documentationHref"])
}
