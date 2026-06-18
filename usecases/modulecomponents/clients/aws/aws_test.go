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

package aws

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	t.Run("when X-Aws-Access-Key header is passed but empty", func(t *testing.T) {
		c := New("", "123", "", 1*time.Second, nullLogger())
		ctxWithValue := context.WithValue(context.Background(),
			"X-Aws-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, Settings{
			Service: "bedrock",
		})

		require.NotNil(t, err)
		assert.Equal(t, "AWS Access Key: no access key found neither in request header: X-AWS-Access-Key nor in environment variable under AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY", err.Error())
	})

	t.Run("when X-Aws-Secret-Key header is passed but empty", func(t *testing.T) {
		c := New("123", "", "", 1*time.Second, nullLogger())
		ctxWithValue := context.WithValue(context.Background(),
			"X-Aws-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, Settings{
			Service: "bedrock",
		})

		require.NotNil(t, err)
		assert.Equal(t, "AWS Secret Key: no secret found neither in request header: X-AWS-Secret-Key nor in environment variable under AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY", err.Error())
	})
}

func TestCreateRequestBody(t *testing.T) {
	input := []string{"Hello, world!"}
	c := New("123", "", "", 1*time.Second, nullLogger())
	req := c.createCohereBody(input, nil, Settings{OperationType: Document})
	assert.Equal(t, "search_document", req.InputType)
	req = c.createCohereBody(input, nil, Settings{OperationType: Query})
	assert.Equal(t, "search_query", req.InputType)
}

func TestCreateAmazonTitanBody(t *testing.T) {
	c := New("123", "", "", 1*time.Second, nullLogger())
	dims := int64(512)

	tests := []struct {
		name              string
		model             string
		dimensions        *int64
		wantDimensions    *int64
		wantEmbeddingConf *int64
	}{
		{
			name:           "titan text v2 with dimensions uses top-level field",
			model:          "amazon.titan-embed-text-v2:0",
			dimensions:     &dims,
			wantDimensions: &dims,
		},
		{
			name:              "titan multimodal with dimensions uses embeddingConfig",
			model:             "amazon.titan-embed-image-v1",
			dimensions:        &dims,
			wantEmbeddingConf: &dims,
		},
		{
			name:       "titan text v1 without dimensions sets neither",
			model:      "amazon.titan-embed-text-v1",
			dimensions: nil,
		},
		{
			name:              "titan text v1 with dimensions uses embeddingConfig",
			model:             "amazon.titan-embed-text-v1",
			dimensions:        &dims,
			wantEmbeddingConf: &dims,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := c.createAmazonTitanBody("Hello, world!", "", Settings{
				Model:      tt.model,
				Dimensions: tt.dimensions,
			})
			assert.Equal(t, tt.wantDimensions, req.Dimensions)
			if tt.wantEmbeddingConf == nil {
				assert.Nil(t, req.EmbeddingConfig)
			} else {
				require.NotNil(t, req.EmbeddingConfig)
				assert.Equal(t, tt.wantEmbeddingConf, req.EmbeddingConfig.OutputEmbeddingLength)
			}
		})
	}
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
