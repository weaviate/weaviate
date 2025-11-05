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

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
