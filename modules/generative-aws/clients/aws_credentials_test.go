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

package clients

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	awsparams "github.com/weaviate/weaviate/modules/generative-aws/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestSignedRequestStaysOnAWS(t *testing.T) {
	server := credentialtest.NewServer(t, map[string]any{"generated_text": "ok"})
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}

	tests := []struct {
		name    string
		options awsparams.Params
	}{
		// buildSagemakerUrl turns these into https://runtime.sagemaker.x@<fake>/.amazonaws.com/...
		{name: "region with userinfo", options: awsparams.Params{Service: "sagemaker", Region: "x@" + server.HostPort() + "/", Endpoint: "e"}},
		{name: "region with a fragment", options: awsparams.Params{Service: "sagemaker", Region: "x@" + server.HostPort() + "#", Endpoint: "e"}},
		{name: "bedrock region with userinfo", options: awsparams.Params{Service: "bedrock", Region: "x@" + server.HostPort() + "/", Model: "amazon.titan-text-lite-v1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New("server-access-key-id", "server-secret", "server-session-token", 0, nullLogger())
			c.httpClient = server.Client()

			_, err := c.GenerateSingleResult(credentialtest.Context(nil), props, "What is my name?", tt.options, false,
				credentialtest.ClassConfig{"service": tt.options.Service, "region": "us-east-1", "endpoint": tt.options.Endpoint})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "region must be an AWS region name")
			assert.Empty(t, server.Requests()[before:], "no request may leave Weaviate")
		})
	}
}
