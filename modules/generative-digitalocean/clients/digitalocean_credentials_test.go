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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	digitaloceanparams "github.com/weaviate/weaviate/modules/generative-digitalocean/parameters"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerKeyOnlyGoesToTheConfiguredHost(t *testing.T) {
	const (
		serverKey  = "server-key"
		serverUUID = "server-uuid"
		callerKey  = "caller-key"
	)
	t.Setenv("VECTOR_DB_UUID", serverUUID)
	serverUserAgent := strings.TrimSpace("vector-db/weaviate/" + serverUUID + " " + build.Version)
	anonymousUserAgent := strings.TrimSpace("vector-db/weaviate/unknown " + build.Version)
	server := credentialtest.NewServer(t, chatResp{
		Choices: []choice{{Message: chatMessage{Role: "assistant", Content: "ok"}}},
	})
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}

	tests := []struct {
		name          string
		classConfig   credentialtest.ClassConfig
		options       *digitaloceanparams.Params
		headers       map[string]string
		wantErr       string
		wantAuth      string
		wantUserAgent string
	}{
		{
			name:    "query baseURL without a request key",
			options: &digitaloceanparams.Params{BaseURL: server.URL},
			wantErr: "baseURL needs X-Digitalocean-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:    "baseURL header without a request key",
			headers: map[string]string{"X-Digitalocean-Baseurl": server.URL},
			wantErr: "X-Digitalocean-Baseurl needs X-Digitalocean-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:          "query baseURL with a request key",
			options:       &digitaloceanparams.Params{BaseURL: server.URL},
			headers:       map[string]string{"X-Digitalocean-Api-Key": callerKey},
			wantAuth:      "Bearer " + callerKey,
			wantUserAgent: anonymousUserAgent,
		},
		{
			name:          "baseURL header with a request key",
			headers:       map[string]string{"X-Digitalocean-Baseurl": server.URL, "X-Digitalocean-Api-Key": callerKey},
			wantAuth:      "Bearer " + callerKey,
			wantUserAgent: anonymousUserAgent,
		},
		{
			name:          "class baseURL gets the server key and UUID",
			classConfig:   credentialtest.ClassConfig{"baseURL": server.URL},
			wantAuth:      "Bearer " + serverKey,
			wantUserAgent: serverUserAgent,
		},
		{
			name:          "query baseURL equal to the class baseURL gets the server key and UUID",
			classConfig:   credentialtest.ClassConfig{"baseURL": server.URL},
			options:       &digitaloceanparams.Params{BaseURL: server.URL},
			wantAuth:      "Bearer " + serverKey,
			wantUserAgent: serverUserAgent,
		},
		{
			name:          "request key to the class baseURL gets the server UUID",
			classConfig:   credentialtest.ClassConfig{"baseURL": server.URL},
			headers:       map[string]string{"X-Digitalocean-Api-Key": callerKey},
			wantAuth:      "Bearer " + callerKey,
			wantUserAgent: serverUserAgent,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, 0, nullLogger())
			c.httpClient = server.Client()
			var options any
			if tt.options != nil {
				options = *tt.options
			}

			_, err := c.GenerateSingleResult(credentialtest.Context(tt.headers), props, "What is my name?", options, false, tt.classConfig)

			got := server.Requests()[before:]
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, got, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, tt.wantAuth, got[0].Header.Get("Authorization"))
			assert.Equal(t, tt.wantUserAgent, got[0].Header.Get("User-Agent"), "the server UUID only goes to the configured host")
		})
	}
}
