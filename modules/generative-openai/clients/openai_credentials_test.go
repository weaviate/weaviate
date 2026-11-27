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
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/modules/generative-openai/config"
	openaiparams "github.com/weaviate/weaviate/modules/generative-openai/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerCredentialsOnlyGoToTheConfiguredHost(t *testing.T) {
	const (
		serverKey      = "server-openai-key"
		serverAzureKey = "server-azure-key"
		callerKey      = "caller-key"
	)
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}
	server := credentialtest.NewServer(t, generateResponse{
		Choices: []choice{{Message: &responseMessage{Role: "assistant", Content: "ok"}}},
	})
	tests := []struct {
		name         string
		classConfig  map[string]any
		options      *openaiparams.Params
		headers      map[string]string
		wantErr      string
		wantAuth     string
		wantAzureKey string
		wantHost     string
	}{
		{
			name:    "query baseURL without a request key",
			options: &openaiparams.Params{BaseURL: server.URL},
			wantErr: "a URL set in the query needs X-Openai-Api-Key: the server's key is sent only to the configured URL",
		},
		{
			name:     "query baseURL with a request key",
			options:  &openaiparams.Params{BaseURL: server.URL},
			headers:  map[string]string{"X-Openai-Api-Key": callerKey},
			wantAuth: "Bearer " + callerKey,
			wantHost: server.HostPort(),
		},
		{
			name:    "baseURL header without a request key",
			headers: map[string]string{"X-Openai-Baseurl": server.URL},
			wantErr: "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is sent only to the configured URL",
		},
		{
			name:        "class baseURL gets the server key",
			classConfig: map[string]any{"baseURL": server.URL},
			wantAuth:    "Bearer " + serverKey,
			wantHost:    server.HostPort(),
		},
		{
			name:    "Azure query baseURL without a request key",
			options: &openaiparams.Params{IsAzure: true, DeploymentID: "d", BaseURL: server.URL},
			wantErr: "a URL set in the query needs X-Azure-Api-Key",
		},
		{
			name:        "Azure query resourceName without a request key",
			classConfig: map[string]any{"resourceName": "corp", "deploymentId": "d"},
			options:     &openaiparams.Params{ResourceName: "other"},
			wantErr:     "resourceName needs X-Azure-Api-Key: the server's key is sent only to the configured URL",
		},
		{
			name: "Azure from headers alone uses the Azure request key",
			headers: map[string]string{
				"X-Azure-Resource-Name": "callers-resource", "X-Azure-Deployment-Id": "d", "X-Azure-Api-Key": callerKey,
			},
			wantAzureKey: callerKey,
			wantHost:     "callers-resource.openai.azure.com",
		},
		{
			name:        "Azure headers on a non-Azure class without a request key",
			classConfig: map[string]any{"baseURL": server.URL},
			headers:     map[string]string{"X-Azure-Resource-Name": "r", "X-Azure-Deployment-Id": "d"},
			wantErr:     "a request that turns on Azure needs X-Azure-Api-Key: the server's key is sent only to the configured URL",
		},
		{
			name:        "query isAzure on a non-Azure class without a request key",
			classConfig: map[string]any{"baseURL": server.URL},
			options:     &openaiparams.Params{IsAzure: true},
			wantErr:     "a request that turns on Azure needs X-Azure-Api-Key",
		},
		{
			name:        "Azure deployment header on a class with only a resourceName without a request key",
			classConfig: map[string]any{"baseURL": server.URL, "resourceName": "corp"},
			headers:     map[string]string{"X-Azure-Deployment-Id": "d"},
			wantErr:     "a request that turns on Azure needs X-Azure-Api-Key",
		},
		{
			name:         "Azure class with a baseURL gets the server key",
			classConfig:  map[string]any{"baseURL": server.URL, "isAzure": true, "deploymentId": "d"},
			wantAzureKey: serverAzureKey,
			wantHost:     server.HostPort(),
		},
		{
			name:         "Azure class resourceName gets the server key",
			classConfig:  map[string]any{"resourceName": "corp", "deploymentId": "d"},
			wantAzureKey: serverAzureKey,
			wantHost:     "corp.openai.azure.com",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, "", serverAzureKey, 0, nullLogger())
			c.httpClient = server.ClientForAnyHost()

			classConfig := credentialtest.ClassConfig{"model": "test-model"}
			for k, v := range tt.classConfig {
				classConfig[k] = v
			}
			var options any
			if tt.options != nil {
				options = *tt.options
			}

			_, err := c.GenerateSingleResult(credentialtest.Context(tt.headers), props, "What is my name?", options, false, classConfig)

			requests := server.Requests()[before:]
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, requests, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, requests, 1)
			assert.Equal(t, tt.wantHost, requests[0].Host)
			assert.Equal(t, tt.wantAuth, requests[0].Header.Get("Authorization"))
			assert.Equal(t, tt.wantAzureKey, requests[0].Header.Get("api-key"))
		})
	}
}

func TestResourceNameThatIsNotAHostLabelSendsNoRequest(t *testing.T) {
	const callerKey = "caller-key"
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}
	server := credentialtest.NewServer(t, generateResponse{
		Choices: []choice{{Message: &responseMessage{Role: "assistant", Content: "ok"}}},
	})

	for _, resourceName := range []string{"attacker.example#", "attacker.example/", "attacker.example?", "10.0.0.5:8443/x#"} {
		tests := []struct {
			name        string
			classConfig map[string]any
			options     *openaiparams.Params
			headers     map[string]string
		}{
			{
				name:        "class resourceName",
				classConfig: map[string]any{"resourceName": resourceName, "deploymentId": "d"},
			},
			{
				name:    "query resourceName with a request key",
				options: &openaiparams.Params{IsAzure: true, ResourceName: resourceName, DeploymentID: "d"},
				headers: map[string]string{"X-Azure-Api-Key": callerKey},
			},
			{
				name: "resourceName header with a request key",
				headers: map[string]string{
					"X-Azure-Resource-Name": resourceName, "X-Azure-Deployment-Id": "d", "X-Azure-Api-Key": callerKey,
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name+" "+resourceName, func(t *testing.T) {
				before := len(server.Requests())
				c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())
				c.httpClient = server.ClientForAnyHost()

				classConfig := credentialtest.ClassConfig{"model": "test-model"}
				for k, v := range tt.classConfig {
					classConfig[k] = v
				}
				var options any
				if tt.options != nil {
					options = *tt.options
				}

				_, err := c.GenerateSingleResult(credentialtest.Context(tt.headers), props, "What is my name?", options, false, classConfig)

				require.Error(t, err)
				assert.Contains(t, err.Error(), "resourceName must be 1 to 63 letters, digits or hyphens")
				var precondition enterrors.ErrModulePrecondition
				assert.ErrorAs(t, err, &precondition, "REST search returns the reason with a 422 only for this type")
				assert.Empty(t, server.Requests()[before:], "no request may leave Weaviate")
			})
		}
	}
}

// The Azure host comes from the resource name only when the base URL is the
// OpenAI default, so a variant of the default must not move the request to
// api.openai.com with the server's Azure key.
func TestAzureDefaultBaseURLVariantsKeepTheResourceHost(t *testing.T) {
	classConfig := credentialtest.ClassConfig{"resourceName": "corp", "deploymentId": "d"}
	tests := []struct {
		name    string
		options openaiparams.Params
		headers map[string]string
		wantErr string
	}{
		{name: "baseURL header with a trailing slash", headers: map[string]string{"X-Openai-Baseurl": "https://api.openai.com/"}},
		{name: "baseURL header with an upper-case host", headers: map[string]string{"X-Openai-Baseurl": "https://API.openai.com"}},
		{name: "query baseURL with a trailing slash", options: openaiparams.Params{BaseURL: "https://api.openai.com/"}},
		{
			name:    "baseURL header with a path",
			headers: map[string]string{"X-Openai-Baseurl": "https://api.openai.com/v1"},
			wantErr: "X-Openai-Baseurl needs X-Azure-Api-Key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())
			params := c.getParameters(classConfig, tt.options, nil)

			endpoint, isAzure, err := c.buildOpenAIUrl(credentialtest.Context(tt.headers), params, config.NewClassSettings(classConfig))

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.True(t, isAzure)
			assert.True(t, strings.HasPrefix(endpoint, "https://corp.openai.azure.com/"), endpoint)
		})
	}
}

// With an Azure gateway base URL such as https://gw.example/team-a, a
// deployment ID of ../team-b would reach another team's path with the server's
// Azure key.
func TestDeploymentIDThatIsNotOnePathSegmentSendsNoRequest(t *testing.T) {
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}
	server := credentialtest.NewServer(t, generateResponse{
		Choices: []choice{{Message: &responseMessage{Role: "assistant", Content: "ok"}}},
	})
	gateway := server.URL + "/team-a"

	for _, deploymentID := range []string{"gpt-4.1-mini", "../../../team-b/x", "%2e%2e/%2e%2e/%2e%2e/team-b/x", "..", "..;/team-b", "d?x"} {
		valid := deploymentID == "gpt-4.1-mini"
		tests := []struct {
			name        string
			classConfig map[string]any
			options     *openaiparams.Params
			headers     map[string]string
		}{
			{
				name:        "class deploymentId",
				classConfig: map[string]any{"baseURL": gateway, "isAzure": true, "deploymentId": deploymentID},
			},
			{
				name:        "query deploymentId",
				classConfig: map[string]any{"baseURL": gateway, "isAzure": true, "deploymentId": "d"},
				options:     &openaiparams.Params{DeploymentID: deploymentID},
			},
			{
				name:        "deployment header",
				classConfig: map[string]any{"baseURL": gateway, "isAzure": true, "deploymentId": "d"},
				headers:     map[string]string{"X-Azure-Deployment-Id": deploymentID},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name+" "+deploymentID, func(t *testing.T) {
				before := len(server.Requests())
				c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())
				c.httpClient = server.ClientForAnyHost()

				classConfig := credentialtest.ClassConfig{"model": "test-model"}
				for k, v := range tt.classConfig {
					classConfig[k] = v
				}
				var options any
				if tt.options != nil {
					options = *tt.options
				}

				_, err := c.GenerateSingleResult(credentialtest.Context(tt.headers), props, "What is my name?", options, false, classConfig)

				got := server.Requests()[before:]
				if valid {
					require.NoError(t, err)
					require.Len(t, got, 1)
					assert.Equal(t, "/team-a/openai/deployments/"+deploymentID+"/chat/completions", got[0].URL.Path)
					return
				}
				require.Error(t, err)
				assert.Contains(t, err.Error(), "deploymentId must be letters, digits, hyphens, underscores, periods or tildes")
				var precondition enterrors.ErrModulePrecondition
				assert.ErrorAs(t, err, &precondition, "REST search returns the reason with a 422 only for this type")
				assert.Empty(t, got, "no request may leave Weaviate")
			})
		}
	}
}
