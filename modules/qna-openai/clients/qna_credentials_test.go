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
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerCredentialsOnlyGoToTheConfiguredHost(t *testing.T) {
	const (
		serverKey      = "server-openai-key"
		serverAzureKey = "server-azure-key"
		callerKey      = "caller-key"
	)
	server := credentialtest.NewServer(t, answersResponse{Choices: []choice{{Text: "John"}}})
	tests := []struct {
		name         string
		classConfig  map[string]any
		headers      map[string]string
		wantErr      string
		wantAuth     string
		wantAzureKey string
		wantHost     string
	}{
		{
			name:    "baseURL header without a request key",
			headers: map[string]string{"X-Openai-Baseurl": server.URL},
			wantErr: "X-Openai-Baseurl needs X-Openai-Api-Key",
		},
		{
			name:     "baseURL header with a request key",
			headers:  map[string]string{"X-Openai-Baseurl": server.URL, "X-Openai-Api-Key": callerKey},
			wantAuth: "Bearer " + callerKey,
			wantHost: server.HostPort(),
		},
		{
			name:        "class baseURL gets the server key",
			classConfig: map[string]any{"baseURL": server.URL},
			wantAuth:    "Bearer " + serverKey,
			wantHost:    server.HostPort(),
		},
		{
			name:        "Azure resourceName header without a request key",
			classConfig: map[string]any{"resourceName": "classResource", "deploymentId": "d"},
			headers:     map[string]string{"X-Azure-Resource-Name": "other"},
			wantErr:     "X-Azure-Resource-Name needs X-Azure-Api-Key",
		},
		{
			name:         "Azure resourceName header with a request key",
			classConfig:  map[string]any{"resourceName": "classResource", "deploymentId": "d"},
			headers:      map[string]string{"X-Azure-Resource-Name": "callers-resource", "X-Azure-Api-Key": callerKey},
			wantAzureKey: callerKey,
			wantHost:     "callers-resource.openai.azure.com",
		},
		{
			name:         "Azure ignores the baseURL header, which its URL does not use",
			classConfig:  map[string]any{"resourceName": "corp", "deploymentId": "d"},
			headers:      map[string]string{"X-Openai-Baseurl": "https://elsewhere.example.com"},
			wantAzureKey: serverAzureKey,
			wantHost:     "corp.openai.azure.com",
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

			_, err := c.Answer(credentialtest.Context(tt.headers), "My name is John", "What is my name?", credentialtest.ClassConfig(tt.classConfig))

			got := server.Requests()[before:]
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, got, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, tt.wantHost, got[0].Host)
			assert.Equal(t, tt.wantAuth, got[0].Header.Get("Authorization"))
			assert.Equal(t, tt.wantAzureKey, got[0].Header.Get("api-key"))
		})
	}
}

func TestResourceNameThatIsNotAHostLabelSendsNoRequest(t *testing.T) {
	server := credentialtest.NewServer(t, answersResponse{Choices: []choice{{Text: "John"}}})

	for _, resourceName := range []string{"attacker.example#", "attacker.example/", "attacker.example?", "10.0.0.5:8443/x#"} {
		tests := []struct {
			name        string
			classConfig map[string]any
			headers     map[string]string
		}{
			{
				name:        "class resourceName",
				classConfig: map[string]any{"resourceName": resourceName, "deploymentId": "d"},
			},
			{
				name:        "resourceName header with a request key",
				classConfig: map[string]any{"resourceName": "corp", "deploymentId": "d"},
				headers:     map[string]string{"X-Azure-Resource-Name": resourceName, "X-Azure-Api-Key": "caller-key"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name+" "+resourceName, func(t *testing.T) {
				before := len(server.Requests())
				c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())
				c.httpClient = server.ClientForAnyHost()

				_, err := c.Answer(credentialtest.Context(tt.headers), "My name is John", "What is my name?", credentialtest.ClassConfig(tt.classConfig))

				require.Error(t, err)
				assert.Contains(t, err.Error(), "resourceName must be 1 to 63 letters, digits or hyphens")
				var precondition enterrors.ErrModulePrecondition
				assert.ErrorAs(t, err, &precondition, "REST search returns the reason with a 422 only for this type")
				assert.Empty(t, server.Requests()[before:], "no request may leave Weaviate")
			})
		}
	}
}

// The qna-openai Azure URL always uses <resourceName>.openai.azure.com, so a
// deployment ID that leaves its path segment stays on the resource that owns
// the key. It is rejected like in the other OpenAI clients.
func TestDeploymentIDThatIsNotOnePathSegmentSendsNoRequest(t *testing.T) {
	server := credentialtest.NewServer(t, answersResponse{Choices: []choice{{Text: "John"}}})

	for _, deploymentID := range []string{"gpt-4.1-mini", "../../../team-b/x", "%2e%2e/%2e%2e/%2e%2e/team-b/x", "..", "..;/team-b", "d?x"} {
		valid := deploymentID == "gpt-4.1-mini"
		tests := []struct {
			name        string
			classConfig map[string]any
			headers     map[string]string
		}{
			{
				name:        "class deploymentId",
				classConfig: map[string]any{"resourceName": "corp", "deploymentId": deploymentID},
			},
			{
				name:        "deployment header",
				classConfig: map[string]any{"resourceName": "corp", "deploymentId": "d"},
				headers:     map[string]string{"X-Azure-Deployment-Id": deploymentID},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name+" "+deploymentID, func(t *testing.T) {
				before := len(server.Requests())
				c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())
				c.httpClient = server.ClientForAnyHost()

				_, err := c.Answer(credentialtest.Context(tt.headers), "My name is John", "What is my name?", credentialtest.ClassConfig(tt.classConfig))

				got := server.Requests()[before:]
				if valid {
					require.NoError(t, err)
					require.Len(t, got, 1)
					assert.Equal(t, "/openai/deployments/"+deploymentID+"/completions", got[0].URL.Path)
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
