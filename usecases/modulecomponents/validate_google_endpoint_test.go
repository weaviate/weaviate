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

package modulecomponents

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateGoogleApiEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		apiEndpoint string
		wantErr     bool
	}{
		{name: "empty means module default", apiEndpoint: ""},
		{name: "regional Vertex host", apiEndpoint: "us-central1-aiplatform.googleapis.com"},
		{name: "global Vertex host", apiEndpoint: "aiplatform.googleapis.com"},
		{name: "AI Studio host", apiEndpoint: "generativelanguage.googleapis.com"},
		{name: "private service connect host", apiEndpoint: "abc123.us-central1.p.googleapis.com"},
		{name: "upper case host", apiEndpoint: "US-CENTRAL1-AIPLATFORM.GOOGLEAPIS.COM"},
		{name: "foreign host", apiEndpoint: "attacker.example.com", wantErr: true},
		{name: "google host outside the API domain", apiEndpoint: "google.com", wantErr: true},
		{name: "suffix as a prefix of a foreign host", apiEndpoint: "aiplatform.googleapis.com.attacker.example.com", wantErr: true},
		{name: "suffix embedded in a longer label", apiEndpoint: "notgoogleapis.com", wantErr: true},
		{name: "credentials in the host", apiEndpoint: "aiplatform.googleapis.com@attacker.example.com", wantErr: true},
		{name: "path appended to the host", apiEndpoint: "attacker.example.com/aiplatform.googleapis.com", wantErr: true},
		{name: "scheme included", apiEndpoint: "https://aiplatform.googleapis.com", wantErr: true},
		{name: "port included", apiEndpoint: "aiplatform.googleapis.com:8080", wantErr: true},
		{name: "trailing dot", apiEndpoint: "aiplatform.googleapis.com.", wantErr: true},
		{name: "empty label", apiEndpoint: "..googleapis.com", wantErr: true},
		{name: "bare suffix", apiEndpoint: ".googleapis.com", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGoogleApiEndpoint(tt.apiEndpoint)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "apiEndpoint must be a Google API host")
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestValidateGoogleLocation(t *testing.T) {
	tests := []struct {
		name     string
		location string
		wantErr  bool
	}{
		{name: "empty means module default", location: ""},
		{name: "region", location: "us-central1"},
		{name: "global", location: "global"},
		{name: "path injection", location: "attacker.example.com/", wantErr: true},
		{name: "credentials injection", location: "attacker.example.com@x", wantErr: true},
		{name: "dotted host", location: "attacker.example.com", wantErr: true},
		{name: "port", location: "us-central1:8080", wantErr: true},
		{name: "leading hyphen", location: "-us-central1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGoogleLocation("location", tt.location)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "location must be a Google region name")
				return
			}
			assert.NoError(t, err)
		})
	}
}
