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

package docker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLicenseKey(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		want    string
		wantErr bool
	}{
		{name: "unset", env: "", wantErr: true},
		{name: "whitespace only", env: " \r\n", wantErr: true},
		{name: "surrounding whitespace is trimmed", env: " some-key\n", want: "some-key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(LicenseKeyEnv, tt.env)
			key, err := LicenseKey()
			if tt.wantErr {
				require.ErrorContains(t, err, LicenseKeyEnv)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, key)
		})
	}
}

// Start fails before it creates a network, so this needs no Docker.
func TestStartNeedsLicenseKey(t *testing.T) {
	tests := []struct {
		name    string
		compose *Compose
	}{
		{name: "namespaces", compose: New().WithWeaviate().WithNamespaces()},
		{name: "license key file", compose: New().WithWeaviate().WithLicenseKeyFile()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(LicenseKeyEnv, "")
			compose, err := tt.compose.Start(context.Background())
			require.ErrorContains(t, err, LicenseKeyEnv)
			require.Nil(t, compose)
		})
	}
}
