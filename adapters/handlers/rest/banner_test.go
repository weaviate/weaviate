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

package rest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestURLFromArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{
			name: "docker image flags",
			args: []string{"--host", "0.0.0.0", "--port", "8080", "--scheme", "http"},
			want: "http://localhost:8080",
		},
		{
			name: "http preferred when both schemes are enabled",
			args: []string{"--scheme", "https", "--scheme", "http", "--host", "10.0.0.5", "--port", "8080", "--tls-port", "8443"},
			want: "http://10.0.0.5:8080",
		},
		{
			name: "https is the generated server's default scheme, tls host falls back to host",
			args: []string{"--host", "weaviate.local", "--tls-port", "8443"},
			want: "https://weaviate.local:8443",
		},
		{
			name: "env fallbacks like the generated server",
			args: []string{"--scheme", "http"},
			env:  map[string]string{"HOST": "::", "PORT": "8099"},
			want: "http://localhost:8099",
		},
		{
			name: "flags win over env",
			args: []string{"--scheme", "http", "--port", "8082"},
			env:  map[string]string{"PORT": "8099"},
			want: "http://localhost:8082",
		},
		{
			name: "unrelated flags are ignored",
			args: []string{"--config-file", "/etc/weaviate.yaml", "--write-timeout", "600s", "--scheme=http", "--port=8080"},
			want: "http://localhost:8080",
		},
		{
			name: "ipv6 host is bracketed",
			args: []string{"--scheme", "http", "--host", "::1", "--port", "8080"},
			want: "http://[::1]:8080",
		},
		{
			name: "no flags at all",
			args: nil,
			want: "https://localhost:0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"HOST", "PORT", "TLS_HOST", "TLS_PORT"} {
				if v, ok := tt.env[k]; ok {
					t.Setenv(k, v)
				} else {
					unsetenv(t, k)
				}
			}
			assert.Equal(t, tt.want, restURLFromArgs(tt.args))
		})
	}
}

func TestBannerDisabled(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want bool
	}{
		{name: "unset", env: "", want: false},
		{name: "true", env: "true", want: true},
		{name: "1", env: "1", want: true},
		{name: "on", env: "on", want: true},
		{name: "false", env: "false", want: false},
		{name: "garbage", env: "nope", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DISABLE_STARTUP_BANNER", tt.env)
			assert.Equal(t, tt.want, bannerDisabled())
		})
	}
}

// unsetenv removes k for the test's duration; t.Setenv(k, "") would leave an
// empty value, which is not the same as an absent one to go-flags.
func unsetenv(t *testing.T, k string) {
	t.Helper()
	old, had := os.LookupEnv(k)
	if !had {
		return
	}
	require.NoError(t, os.Unsetenv(k))
	t.Cleanup(func() { _ = os.Setenv(k, old) })
}
