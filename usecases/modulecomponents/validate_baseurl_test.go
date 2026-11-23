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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cases use IP literals and blocked hostnames/suffixes (which short-circuit
// before the DNS step) so the test stays hermetic — no live DNS.
func TestValidateBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{name: "empty baseURL is allowed", baseURL: "", wantErr: false},
		{name: "http scheme rejected", baseURL: "http://example.com", wantErr: true},
		{name: "no scheme rejected", baseURL: "example.com", wantErr: true},
		{name: "empty host rejected", baseURL: "https://", wantErr: true},
		{name: "loopback IPv4 rejected", baseURL: "https://127.0.0.1", wantErr: true},
		{name: "loopback IPv6 rejected", baseURL: "https://[::1]", wantErr: true},
		{name: "private 10/8 rejected", baseURL: "https://10.0.0.1", wantErr: true},
		{name: "private 192.168 rejected", baseURL: "https://192.168.1.1:8080", wantErr: true},
		{name: "private 172.16 rejected", baseURL: "https://172.16.0.5", wantErr: true},
		{name: "cloud metadata IP rejected", baseURL: "https://169.254.169.254", wantErr: true},
		{name: "unspecified addr rejected", baseURL: "https://0.0.0.0", wantErr: true},
		{name: "localhost rejected", baseURL: "https://localhost", wantErr: true},
		{name: ".local suffix rejected", baseURL: "https://myhost.local", wantErr: true},
		{name: ".internal suffix rejected", baseURL: "https://svc.internal", wantErr: true},
		{name: ".localdomain suffix rejected", baseURL: "https://box.localdomain", wantErr: true},
		// A routable public IP literal is allowed (no DNS needed).
		{name: "public IP literal allowed", baseURL: "https://8.8.8.8", wantErr: false},
	}

	t.Run("validation disabled (default) is a no-op", func(t *testing.T) {
		// Unset → every URL is accepted, preserving backwards compatibility.
		for _, tt := range tests {
			assert.NoError(t, ValidateBaseURL(tt.baseURL), tt.name)
		}
		assert.NoError(t, ValidateBaseURL("http://169.254.169.254"))
	})

	t.Run("validation enabled", func(t *testing.T) {
		t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
		for _, tt := range tests {
			err := ValidateBaseURL(tt.baseURL)
			if tt.wantErr {
				assert.Error(t, err, tt.name)
			} else {
				assert.NoError(t, err, tt.name)
			}
		}
	})
}

// Guards the SSRF-via-header vector from hackerone #3768976.
func TestValidateBaseURLHeader(t *testing.T) {
	const key, apiKey = "X-Openai-Baseurl", "X-Openai-Api-Key"
	ctxWith := func(v string) context.Context {
		return context.WithValue(context.Background(), key, []string{v})
	}
	validate := func(ctx context.Context) error {
		ctx = context.WithValue(ctx, apiKey, []string{"caller"})
		_, err := RequestBaseURL(ctx, key, "", "", apiKey)
		return err
	}

	t.Run("validation enabled", func(t *testing.T) {
		t.Setenv("MODULES_VALIDATE_BASE_URL", "true")

		assert.NoError(t, validate(context.Background())) // absent
		assert.NoError(t, validate(ctxWith("https://8.8.8.8")))
		assert.Error(t, validate(ctxWith("https://169.254.169.254/")))
		assert.Error(t, validate(ctxWith("https://127.0.0.1/")))
		assert.Error(t, validate(ctxWith("https://10.0.0.5/")))
		assert.Error(t, validate(ctxWith("https://localhost/")))
		assert.Error(t, validate(ctxWith("http://169.254.169.254/")))
	})

	t.Run("validation disabled is a no-op even for internal headers", func(t *testing.T) {
		assert.NoError(t, validate(ctxWith("http://169.254.169.254/")))
	})
}

func TestRequestBaseURL(t *testing.T) {
	const (
		baseURLHeader = "X-Openai-Baseurl"
		keyHeader     = "X-Openai-Api-Key"
		configured    = "https://api.openai.com"
		attacker      = "https://attacker.example.com"
	)
	tests := []struct {
		name       string
		headers    map[string]string
		requested  string
		configured string
		validate   bool
		wantURL    string
		wantErr    string
	}{
		{name: "nothing requested", wantURL: configured},
		{name: "requested equals configured", requested: configured, wantURL: configured},
		{name: "requested adds a trailing slash", requested: configured + "/", wantURL: configured},
		{name: "requested host differs only in case", requested: "https://API.openai.com", wantURL: configured},
		{name: "requested scheme differs only in case", requested: "HTTPS://api.openai.com", wantURL: configured},
		{name: "requested ends with an empty query", requested: configured + "?", wantURL: configured},
		{name: "requested ends with an empty fragment", requested: configured + "#", wantURL: configured},
		{name: "requested equals a configured value without a host", requested: "baseurl", configured: "baseurl", wantURL: "baseurl"},
		{
			name:       "requested drops the trailing slash of a configured path",
			requested:  "https://gw.example.com/v1/openai",
			configured: "https://gw.example.com/v1/openai/",
			wantURL:    "https://gw.example.com/v1/openai/",
		},
		{
			name:      "requested path on the configured host without a request key",
			requested: configured + "/proxy",
			wantErr:   "baseURL needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:      "requested path on the configured host with a request key",
			requested: configured + "/proxy",
			headers:   map[string]string{keyHeader: "caller"},
			wantURL:   configured + "/proxy",
		},
		{
			name:       "requested gateway path of another tenant",
			requested:  "https://gw.example.com/v1/other-account/openai",
			configured: "https://gw.example.com/v1/openai",
			wantErr:    "baseURL needs X-Openai-Api-Key",
		},
		{
			name:       "requested path differs only in case",
			requested:  "https://api.openai.com/V1",
			configured: "https://api.openai.com/v1",
			wantErr:    "baseURL needs X-Openai-Api-Key",
		},
		{name: "requested adds a query", requested: configured + "?x=1", wantErr: "baseURL needs X-Openai-Api-Key"},
		{name: "requested adds a fragment", requested: configured + "#x", wantErr: "baseURL needs X-Openai-Api-Key"},
		{name: "requested adds userinfo", requested: "https://user@api.openai.com", wantErr: "baseURL needs X-Openai-Api-Key"},
		{name: "requested adds the default port", requested: "https://api.openai.com:443", wantErr: "baseURL needs X-Openai-Api-Key"},
		{
			name:      "requested host without a request key",
			requested: attacker,
			wantErr:   "baseURL needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:      "requested host with a request key",
			requested: attacker,
			headers:   map[string]string{keyHeader: "caller"},
			wantURL:   attacker,
		},
		{
			name:      "requested scheme differs",
			requested: "http://api.openai.com",
			wantErr:   "baseURL needs X-Openai-Api-Key",
		},
		{
			name:      "requested port differs",
			requested: "https://api.openai.com:8443",
			wantErr:   "baseURL needs X-Openai-Api-Key",
		},
		{
			name:      "requested host hidden behind userinfo",
			requested: "https://api.openai.com@attacker.example.com",
			wantErr:   "baseURL needs X-Openai-Api-Key",
		},
		{
			name:      "requested URL does not parse",
			requested: "https://api.openai.com%zz",
			wantErr:   "baseURL needs X-Openai-Api-Key",
		},
		{
			name:    "header host without a request key",
			headers: map[string]string{baseURLHeader: attacker},
			wantErr: "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:    "header host with a request key",
			headers: map[string]string{baseURLHeader: attacker, keyHeader: "caller"},
			wantURL: attacker,
		},
		{
			name:      "header wins over requested",
			headers:   map[string]string{baseURLHeader: configured},
			requested: attacker,
			wantURL:   configured,
		},
		{
			name:    "header with a trailing slash returns configured",
			headers: map[string]string{baseURLHeader: configured + "/"},
			wantURL: configured,
		},
		{
			name:      "another provider's key does not count",
			requested: attacker,
			headers:   map[string]string{"X-Azure-Api-Key": "caller"},
			wantErr:   "baseURL needs X-Openai-Api-Key",
		},
		{
			name:      "requested host validated when validation is on",
			requested: "https://127.0.0.1",
			headers:   map[string]string{keyHeader: "caller"},
			validate:  true,
			wantErr:   "baseURL cannot target internal addresses",
		},
		{
			name:     "header host validated when validation is on",
			headers:  map[string]string{baseURLHeader: "https://10.0.0.5", keyHeader: "caller"},
			validate: true,
			wantErr:  "baseURL cannot target internal addresses",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.validate {
				t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
			}
			ctx := context.Background()
			for k, v := range tt.headers {
				ctx = context.WithValue(ctx, k, []string{v})
			}
			configuredURL := tt.configured
			if configuredURL == "" {
				configuredURL = configured
			}
			got, err := RequestBaseURL(ctx, baseURLHeader, tt.requested, configuredURL, keyHeader)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, got)
		})
	}
}

func TestRequestAzureResourceName(t *testing.T) {
	const configured = "class-resource"
	tests := []struct {
		name      string
		headers   map[string]string
		requested string
		wantName  string
		wantErr   string
	}{
		{name: "nothing requested", wantName: configured},
		{name: "requested equals configured", requested: configured, wantName: configured},
		{name: "requested differs only in case", requested: "Class-Resource", wantName: configured},
		{
			name:      "requested name without a request key",
			requested: "other",
			wantErr:   "resourceName needs X-Azure-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:      "requested name with a request key",
			requested: "other",
			headers:   map[string]string{"X-Azure-Api-Key": "caller"},
			wantName:  "other",
		},
		{
			name:    "header name without a request key",
			headers: map[string]string{"X-Azure-Resource-Name": "attacker.example.com#"},
			wantErr: "X-Azure-Resource-Name needs X-Azure-Api-Key",
		},
		{
			name:     "header name with a request key",
			headers:  map[string]string{"X-Azure-Resource-Name": "other", "X-Azure-Api-Key": "caller"},
			wantName: "other",
		},
		{
			name:      "header wins over requested",
			headers:   map[string]string{"X-Azure-Resource-Name": configured},
			requested: "other",
			wantName:  configured,
		},
		{
			name:      "an OpenAI key does not count",
			requested: "other",
			headers:   map[string]string{"X-Openai-Api-Key": "caller"},
			wantErr:   "resourceName needs X-Azure-Api-Key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			for k, v := range tt.headers {
				ctx = context.WithValue(ctx, k, []string{v})
			}
			got, err := RequestAzureResourceName(ctx, tt.requested, configured)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, got)
		})
	}
}

func TestSSRFDialGuard(t *testing.T) {
	tests := []struct {
		name    string
		address string
		wantErr bool
	}{
		{name: "loopback blocked", address: "127.0.0.1:80", wantErr: true},
		{name: "private 10/8 blocked", address: "10.1.2.3:443", wantErr: true},
		{name: "private 192.168 blocked", address: "192.168.0.1:443", wantErr: true},
		{name: "cloud metadata blocked", address: "169.254.169.254:80", wantErr: true},
		{name: "unspecified blocked", address: "0.0.0.0:80", wantErr: true},
		{name: "IPv6 loopback blocked", address: "[::1]:80", wantErr: true},
		{name: "public address allowed", address: "8.8.8.8:443", wantErr: false},
		{name: "invalid address rejected", address: "not-an-address", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ssrfDialGuard("tcp", tt.address, nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
