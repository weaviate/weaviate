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
	const key = "X-Openai-Baseurl"
	ctxWith := func(v string) context.Context {
		return context.WithValue(context.Background(), key, []string{v})
	}
	validate := func(ctx context.Context) error {
		_, err := ValidatedBaseURLFromHeader(ctx, key, "")
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
