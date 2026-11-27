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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateCollectionBaseURL(t *testing.T) {
	tests := []struct {
		name      string
		allowlist string
		defaults  []string
		baseURL   string
		wantErr   string
	}{
		{name: "unset, so the module default", baseURL: ""},
		{name: "unset, no module default", defaults: []string{}, baseURL: ""},
		{name: "module default", baseURL: "https://api.openai.com"},
		{name: "module default given with a path", defaults: []string{"https://api.openai.com/v1"}, baseURL: "https://api.openai.com"},
		{name: "second module default", defaults: []string{"https://a.example", "https://api.openai.com"}, baseURL: "https://api.openai.com/v1"},
		{name: "module without a default", defaults: []string{}, baseURL: "https://api.openai.com", wantErr: "is not allowed"},
		{name: "empty module default matches nothing", defaults: []string{""}, baseURL: "https://api.openai.com", wantErr: "is not allowed"},
		{name: "module default with a path", baseURL: "https://api.openai.com/v1"},
		{name: "module default, host in upper case", baseURL: "HTTPS://API.OPENAI.COM"},
		{name: "foreign host", baseURL: "https://attacker.example", wantErr: "is not allowed"},
		{name: "foreign host names the env var and origin", baseURL: "https://attacker.example/v1", wantErr: `add "https://attacker.example" to MODULES_ALLOWED_BASE_URLS`},
		{name: "listed host", allowlist: "https://gateway.example", baseURL: "https://gateway.example/v1"},
		{name: "listed host among others, with spaces", allowlist: " https://a.example , https://gateway.example ", baseURL: "https://gateway.example"},
		{name: "listed host in upper case", allowlist: "https://GATEWAY.example", baseURL: "https://gateway.EXAMPLE"},
		{name: "listed host, other scheme", allowlist: "https://gateway.example", baseURL: "http://gateway.example", wantErr: "is not allowed"},
		{name: "listed host, other port", allowlist: "https://gateway.example", baseURL: "https://gateway.example:8443", wantErr: "is not allowed"},
		{name: "listed host and port", allowlist: "http://ollama:11434", baseURL: "http://ollama:11434"},
		{name: "explicit default https port", baseURL: "https://api.openai.com:443"},
		{name: "listed with default https port, used without", allowlist: "https://gateway.example:443", baseURL: "https://gateway.example/v1"},
		{name: "listed without default https port, used with", allowlist: "https://gateway.example", baseURL: "https://gateway.example:443/v1"},
		{name: "listed with default http port, used without", allowlist: "http://ollama:80", baseURL: "http://ollama"},
		{name: "listed without default http port, used with", allowlist: "http://ollama", baseURL: "http://ollama:80"},
		{name: "https port on http is not a default", allowlist: "http://ollama", baseURL: "http://ollama:443", wantErr: "is not allowed"},
		{name: "http port on https is not a default", allowlist: "https://gateway.example", baseURL: "https://gateway.example:80", wantErr: "is not allowed"},
		{name: "listed IPv6 with default port", allowlist: "https://[fd00::1]:443", baseURL: "https://[fd00::1]"},
		{name: "trailing dot is a different origin", baseURL: "https://api.openai.com.", wantErr: "is not allowed"},
		{name: "empty port is a different origin", baseURL: "https://api.openai.com:", wantErr: "is not allowed"},
		{name: "subdomain of the default", baseURL: "https://api.openai.com.attacker.example", wantErr: "is not allowed"},
		{name: "userinfo naming the default host", baseURL: "https://api.openai.com@attacker.example", wantErr: `"https://attacker.example"`},
		{name: "userinfo naming the default host, escaped slash", baseURL: "https://api.openai.com%2f@attacker.example", wantErr: "is not allowed"},
		{name: "userinfo on the default host", baseURL: "https://user:pass@api.openai.com"},
		{name: "fragment naming another host", baseURL: "https://api.openai.com#@attacker.example"},
		{name: "backslash", baseURL: `https://api.openai.com\@attacker.example`, wantErr: "is not a valid"},
		{name: "listed IPv6 literal", allowlist: "http://[fd00::1]:8080", baseURL: "http://[FD00::1]:8080/v1"},
		{name: "unlisted IPv6 literal", baseURL: "http://[fd00::1]:8080", wantErr: "is not allowed"},
		{name: "non-ASCII host, even when listed", allowlist: "https://bücher.example", baseURL: "https://bücher.example", wantErr: "is not a valid"},
		{name: "non-ASCII letter that lowercases to ASCII", allowlist: "https://kelvin.example", baseURL: "https://\u212aelvin.example", wantErr: "is not a valid"},
		{name: "listed punycode host", allowlist: "https://xn--bcher-kva.example", baseURL: "https://xn--bcher-kva.example"},
		{name: "no scheme", baseURL: "api.openai.com", wantErr: "is not a valid"},
		{name: "scheme-relative", baseURL: "//api.openai.com", wantErr: "is not a valid"},
		{name: "opaque", baseURL: "https:api.openai.com", wantErr: "is not a valid"},
		{name: "wildcard entry matches nothing", allowlist: "https://*.example", baseURL: "https://a.example", wantErr: "is not allowed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(AllowedBaseURLsEnv, tt.allowlist)
			defaults := tt.defaults
			if defaults == nil {
				defaults = []string{"https://api.openai.com"}
			}
			err := ValidateCollectionBaseURL("baseURL", tt.baseURL, defaults...)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestValidateCollectionBaseURL_IndependentOfSSRFFlag(t *testing.T) {
	for _, flag := range []string{"", "true"} {
		t.Run("MODULES_VALIDATE_BASE_URL="+flag, func(t *testing.T) {
			t.Setenv("MODULES_VALIDATE_BASE_URL", flag)
			t.Setenv(AllowedBaseURLsEnv, "")
			require.ErrorContains(t, ValidateCollectionBaseURL("baseURL", "https://attacker.example"), "is not allowed")
		})
	}
}

func TestValidateHostLabels(t *testing.T) {
	tests := []struct {
		value   string
		wantErr bool
	}{
		{value: ""},
		{value: "my-resource"},
		{value: "MyResource1"},
		{value: "us-east-1"},
		{value: "us-gov-west-1"},
		{value: "attacker.example/x?", wantErr: true},
		{value: "attacker.example/", wantErr: true},
		{value: "a.b", wantErr: true},
		{value: "x@attacker.example#", wantErr: true},
		{value: "-leading", wantErr: true},
		{value: "a_b", wantErr: true},
		{value: "\u212aelvin", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			azureErr, awsErr := ValidateAzureResourceName(tt.value), ValidateAWSRegion(tt.value)
			if !tt.wantErr {
				require.NoError(t, azureErr)
				require.NoError(t, awsErr)
				return
			}
			require.ErrorContains(t, azureErr, "is not a valid Azure resource name")
			require.ErrorContains(t, awsErr, "is not a valid AWS region name")
			require.NotContains(t, awsErr.Error(), AllowedBaseURLsEnv)
		})
	}
}

func TestHasHostSetting(t *testing.T) {
	tests := []struct {
		name     string
		settings any
		want     bool
	}{
		{name: "nil", settings: nil},
		{name: "no host setting", settings: map[string]any{"model": "m", "temperature": 0.5}},
		{name: "baseURL", settings: map[string]any{"baseURL": "https://a.example"}, want: true},
		{name: "other case", settings: map[string]any{"BASEURL": "https://a.example"}, want: true},
		{name: "legacy baseUrl spelling", settings: map[string]any{"baseUrl": "https://a.example"}, want: true},
		{name: "region", settings: map[string]any{"region": "us-east-1"}, want: true},
		{name: "empty value still counts", settings: map[string]any{"apiEndpoint": ""}, want: true},
		{name: "nested object", settings: map[string]any{"options": map[string]any{"endpoint": "x"}}, want: true},
		{name: "nested in a list", settings: map[string]any{"list": []any{map[string]any{"url": "x"}}}, want: true},
		{name: "host name as a value only", settings: map[string]any{"model": "baseURL"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HasHostSetting(tt.settings))
		})
	}
}

func TestParseAllowedOrigins(t *testing.T) {
	origins, invalid := ParseAllowedOrigins("https://A.example/v1, ,http://b.example:8080,not-a-url,https://*.example")
	assert.Equal(t, []string{"https://a.example", "http://b.example:8080"}, origins)
	assert.Equal(t, []string{"not-a-url", "https://*.example"}, invalid)
}

func TestWarnInvalidAllowedBaseURLs(t *testing.T) {
	tests := []struct {
		allowlist string
		want      []string
	}{
		{allowlist: "https://ok.example, http://ok.example:8080/"},
		{allowlist: "https://ok.example,gateway.example", want: []string{`ignoring entry "gateway.example"`}},
		{allowlist: "https://*.example", want: []string{`ignoring entry "https://*.example"`}},
		{allowlist: "https://gateway.example/v1", want: []string{`entry "https://gateway.example/v1" allows the whole origin "https://gateway.example"`}},
		{allowlist: "https://gateway.example?x=1", want: []string{`allows the whole origin "https://gateway.example"`}},
		{allowlist: "https://user@gateway.example", want: []string{`allows the whole origin "https://gateway.example"`}},
	}
	for _, tt := range tests {
		t.Run(tt.allowlist, func(t *testing.T) {
			t.Setenv(AllowedBaseURLsEnv, tt.allowlist)
			logger, hook := test.NewNullLogger()
			WarnInvalidAllowedBaseURLs(logger)
			var got []string
			for _, entry := range hook.AllEntries() {
				got = append(got, entry.Message)
			}
			require.Len(t, got, len(tt.want))
			for i, want := range tt.want {
				assert.Contains(t, got[i], want)
			}
		})
	}
}
