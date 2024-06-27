package sentry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSentryEnabled(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    bool
		expectedErr bool
	}{
		{"Valid: true", []string{"true"}, true, false},
		{"Valid: false", []string{"false"}, false, false},
		{"Valid: 1", []string{"1"}, true, false},
		{"Valid: 0", []string{"0"}, false, false},
		{"Valid: on", []string{"on"}, true, false},
		{"Valid: off", []string{"off"}, false, false},
		{"not given", []string{}, false, false},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("SENTRY_ENABLED", tt.value[0])
			}
			conf, err := ParseSentryConfigFromEnv()

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Enabled)
			}
		})
	}
}

func TestSentryConfig(t *testing.T) {
	type test struct {
		name           string
		vars           map[string]string
		expectErr      bool
		expectedConfig Config
	}

	tests := []test{
		{
			name: "enabled, everything set",
			vars: map[string]string{
				"SENTRY_ENABLED": "true",
				"SENTRY_DSN":     "http://dsn",
				"SENTRY_DEBUG":   "true",
			},
			expectErr: false,
			expectedConfig: Config{
				Enabled: true,
				DSN:     "http://dsn",
				Debug:   true,
			},
		},
		{
			name: "enabled, without optional vars",
			vars: map[string]string{
				"SENTRY_ENABLED": "true",
				"SENTRY_DSN":     "http://dsn",
			},
			expectErr: false,
			expectedConfig: Config{
				Enabled: true,
				DSN:     "http://dsn",
				Debug:   false,
			},
		},
		{
			name: "disabled",
			vars: map[string]string{
				"SENTRY_ENABLED": "false",
			},
			expectErr: false,
			expectedConfig: Config{
				Enabled: false,
			},
		},
		{
			name: "enabled, but required fields not set",
			vars: map[string]string{
				"SENTRY_ENABLED": "true",
			},
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, value := range test.vars {
				t.Setenv(key, value)
			}

			config, err := ParseSentryConfigFromEnv()

			if test.expectErr {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, test.expectedConfig, config)
			}
		})
	}
}
