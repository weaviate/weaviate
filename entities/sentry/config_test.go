package sentry

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
				t.Setenv("SENTRY_DSN", "http://dsn")
			}
			Config = nil
			conf, err := InitSentryConfig()

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
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
		expectedConfig ConfigOpts
	}

	tests := []test{
		{
			name: "enabled, everything set",
			vars: map[string]string{
				"SENTRY_ENABLED":   "true",
				"SENTRY_DSN":       "http://dsn",
				"SENTRY_DEBUG":     "true",
				"SENTRY_TAG_hello": "world",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled: true,
				DSN:     "http://dsn",
				Debug:   true,
				Tags: map[string]string{
					"hello": "world",
				},
			},
		},
		{
			name: "enabled, without optional vars",
			vars: map[string]string{
				"SENTRY_ENABLED": "true",
				"SENTRY_DSN":     "http://dsn",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled: true,
				DSN:     "http://dsn",
				Debug:   false,
				Tags:    map[string]string{},
			},
		},
		{
			name: "disabled",
			vars: map[string]string{
				"SENTRY_ENABLED": "false",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
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

			Config = nil
			config, err := InitSentryConfig()

			if test.expectErr {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, &test.expectedConfig, config)
			}
		})
	}
}

func TestParseSentryTags(t *testing.T) {
	tests := []struct {
		name        string
		envVars     map[string]string
		expected    map[string]string
		expectedErr error
	}{
		{
			name: "valid tags",
			envVars: map[string]string{
				"SENTRY_TAG_validKey1": "validValue1",
				"SENTRY_TAG_validKey2": "validValue2",
			},
			expected: map[string]string{
				"validKey1": "validValue1",
				"validKey2": "validValue2",
			},
			expectedErr: nil,
		},
		{
			name: "invalid key",
			envVars: map[string]string{
				"SENTRY_TAG_invalidKeyWithMoreThanThirtyTwoCharacters12345": "value",
			},
			expected:    map[string]string{},
			expectedErr: errors.New("invalid tag key: invalidKeyWithMoreThanThirtyTwoCharacters12345"),
		},
		{
			name: "invalid value",
			envVars: map[string]string{
				"SENTRY_TAG_validKey": "value\nwith\nnewlines",
			},
			expected:    map[string]string{},
			expectedErr: errors.New("invalid tag value for key: validKey"),
		},
		{
			name: "mixed valid and invalid",
			envVars: map[string]string{
				"SENTRY_TAG_validKey1":                            "validValue1",
				"SENTRY_TAG_invalidKeyWithMoreThanThirtyTwoChars": "value",
				"SENTRY_TAG_validKey2":                            "validValue2",
				"SENTRY_TAG_validKey3":                            "value\nwith\nnewlines",
			},
			expected: map[string]string{
				"validKey1": "validValue1",
			},
			expectedErr: errors.New("invalid tag key: invalidKeyWithMoreThanThirtyTwoChars"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.envVars {
				os.Setenv(key, value)
			}

			tags, err := parseTags()
			if tt.expectedErr != nil {
				assert.EqualError(t, err, tt.expectedErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, tags)
			}

			for key := range tt.envVars {
				os.Unsetenv(key)
			}
		})
	}
}
