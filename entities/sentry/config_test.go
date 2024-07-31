//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

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
				"SENTRY_ENABLED":       "true",
				"SENTRY_DSN":           "http://dsn",
				"SENTRY_DEBUG":         "true",
				"SENTRY_TAG_hello":     "world",
				"SENTRY_CLUSTER_OWNER": "im_the_owner",
				"SENTRY_CLUSTER_ID":    "id123",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:           true,
				DSN:               "http://dsn",
				Debug:             true,
				Environment:       "unknown",
				ErrorSampleRate:   1.0,
				TracesSampleRate:  0.1,
				ProfileSampleRate: 1.0,
				ClusterId:         "id123",
				ClusterOwner:      "im_the_owner",
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
				Enabled:           true,
				DSN:               "http://dsn",
				Debug:             false,
				Environment:       "unknown",
				ErrorSampleRate:   1.0,
				TracesSampleRate:  0.1,
				ProfileSampleRate: 1.0,
				Tags:              map[string]string{},
			},
		},
		{
			name: "enabled, with environment and release",
			vars: map[string]string{
				"SENTRY_ENABLED":     "true",
				"SENTRY_ENVIRONMENT": "prod",
				"SENTRY_RELEASE":     "123.321",
				"SENTRY_DSN":         "http://dsn",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:           true,
				DSN:               "http://dsn",
				Debug:             false,
				Environment:       "prod",
				Release:           "123.321",
				ErrorSampleRate:   1.0,
				TracesSampleRate:  0.1,
				ProfileSampleRate: 1.0,
				Tags:              map[string]string{},
			},
		},
		{
			name: "enabled, with everything disabled",
			vars: map[string]string{
				"SENTRY_ENABLED":                  "true",
				"SENTRY_DSN":                      "http://dsn",
				"SENTRY_ERROR_REPORTING_DISABLED": "true",
				"SENTRY_PROFILING_DISABLED":       "true",
				"SENTRY_TRACING_DISABLED":         "true",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:                true,
				DSN:                    "http://dsn",
				Environment:            "unknown",
				Debug:                  false,
				ErrorReportingDisabled: true,
				ProfilingDisabled:      true,
				TracingDisabled:        true,
				ErrorSampleRate:        0.0,
				TracesSampleRate:       0.0,
				ProfileSampleRate:      0.0,
				Tags:                   map[string]string{},
			},
		},
		{
			name: "enabled, with error only disabled",
			vars: map[string]string{
				"SENTRY_ENABLED":                  "true",
				"SENTRY_DSN":                      "http://dsn",
				"SENTRY_ERROR_REPORTING_DISABLED": "true",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:                true,
				DSN:                    "http://dsn",
				Environment:            "unknown",
				Debug:                  false,
				ErrorReportingDisabled: true,
				ErrorSampleRate:        0.0,
				TracesSampleRate:       0.1,
				ProfileSampleRate:      1.0,
				Tags:                   map[string]string{},
			},
		},
		{
			name: "enabled, with traces only disabled",
			vars: map[string]string{
				"SENTRY_ENABLED":          "true",
				"SENTRY_DSN":              "http://dsn",
				"SENTRY_TRACING_DISABLED": "true",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:           true,
				DSN:               "http://dsn",
				Environment:       "unknown",
				Debug:             false,
				TracingDisabled:   true,
				ErrorSampleRate:   1.0,
				TracesSampleRate:  0.0,
				ProfileSampleRate: 1.0,
				Tags:              map[string]string{},
			},
		},
		{
			name: "enabled, with profile only disabled",
			vars: map[string]string{
				"SENTRY_ENABLED":            "true",
				"SENTRY_DSN":                "http://dsn",
				"SENTRY_PROFILING_DISABLED": "true",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:           true,
				DSN:               "http://dsn",
				Environment:       "unknown",
				Debug:             false,
				ProfilingDisabled: true,
				ErrorSampleRate:   1.0,
				TracesSampleRate:  0.1,
				ProfileSampleRate: 0.0,
				Tags:              map[string]string{},
			},
		},
		{
			name: "enabled, with tracing, profiling and sampling",
			vars: map[string]string{
				"SENTRY_ENABLED":             "true",
				"SENTRY_DSN":                 "http://dsn",
				"SENTRY_ERROR_SAMPLE_RATE":   "0.75",
				"SENTRY_TRACES_SAMPLE_RATE":  "0.55",
				"SENTRY_PROFILE_SAMPLE_RATE": "0.55",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:           true,
				DSN:               "http://dsn",
				Debug:             false,
				TracingDisabled:   false,
				Environment:       "unknown",
				ErrorSampleRate:   0.75,
				TracesSampleRate:  0.55,
				ProfileSampleRate: 0.55,
				Tags:              map[string]string{},
			},
		},
		{
			name: "enabled, with tracing and too high sampling",
			vars: map[string]string{
				"SENTRY_ENABLED":             "true",
				"SENTRY_DSN":                 "http://dsn",
				"SENTRY_ERROR_SAMPLE_RATE":   "1.01",
				"SENTRY_TRACES_SAMPLE_RATE":  "1.01",
				"SENTRY_PROFILE_SAMPLE_RATE": "1.01",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:           true,
				DSN:               "http://dsn",
				Debug:             false,
				Environment:       "unknown",
				ErrorSampleRate:   1.0,
				TracesSampleRate:  0.1,
				ProfileSampleRate: 1.0,
				Tags:              map[string]string{},
			},
		},
		{
			name: "enabled, with tracing and too low sampling",
			vars: map[string]string{
				"SENTRY_ENABLED":             "true",
				"SENTRY_DSN":                 "http://dsn",
				"SENTRY_ERROR_SAMPLE_RATE":   "-1",
				"SENTRY_TRACES_SAMPLE_RATE":  "-1",
				"SENTRY_PROFILE_SAMPLE_RATE": "-1",
			},
			expectErr: false,
			expectedConfig: ConfigOpts{
				Enabled:           true,
				DSN:               "http://dsn",
				Debug:             false,
				Environment:       "unknown",
				ErrorSampleRate:   1.0,
				TracesSampleRate:  0.1,
				ProfileSampleRate: 1.0,
				Tags:              map[string]string{},
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
				"SENTRY_TAG_validKey3":                            "value",
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
