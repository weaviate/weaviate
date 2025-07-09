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

package config

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const DefaultGoroutineFactor = 1.5

func TestEnvironmentImportGoroutineFactor(t *testing.T) {
	factors := []struct {
		name            string
		goroutineFactor []string
		expected        float64
		expectedErr     bool
	}{
		{"Valid factor", []string{"1"}, 1, false},
		{"Low factor", []string{"0.5"}, 0.5, false},
		{"not given", []string{}, DefaultGoroutineFactor, false},
		{"High factor", []string{"5"}, 5, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.goroutineFactor) == 1 {
				t.Setenv("MAX_IMPORT_GOROUTINES_FACTOR", tt.goroutineFactor[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.MaxImportGoroutinesFactor)
			}
		})
	}
}

func TestEnvironmentSetFlushAfter_AllNames(t *testing.T) {
	factors := []struct {
		name        string
		flushAfter  []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"1"}, 1, false},
		{"not given", []string{}, DefaultPersistenceMemtablesFlushDirtyAfter, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	envNames := []struct {
		name    string
		envName string
	}{
		{name: "fallback idle (1st)", envName: "PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER"},
		{name: "fallback idle (2nd)", envName: "PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS"},
		{name: "dirty", envName: "PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS"},
	}

	for _, n := range envNames {
		t.Run(n.name, func(t *testing.T) {
			for _, tt := range factors {
				t.Run(tt.name, func(t *testing.T) {
					if len(tt.flushAfter) == 1 {
						t.Setenv(n.envName, tt.flushAfter[0])
					}
					conf := Config{}
					err := FromEnv(&conf)

					if tt.expectedErr {
						require.NotNil(t, err)
					} else {
						require.Equal(t, tt.expected, conf.Persistence.MemtablesFlushDirtyAfter)
					}
				})
			}
		})
	}
}

func TestEnvironmentFlushConflictingValues(t *testing.T) {
	// if all 3 variable names are used, the newest variable name
	// should be taken into consideration
	os.Clearenv()
	t.Setenv("PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER", "16")
	t.Setenv("PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS", "17")
	t.Setenv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "18")
	conf := Config{}
	err := FromEnv(&conf)
	require.Nil(t, err)

	assert.Equal(t, 18, conf.Persistence.MemtablesFlushDirtyAfter)
}

func TestEnvironmentPersistence_dataPath(t *testing.T) {
	factors := []struct {
		name     string
		value    []string
		config   Config
		expected string
	}{
		{
			name:     "given",
			value:    []string{"/var/lib/weaviate"},
			config:   Config{},
			expected: "/var/lib/weaviate",
		},
		{
			name:  "given with config set",
			value: []string{"/var/lib/weaviate"},
			config: Config{
				Persistence: Persistence{
					DataPath: "/var/data/weaviate",
				},
			},
			expected: "/var/lib/weaviate",
		},
		{
			name:     "not given",
			value:    []string{},
			config:   Config{},
			expected: DefaultPersistenceDataPath,
		},
		{
			name:  "not given with config set",
			value: []string{},
			config: Config{
				Persistence: Persistence{
					DataPath: "/var/data/weaviate",
				},
			},
			expected: "/var/data/weaviate",
		},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("PERSISTENCE_DATA_PATH", tt.value[0])
			}
			conf := tt.config
			err := FromEnv(&conf)
			require.Nil(t, err)
			require.Equal(t, tt.expected, conf.Persistence.DataPath)
		})
	}
}

func TestEnvironmentMemtable_MaxSize(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"100"}, 100, false},
		{"not given", []string{}, DefaultPersistenceMemtablesMaxSize, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("PERSISTENCE_MEMTABLES_MAX_SIZE_MB", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.MemtablesMaxSizeMB)
			}
		})
	}
}

func TestEnvironmentMemtable_MinDuration(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"100"}, 100, false},
		{"not given", []string{}, DefaultPersistenceMemtablesMinDuration, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("PERSISTENCE_MEMTABLES_MIN_ACTIVE_DURATION_SECONDS", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.MemtablesMinActiveDurationSeconds)
			}
		})
	}
}

func TestEnvironmentMemtable_MaxDuration(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"100"}, 100, false},
		{"not given", []string{}, DefaultPersistenceMemtablesMaxDuration, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("PERSISTENCE_MEMTABLES_MAX_ACTIVE_DURATION_SECONDS", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.MemtablesMaxActiveDurationSeconds)
			}
		})
	}
}

func TestEnvironmentParseClusterConfig(t *testing.T) {
	hostname, _ := os.Hostname()
	tests := []struct {
		name           string
		envVars        map[string]string
		expectedResult cluster.Config
		expectedErr    error
	}{
		{
			name: "valid cluster config - ports and advertiseaddr provided",
			envVars: map[string]string{
				"CLUSTER_GOSSIP_BIND_PORT": "7100",
				"CLUSTER_DATA_BIND_PORT":   "7101",
				"CLUSTER_ADVERTISE_ADDR":   "193.0.0.1",
				"CLUSTER_ADVERTISE_PORT":   "9999",
			},
			expectedResult: cluster.Config{
				Hostname:         hostname,
				GossipBindPort:   7100,
				DataBindPort:     7101,
				AdvertiseAddr:    "193.0.0.1",
				AdvertisePort:    9999,
				MaintenanceNodes: make([]string, 0),
			},
		},
		{
			name: "valid cluster config - no ports and advertiseaddr provided",
			expectedResult: cluster.Config{
				Hostname:         hostname,
				GossipBindPort:   DefaultGossipBindPort,
				DataBindPort:     DefaultGossipBindPort + 1,
				AdvertiseAddr:    "",
				MaintenanceNodes: make([]string, 0),
			},
		},
		{
			name: "valid cluster config - only gossip bind port provided",
			envVars: map[string]string{
				"CLUSTER_GOSSIP_BIND_PORT": "7777",
			},
			expectedResult: cluster.Config{
				Hostname:         hostname,
				GossipBindPort:   7777,
				DataBindPort:     7778,
				MaintenanceNodes: make([]string, 0),
			},
		},
		{
			name: "invalid cluster config - both ports provided",
			envVars: map[string]string{
				"CLUSTER_GOSSIP_BIND_PORT": "7100",
				"CLUSTER_DATA_BIND_PORT":   "7111",
			},
			expectedErr: errors.New("CLUSTER_DATA_BIND_PORT must be one port " +
				"number greater than CLUSTER_GOSSIP_BIND_PORT"),
		},
		{
			name: "invalid config - only data bind port provided",
			envVars: map[string]string{
				"CLUSTER_DATA_BIND_PORT": "7101",
			},
			expectedErr: errors.New("CLUSTER_DATA_BIND_PORT must be one port " +
				"number greater than CLUSTER_GOSSIP_BIND_PORT"),
		},
		{
			name: "schema sync disabled",
			envVars: map[string]string{
				"CLUSTER_IGNORE_SCHEMA_SYNC": "true",
			},
			expectedResult: cluster.Config{
				Hostname:                hostname,
				GossipBindPort:          7946,
				DataBindPort:            7947,
				IgnoreStartupSchemaSync: true,
				MaintenanceNodes:        make([]string, 0),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for k, v := range test.envVars {
				t.Setenv(k, v)
			}
			cfg, err := parseClusterConfig()
			if test.expectedErr != nil {
				assert.EqualError(t, err, test.expectedErr.Error(),
					"expected err: %v, got: %v", test.expectedErr, err)
			} else {
				assert.Nil(t, err, "expected nil, got: %v", err)
				assert.EqualValues(t, test.expectedResult, cfg)
			}
		})
	}
}

func TestEnvironmentSetDefaultVectorDistanceMetric(t *testing.T) {
	t.Run("DefaultVectorDistanceMetricIsEmpty", func(t *testing.T) {
		os.Clearenv()
		conf := Config{}
		FromEnv(&conf)
		require.Equal(t, "", conf.DefaultVectorDistanceMetric)
	})

	t.Run("NonEmptyDefaultVectorDistanceMetric", func(t *testing.T) {
		os.Clearenv()
		t.Setenv("DEFAULT_VECTOR_DISTANCE_METRIC", "l2-squared")
		conf := Config{}
		FromEnv(&conf)
		require.Equal(t, "l2-squared", conf.DefaultVectorDistanceMetric)
	})
}

func TestEnvironmentMaxConcurrentGetRequests(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"100"}, 100, false},
		{"not given", []string{}, DefaultMaxConcurrentGetRequests, false},
		{"unlimited", []string{"-1"}, -1, false},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("MAXIMUM_CONCURRENT_GET_REQUESTS", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.MaximumConcurrentGetRequests)
			}
		})
	}
}

func TestEnvironmentCORS_Origin(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    string
		expectedErr bool
	}{
		{"Valid", []string{"http://foo.com"}, "http://foo.com", false},
		{"not given", []string{}, DefaultCORSAllowOrigin, false},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			if len(tt.value) == 1 {
				os.Setenv("CORS_ALLOW_ORIGIN", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.CORS.AllowOrigin)
			}
		})
	}
}

func TestEnvironmentGRPCPort(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"50052"}, 50052, false},
		{"not given", []string{}, DefaultGRPCPort, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("GRPC_PORT", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.GRPC.Port)
			}
		})
	}
}

func TestEnvironmentCORS_Methods(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    string
		expectedErr bool
	}{
		{"Valid", []string{"POST"}, "POST", false},
		{"not given", []string{}, DefaultCORSAllowMethods, false},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			if len(tt.value) == 1 {
				os.Setenv("CORS_ALLOW_METHODS", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.CORS.AllowMethods)
			}
		})
	}
}

func TestEnvironmentDisableGraphQL(t *testing.T) {
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
				t.Setenv("DISABLE_GRAPHQL", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.DisableGraphQL)
			}
		})
	}
}

func TestEnvironmentCORS_Headers(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    string
		expectedErr bool
	}{
		{"Valid", []string{"Authorization"}, "Authorization", false},
		{"not given", []string{}, DefaultCORSAllowHeaders, false},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			if len(tt.value) == 1 {
				os.Setenv("CORS_ALLOW_HEADERS", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.CORS.AllowHeaders)
			}
		})
	}
}

func TestEnvironmentPrometheusGroupClasses_OldName(t *testing.T) {
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
			t.Setenv("PROMETHEUS_MONITORING_ENABLED", "true")
			if len(tt.value) == 1 {
				t.Setenv("PROMETHEUS_MONITORING_GROUP_CLASSES", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Monitoring.Group)
			}
		})
	}
}

func TestEnvironmentPrometheusGroupClasses_NewName(t *testing.T) {
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
			t.Setenv("PROMETHEUS_MONITORING_ENABLED", "true")
			if len(tt.value) == 1 {
				t.Setenv("PROMETHEUS_MONITORING_GROUP", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Monitoring.Group)
			}
		})
	}
}

func TestEnvironmentMinimumReplicationFactor(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"3"}, 3, false},
		{"not given", []string{}, DefaultMinimumReplicationFactor, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("REPLICATION_MINIMUM_FACTOR", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Replication.MinimumFactor)
			}
		})
	}
}

func TestEnvironmentQueryDefaults_Limit(t *testing.T) {
	factors := []struct {
		name     string
		value    []string
		config   Config
		expected int64
	}{
		{
			name:     "Valid",
			value:    []string{"3"},
			config:   Config{},
			expected: 3,
		},
		{
			name:  "Valid with config already set",
			value: []string{"3"},
			config: Config{
				QueryDefaults: QueryDefaults{
					Limit: 20,
				},
			},
			expected: 3,
		},
		{
			name:  "not given with config set",
			value: []string{},
			config: Config{
				QueryDefaults: QueryDefaults{
					Limit: 20,
				},
			},
			expected: 20,
		},
		{
			name:     "not given with config set",
			value:    []string{},
			config:   Config{},
			expected: DefaultQueryDefaultsLimit,
		},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("QUERY_DEFAULTS_LIMIT", tt.value[0])
			}
			conf := tt.config
			err := FromEnv(&conf)

			require.Nil(t, err)
			require.Equal(t, tt.expected, conf.QueryDefaults.Limit)
		})
	}
}

func TestEnvironmentAuthentication(t *testing.T) {
	factors := []struct {
		name         string
		auth_env_var []string
		expected     Authentication
	}{
		{
			name:         "Valid API Key",
			auth_env_var: []string{"AUTHENTICATION_APIKEY_ENABLED"},
			expected: Authentication{
				APIKey: StaticAPIKey{
					Enabled: true,
				},
			},
		},
		{
			name:         "Valid Anonymous Access",
			auth_env_var: []string{"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED"},
			expected: Authentication{
				AnonymousAccess: AnonymousAccess{
					Enabled: true,
				},
			},
		},
		{
			name:         "Valid OIDC Auth",
			auth_env_var: []string{"AUTHENTICATION_OIDC_ENABLED"},
			expected: Authentication{
				OIDC: OIDC{
					Enabled:           true,
					Issuer:            runtime.NewDynamicValue(""),
					ClientID:          runtime.NewDynamicValue(""),
					SkipClientIDCheck: runtime.NewDynamicValue(false),
					UsernameClaim:     runtime.NewDynamicValue(""),
					GroupsClaim:       runtime.NewDynamicValue(""),
					Scopes:            runtime.NewDynamicValue([]string(nil)),
					Certificate:       runtime.NewDynamicValue(""),
				},
			},
		},
		{
			name:         "Enabled db user",
			auth_env_var: []string{"AUTHENTICATION_DB_USERS_ENABLED"},
			expected: Authentication{
				DBUsers: DbUsers{Enabled: true},
			},
		},
		{
			name:         "not given",
			auth_env_var: []string{},
			expected: Authentication{
				AnonymousAccess: AnonymousAccess{
					Enabled: true,
				},
			},
		},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.auth_env_var) == 1 {
				t.Setenv(tt.auth_env_var[0], "true")
			}
			conf := Config{}
			err := FromEnv(&conf)
			require.Nil(t, err)
			require.Equal(t, tt.expected, conf.Authentication)
		})
	}
}

func TestEnvironmentHNSWMaxLogSize(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int64
		expectedErr bool
	}{
		{"Valid no unit", []string{"3"}, 3, false},
		{"Valid IEC unit", []string{"3KB"}, 3000, false},
		{"Valid SI unit", []string{"3KiB"}, 3 * 1024, false},
		{"not given", []string{}, DefaultPersistenceHNSWMaxLogSize, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("PERSISTENCE_HNSW_MAX_LOG_SIZE", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.HNSWMaxLogSize)
			}
		})
	}
}

func TestEnvironmentHNSWWaitForPrefill(t *testing.T) {
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
				t.Setenv("HNSW_STARTUP_WAIT_FOR_VECTOR_CACHE", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.HNSWStartupWaitForVectorCache)
			}
		})
	}
}

func TestEnvironmentHNSWVisitedListPoolMaxSize(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"3"}, 3, false},
		{"not given", []string{}, DefaultHNSWVisitedListPoolSize, false},
		{"valid negative", []string{"-1"}, -1, false},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("HNSW_VISITED_LIST_POOL_MAX_SIZE", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.HNSWVisitedListPoolMaxSize)
			}
		})
	}
}

func TestEnvironmentHNSWFlatSearchConcurrency(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"3"}, 3, false},
		{"not given", []string{}, DefaultHNSWFlatSearchConcurrency, false},
		{"valid negative", []string{"-1"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("HNSW_FLAT_SEARCH_CONCURRENCY", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.HNSWFlatSearchConcurrency)
			}
		})
	}
}

func TestEnvironmentHNSWAcornFilterRatio(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    float64
		expectedErr bool
	}{
		{"Valid", []string{"0.5"}, 0.5, false},
		{"not given", []string{}, 0.4, false},
		{"max", []string{"0.0"}, 0.0, false},
		{"min", []string{"1.0"}, 1.0, false},
		{"negative", []string{"-1.2"}, -1.0, true},
		{"too large", []string{"1.2"}, -1.0, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("HNSW_ACORN_FILTER_RATIO", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.HNSWAcornFilterRatio)
			}
		})
	}
}

func TestEnabledForHost(t *testing.T) {
	localHostname := "weaviate-1"
	envName := "HOSTBASED_SETTING"

	enabledVals := []string{"enabled", "1", "true", "on", "weaviate-1", "weaviate-0,weaviate-1,weaviate-2"}
	for _, val := range enabledVals {
		t.Run(fmt.Sprintf("enabled %q", val), func(t *testing.T) {
			t.Setenv(envName, val)
			assert.True(t, enabledForHost(envName, localHostname))
		})
	}

	disabledVals := []string{"disabled", "0", "false", "off", "weaviate-0", "weaviate-0,weaviate-2,weaviate-3", ""}
	for _, val := range disabledVals {
		t.Run(fmt.Sprintf("disabled %q", val), func(t *testing.T) {
			t.Setenv(envName, val)
			assert.False(t, enabledForHost(envName, localHostname))
		})
	}
}

func TestParseCollectionPropsTenants(t *testing.T) {
	type testCase struct {
		env            string
		expected       []CollectionPropsTenants
		expectedErrMsg string
	}

	p := newCollectionPropsTenantsParser()

	testCases := []testCase{
		{
			env:      "",
			expected: []CollectionPropsTenants{},
		},

		// collections
		{
			env: "Collection1",
			expected: []CollectionPropsTenants{
				{Collection: "Collection1"},
			},
		},
		{
			env: "Collection1; Collection2; ;",
			expected: []CollectionPropsTenants{
				{Collection: "Collection1"},
				{Collection: "Collection2"},
			},
		},
		{
			env: "Collection1:; Collection2::; ;",
			expected: []CollectionPropsTenants{
				{Collection: "Collection1"},
				{Collection: "Collection2"},
			},
		},

		// collections + props
		{
			env: "Collection1:prop1,prop2",
			expected: []CollectionPropsTenants{
				{
					Collection: "Collection1",
					Props:      []string{"prop1", "prop2"},
				},
			},
		},
		{
			env: "Collection1:prop1, prop2;Collection2:prop3: ;",
			expected: []CollectionPropsTenants{
				{
					Collection: "Collection1",
					Props:      []string{"prop1", "prop2"},
				},
				{
					Collection: "Collection2",
					Props:      []string{"prop3"},
				},
			},
		},

		// collections + tenants
		{
			env: "Collection1::tenant1,tenant2",
			expected: []CollectionPropsTenants{
				{
					Collection: "Collection1",
					Tenants:    []string{"tenant1", "tenant2"},
				},
			},
		},
		{
			env: "Collection1::tenant1, tenant2;Collection2::tenant3",
			expected: []CollectionPropsTenants{
				{
					Collection: "Collection1",
					Tenants:    []string{"tenant1", "tenant2"},
				},
				{
					Collection: "Collection2",
					Tenants:    []string{"tenant3"},
				},
			},
		},

		// collections + props + tenants
		{
			env: "Collection1:prop1:tenant1,tenant2",
			expected: []CollectionPropsTenants{
				{
					Collection: "Collection1",
					Props:      []string{"prop1"},
					Tenants:    []string{"tenant1", "tenant2"},
				},
			},
		},
		{
			env: "Collection1:prop1 :tenant1, tenant2;Collection2:prop2,prop3 :tenant3 ; ",
			expected: []CollectionPropsTenants{
				{
					Collection: "Collection1",
					Props:      []string{"prop1"},
					Tenants:    []string{"tenant1", "tenant2"},
				},
				{
					Collection: "Collection2",
					Props:      []string{"prop2", "prop3"},
					Tenants:    []string{"tenant3"},
				},
			},
		},

		// unique / merged
		{
			env: "Collection1:prop1,prop2:tenant1,tenant2;Collection2:propX;Collection1:prop2,prop3;Collection3::tenantY;Collection1:prop4:tenant2,tenant3",
			expected: []CollectionPropsTenants{
				{
					Collection: "Collection1",
					Props:      []string{"prop1", "prop2", "prop3", "prop4"},
					Tenants:    []string{"tenant1", "tenant2", "tenant3"},
				},
				{
					Collection: "Collection2",
					Props:      []string{"propX"},
				},
				{
					Collection: "Collection3",
					Tenants:    []string{"tenantY"},
				},
			},
		},

		// errors
		{
			env:            "lowerCaseCollectionName",
			expectedErrMsg: "invalid collection name",
		},
		{
			env:            "InvalidChars#",
			expectedErrMsg: "invalid collection name",
		},
		{
			env:            "Collection1:InvalidChars#",
			expectedErrMsg: "invalid property name",
		},
		{
			env:            "Collection1::InvalidChars#",
			expectedErrMsg: "invalid tenant/shard name",
		},
		{
			env:            ":prop",
			expectedErrMsg: "missing collection name",
		},
		{
			env:            "::tenant",
			expectedErrMsg: "missing collection name",
		},
		{
			env:            ":prop:tenant",
			expectedErrMsg: "missing collection name",
		},
		{
			env:            "Collection1:::",
			expectedErrMsg: "too many parts",
		},
		{
			env:            "Collection1:prop:tenant:",
			expectedErrMsg: "too many parts",
		},
		{
			env:            "Collection1:prop:tenant:something",
			expectedErrMsg: "too many parts",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.env, func(t *testing.T) {
			cpts, err := p.parse(tc.env)

			if tc.expectedErrMsg != "" {
				assert.ErrorContains(t, err, tc.expectedErrMsg)
			} else {
				assert.NoError(t, err)
			}

			assert.ElementsMatch(t, tc.expected, cpts)
		})
	}
}

func TestEnvironmentPersistenceMinMMapSize(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int64
		expectedErr bool
	}{
		{"Valid no unit", []string{"3"}, 3, false},
		{"Valid IEC unit", []string{"3KB"}, 3000, false},
		{"Valid SI unit", []string{"3KiB"}, 3 * 1024, false},
		{"not given", []string{}, DefaultPersistenceMinMMapSize, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("PERSISTENCE_MIN_MMAP_SIZE", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.MinMMapSize)
			}
		})
	}
}

func TestEnvironmentPersistenceMaxReuseWalSize(t *testing.T) {
	factors := []struct {
		name        string
		value       []string
		expected    int64
		expectedErr bool
	}{
		{"Valid no unit", []string{"3"}, 3, false},
		{"Valid IEC unit", []string{"3KB"}, 3000, false},
		{"Valid SI unit", []string{"3KiB"}, 3 * 1024, false},
		{"not given", []string{}, DefaultPersistenceMaxReuseWalSize, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.value) == 1 {
				t.Setenv("PERSISTENCE_MAX_REUSE_WAL_SIZE", tt.value[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.MaxReuseWalSize)
			}
		})
	}
}

func TestParsePositiveFloat(t *testing.T) {
	tests := []struct {
		name         string
		envName      string
		envValue     string
		defaultValue float64
		expected     float64
		expectError  bool
	}{
		{
			name:         "valid positive float",
			envName:      "TEST_POSITIVE_FLOAT",
			envValue:     "1.5",
			defaultValue: 2.0,
			expected:     1.5,
			expectError:  false,
		},
		{
			name:         "valid integer as float",
			envName:      "TEST_POSITIVE_FLOAT",
			envValue:     "2",
			defaultValue: 1.0,
			expected:     2.0,
			expectError:  false,
		},
		{
			name:         "use default when env not set",
			envName:      "TEST_POSITIVE_FLOAT",
			envValue:     "",
			defaultValue: 3.0,
			expected:     3.0,
			expectError:  false,
		},
		{
			name:         "zero value should error",
			envName:      "TEST_POSITIVE_FLOAT",
			envValue:     "0",
			defaultValue: 1.0,
			expected:     0,
			expectError:  true,
		},
		{
			name:         "negative value should error",
			envName:      "TEST_POSITIVE_FLOAT",
			envValue:     "-1.5",
			defaultValue: 1.0,
			expected:     0,
			expectError:  true,
		},
		{
			name:         "invalid float should error",
			envName:      "TEST_POSITIVE_FLOAT",
			envValue:     "not-a-float",
			defaultValue: 1.0,
			expected:     0,
			expectError:  true,
		},
		{
			name:         "very small positive float",
			envName:      "TEST_POSITIVE_FLOAT",
			envValue:     "0.0000001",
			defaultValue: 1.0,
			expected:     0.0000001,
			expectError:  false,
		},
		{
			name:         "very large positive float",
			envName:      "TEST_POSITIVE_FLOAT",
			envValue:     "999999.999999",
			defaultValue: 1.0,
			expected:     999999.999999,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up environment
			if tt.envValue != "" {
				t.Setenv(tt.envName, tt.envValue)
			} else {
				os.Unsetenv(tt.envName)
			}

			// Create a variable to store the result
			var result float64

			// Call the function
			err := parsePositiveFloat(tt.envName, func(val float64) {
				result = val
			}, tt.defaultValue)

			// Check error
			if tt.expectError {
				assert.Error(t, err)
				if tt.envValue != "" {
					assert.Contains(t, err.Error(), tt.envName)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
