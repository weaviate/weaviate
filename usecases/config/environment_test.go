//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/cluster"
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

func TestEnvironmentSetFlushAfter_BackwardCompatibility(t *testing.T) {
	factors := []struct {
		name        string
		flushAfter  []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"1"}, 1, false},
		{"not given", []string{}, DefaultPersistenceFlushIdleMemtablesAfter, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.flushAfter) == 1 {
				t.Setenv("PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER", tt.flushAfter[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.FlushIdleMemtablesAfter)
			}
		})
	}
}

func TestEnvironmentSetFlushAfter_NewName(t *testing.T) {
	factors := []struct {
		name        string
		flushAfter  []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"1"}, 1, false},
		{"not given", []string{}, DefaultPersistenceFlushIdleMemtablesAfter, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.flushAfter) == 1 {
				t.Setenv("PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS", tt.flushAfter[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.FlushIdleMemtablesAfter)
			}
		})
	}
}

func TestEnvironmentFlushConflictingValues(t *testing.T) {
	// if both the old and new variable names are used the new variable name
	// should be taken into consideration
	os.Clearenv()
	t.Setenv("PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER", "16")
	t.Setenv("PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS", "17")
	conf := Config{}
	err := FromEnv(&conf)
	require.Nil(t, err)

	assert.Equal(t, 17, conf.Persistence.FlushIdleMemtablesAfter)
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
	tests := []struct {
		name           string
		envVars        map[string]string
		expectedResult cluster.Config
		expectedErr    error
	}{
		{
			name: "valid cluster config - both ports provided",
			envVars: map[string]string{
				"CLUSTER_GOSSIP_BIND_PORT": "7100",
				"CLUSTER_DATA_BIND_PORT":   "7101",
			},
			expectedResult: cluster.Config{
				GossipBindPort: 7100,
				DataBindPort:   7101,
			},
		},
		{
			name: "valid cluster config - no ports provided",
			expectedResult: cluster.Config{
				GossipBindPort: DefaultGossipBindPort,
				DataBindPort:   DefaultGossipBindPort + 1,
			},
		},
		{
			name: "valid cluster config - only gossip bind port provided",
			envVars: map[string]string{
				"CLUSTER_GOSSIP_BIND_PORT": "7777",
			},
			expectedResult: cluster.Config{
				GossipBindPort: 7777,
				DataBindPort:   7778,
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
				GossipBindPort:          7946, // TODO: 7946 might conflict with Docker swarm
				DataBindPort:            7947,
				IgnoreStartupSchemaSync: true,
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
