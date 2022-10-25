//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package config

import (
	"errors"
	"os"
	"testing"

	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			os.Clearenv()
			if len(tt.goroutineFactor) == 1 {
				os.Setenv("MAX_IMPORT_GOROUTINES_FACTOR", tt.goroutineFactor[0])
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

func TestEnvironmentSetFlushAfter(t *testing.T) {
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
			os.Clearenv()
			if len(tt.flushAfter) == 1 {
				os.Setenv("PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER", tt.flushAfter[0])
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
		os.Setenv("DEFAULT_VECTOR_DISTANCE_METRIC", "l2-squared")
		conf := Config{}
		FromEnv(&conf)
		require.Equal(t, "l2-squared", conf.DefaultVectorDistanceMetric)
	})
}
