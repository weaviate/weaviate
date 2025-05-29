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

package cluster

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaftConfig(t *testing.T) {
	tests := []struct {
		name           string
		config         Config
		expectedConfig func(*raft.Config)
	}{
		{
			name: "default configuration",
			config: Config{
				NodeID:          "node1",
				Logger:          logrus.New(),
				Voter:           true,
				WorkDir:         t.TempDir(),
				Host:            "localhost",
				RaftPort:        9090,
				RPCPort:         9091,
				BootstrapExpect: 1,
			},
			expectedConfig: func(cfg *raft.Config) {
				assert.Equal(t, raft.ServerID("node1"), cfg.LocalID)
				assert.Equal(t, "info", cfg.LogLevel)
				assert.True(t, cfg.NoLegacyTelemetry)
				assert.NotNil(t, cfg.Logger)
				// Default timeouts should be used since none were specified
				assert.Equal(t, raft.DefaultConfig().HeartbeatTimeout, cfg.HeartbeatTimeout)
				assert.Equal(t, raft.DefaultConfig().ElectionTimeout, cfg.ElectionTimeout)
				assert.Equal(t, raft.DefaultConfig().LeaderLeaseTimeout, cfg.LeaderLeaseTimeout)
				assert.Equal(t, raft.DefaultConfig().SnapshotInterval, cfg.SnapshotInterval)
				assert.Equal(t, raft.DefaultConfig().SnapshotThreshold, cfg.SnapshotThreshold)
				assert.Equal(t, raft.DefaultConfig().TrailingLogs, cfg.TrailingLogs)
			},
		},
		{
			name: "custom timeouts with multiplier",
			config: Config{
				NodeID:             "node1",
				Logger:             logrus.New(),
				Voter:              true,
				WorkDir:            t.TempDir(),
				Host:               "localhost",
				RaftPort:           9090,
				RPCPort:            9091,
				BootstrapExpect:    1,
				HeartbeatTimeout:   2 * time.Second,
				ElectionTimeout:    3 * time.Second,
				LeaderLeaseTimeout: 4 * time.Second,
				TimeoutsMultiplier: 2,
			},
			expectedConfig: func(cfg *raft.Config) {
				assert.Equal(t, raft.ServerID("node1"), cfg.LocalID)
				assert.Equal(t, "info", cfg.LogLevel)
				assert.True(t, cfg.NoLegacyTelemetry)
				assert.NotNil(t, cfg.Logger)
				// Timeouts should be multiplied by 2
				assert.Equal(t, 4*time.Second, cfg.HeartbeatTimeout)
				assert.Equal(t, 6*time.Second, cfg.ElectionTimeout)
				assert.Equal(t, 8*time.Second, cfg.LeaderLeaseTimeout)
			},
		},
		{
			name: "custom timeouts without multiplier",
			config: Config{
				NodeID:             "node1",
				Logger:             logrus.New(),
				Voter:              true,
				WorkDir:            t.TempDir(),
				Host:               "localhost",
				RaftPort:           9090,
				RPCPort:            9091,
				BootstrapExpect:    1,
				HeartbeatTimeout:   2 * time.Second,
				ElectionTimeout:    3 * time.Second,
				LeaderLeaseTimeout: 4 * time.Second,
				// TimeoutsMultiplier not set, should default to 1
			},
			expectedConfig: func(cfg *raft.Config) {
				assert.Equal(t, raft.ServerID("node1"), cfg.LocalID)
				assert.Equal(t, "info", cfg.LogLevel)
				assert.True(t, cfg.NoLegacyTelemetry)
				assert.NotNil(t, cfg.Logger)
				// Timeouts should remain unchanged since multiplier is not set
				assert.Equal(t, 2*time.Second, cfg.HeartbeatTimeout)
				assert.Equal(t, 3*time.Second, cfg.ElectionTimeout)
				assert.Equal(t, 4*time.Second, cfg.LeaderLeaseTimeout)
			},
		},
		{
			name: "custom snapshot settings",
			config: Config{
				NodeID:            "node1",
				Logger:            logrus.New(),
				Voter:             true,
				WorkDir:           t.TempDir(),
				Host:              "localhost",
				RaftPort:          9090,
				RPCPort:           9091,
				BootstrapExpect:   1,
				SnapshotInterval:  5 * time.Second,
				SnapshotThreshold: 100,
				TrailingLogs:      200,
			},
			expectedConfig: func(cfg *raft.Config) {
				assert.Equal(t, raft.ServerID("node1"), cfg.LocalID)
				assert.Equal(t, "info", cfg.LogLevel)
				assert.True(t, cfg.NoLegacyTelemetry)
				assert.NotNil(t, cfg.Logger)
				// Snapshot settings should be set to custom values
				assert.Equal(t, 5*time.Second, cfg.SnapshotInterval)
				assert.Equal(t, uint64(100), cfg.SnapshotThreshold)
				assert.Equal(t, uint64(200), cfg.TrailingLogs)
			},
		},
		{
			name: "debug log level",
			config: Config{
				NodeID:          "node1",
				Logger:          func() *logrus.Logger { l := logrus.New(); l.SetLevel(logrus.DebugLevel); return l }(),
				Voter:           true,
				WorkDir:         t.TempDir(),
				Host:            "localhost",
				RaftPort:        9090,
				RPCPort:         9091,
				BootstrapExpect: 1,
			},
			expectedConfig: func(cfg *raft.Config) {
				assert.Equal(t, raft.ServerID("node1"), cfg.LocalID)
				assert.Equal(t, "debug", cfg.LogLevel)
				assert.True(t, cfg.NoLegacyTelemetry)
				assert.NotNil(t, cfg.Logger)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewFSM(tt.config, nil, nil, prometheus.NewPedanticRegistry())
			cfg := store.raftConfig()
			require.NotNil(t, cfg)
			tt.expectedConfig(cfg)
		})
	}
}
