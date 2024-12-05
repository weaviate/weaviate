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

package rest

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestGetCores(t *testing.T) {
	tests := []struct {
		name     string
		cpuset   string
		expected int
		wantErr  bool
	}{
		{"Single core", "0", 1, false},
		{"Multiple cores", "0,1,2,3", 4, false},
		{"Range of cores", "0-3", 4, false},
		{"Multiple ranges", "0-3,5-7", 7, false},
		{"Mixed format", "0-2,4,6-7", 6, false},
		{"Mixed format 2", "0,2-4,7", 5, false},
		{"Empty cpuset", "", 0, false},
		{"Invalid format", "0-2-4", 0, true},
		{"Non-numeric", "a-b", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := calcCPUs(tt.cpuset)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCores() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("getCores() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestAdjustBootStrapExpect(t *testing.T) {
	type testCase struct {
		name                    string
		metadataVotersOnlyNodes []string
		allNames                []string
		localName               string
		initialRaftConfig       config.Raft
		expectedJoinLength      int
		expectedBootstrapExpect int
		checkLocalName          bool
		skipAllNames            bool
	}

	testCases := []testCase{
		{
			name:                    "No config (1 Node)",
			metadataVotersOnlyNodes: []string{},
			allNames:                []string{"node1"},
			localName:               "",
			initialRaftConfig:       config.Raft{},
			expectedJoinLength:      1,
			expectedBootstrapExpect: 1,
		},
		{
			name:                    "No config (2 Nodes)",
			metadataVotersOnlyNodes: []string{},
			allNames:                []string{"node1", "node2"},
			localName:               "",
			initialRaftConfig:       config.Raft{},
			expectedJoinLength:      2,
			expectedBootstrapExpect: 1,
		},
		{
			name:                    "No config (3 Nodes)",
			metadataVotersOnlyNodes: []string{},
			allNames:                []string{"node1", "node2", "node3"},
			localName:               "",
			initialRaftConfig:       config.Raft{},
			expectedJoinLength:      3,
			expectedBootstrapExpect: 3,
		},
		{
			name:                    "No config (5 Nodes)",
			metadataVotersOnlyNodes: []string{},
			allNames:                []string{"node1", "node2", "node3", "node4", "node5"},
			localName:               "",
			initialRaftConfig:       config.Raft{},
			expectedJoinLength:      5,
			expectedBootstrapExpect: 3,
		},
		{
			name:                    "Single node config",
			metadataVotersOnlyNodes: []string{},
			allNames:                []string{"node1"},
			localName:               "node1",
			initialRaftConfig: config.Raft{
				Join:            []string{"node1"},
				Port:            8300,
				BootstrapExpect: 1,
			},
			expectedJoinLength:      1,
			expectedBootstrapExpect: 1,
			checkLocalName:          true,
		},
		{
			name:                    "Multiple nodes config",
			metadataVotersOnlyNodes: []string{},
			allNames:                []string{"node1", "node2", "node3"},
			localName:               "node1",
			initialRaftConfig: config.Raft{
				Join:            []string{"node1:8300", "node2:8301", "node3:8302"},
				Port:            8300,
				BootstrapExpect: 3,
			},
			expectedJoinLength:      3,
			expectedBootstrapExpect: 3,
			checkLocalName:          true,
		},
		{
			name:                    "Local node not in config",
			metadataVotersOnlyNodes: []string{},
			allNames:                []string{"node1", "node2", "node3"},
			localName:               "node4",
			initialRaftConfig: config.Raft{
				Join:            []string{"node1:8300", "node2:8301"},
				Port:            8300,
				BootstrapExpect: 2,
			},
			expectedJoinLength:      2,
			expectedBootstrapExpect: 1,
			checkLocalName:          true,
		},
		{
			name:                    "Non-storage nodes present (votes only)",
			metadataVotersOnlyNodes: []string{"node4", "node5"},
			allNames:                []string{"node1", "node2", "node3"},
			localName:               "node1",
			initialRaftConfig: config.Raft{
				Join:            []string{"node1:8300", "node2:8301"},
				Port:            8300,
				BootstrapExpect: 2,
			},
			expectedJoinLength:      2,
			expectedBootstrapExpect: 1,
			checkLocalName:          true,
			skipAllNames:            true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selector := mocks.NewSelector(t)
			appState := &state.State{
				Cluster: selector,
				ServerConfig: &config.WeaviateConfig{
					Config: config.Config{
						Raft: tc.initialRaftConfig,
					},
				},
				Logger: logrus.New(),
			}

			selector.On("NonStorageNodes").Return(tc.metadataVotersOnlyNodes)
			if !tc.skipAllNames {
				selector.On("AllNames").Return(tc.allNames)
			}

			if tc.checkLocalName {
				selector.On("LocalName").Return(tc.localName)
			}

			m, err := discoverNodes2Port(appState)
			require.NoError(t, err)
			adjustBootStrapExpect(appState, m)

			require.Equal(t, tc.expectedJoinLength, len(appState.ServerConfig.Config.Raft.Join))
			require.Equal(t, tc.expectedBootstrapExpect, appState.ServerConfig.Config.Raft.BootstrapExpect)
			selector.AssertExpectations(t)
		})
	}
}
