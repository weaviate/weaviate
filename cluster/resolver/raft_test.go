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

package resolver

import (
	"testing"

	raftImpl "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"

	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
)

func TestServerAddr(t *testing.T) {
	tests := []struct {
		name     string
		resolver *raft
		queryID  string
		want     raftImpl.ServerAddress
		wantErr  bool
	}{
		{
			name: "self uses LocalAddress",
			resolver: &raft{
				ClusterStateReader: clusterMocks.NewMockNodeSelector("node-1", "node-2"),
				RaftPort:           8300,
				IsLocalCluster:     false,
				NodeNameToPortMap:  map[string]int{},
				LocalName:          "node-1",
				LocalAddress:       "127.0.0.1:8300",
			},
			queryID: "node-1",
			want:    raftImpl.ServerAddress("127.0.0.1:8300"),
		},
		{
			name: "remote non-local cluster uses default raft port",
			resolver: &raft{
				ClusterStateReader: clusterMocks.NewMockNodeSelector("node-1", "node-2"),
				RaftPort:           8300,
				IsLocalCluster:     false,
				NodeNameToPortMap:  map[string]int{},
				LocalName:          "node-1",
				LocalAddress:       "127.0.0.1:8300",
			},
			queryID: "node-2",
			want:    raftImpl.ServerAddress("node-2:8300"),
		},
		{
			name: "local cluster uses port mapping",
			resolver: &raft{
				ClusterStateReader: clusterMocks.NewMockNodeSelector("node-1", "node-2"),
				RaftPort:           8300,
				IsLocalCluster:     true,
				NodeNameToPortMap:  map[string]int{"node-2": 8305},
				LocalName:          "node-1",
				LocalAddress:       "127.0.0.1:8301",
			},
			queryID: "node-2",
			want:    raftImpl.ServerAddress("node-2:8305"),
		},
		{
			name: "missing remote address returns error",
			resolver: &raft{
				ClusterStateReader: clusterMocks.NewMockNodeSelector("node-1"),
				RaftPort:           8300,
				IsLocalCluster:     false,
				NodeNameToPortMap:  map[string]int{},
				LocalName:          "node-1",
				LocalAddress:       "127.0.0.1:8300",
			},
			queryID: "node-2",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := tt.resolver.ServerAddr(raftImpl.ServerID(tt.queryID))
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, addr)
		})
	}
}
