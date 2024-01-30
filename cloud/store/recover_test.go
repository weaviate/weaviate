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

package store

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack"
)

func TestGeneratePeersFileFromBoltBad(t *testing.T) {
	raftpath := fmt.Sprintf("%s/raft.db", t.TempDir())
	store := testBoltStore(t, raftpath)
	err := store.StoreLogs([]*raft.Log{testRaftLog(raft.LogConfiguration, 1, []byte(""))})
	assert.Nil(t, err)
	store.Close()

	m := NewMockStore(t, "Node-1", 8080)
	err = m.store.genPeersFileFromBolt(raftpath, fmt.Sprintf("%s/peers.json", t.TempDir()))
	assert.Nil(t, err)

	// didn't create file because there was no server
	configuration, err := raft.ReadConfigJSON(fmt.Sprintf("%s/peers.json", t.TempDir()))
	assert.Error(t, err)
	assert.Equal(t, 0, len(configuration.Servers))
}

func TestGeneratePeersFileFromBoltGood(t *testing.T) {
	raftpath := fmt.Sprintf("%s/raft.db", t.TempDir())
	store := testBoltStore(t, raftpath)
	existedRaftCfg := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:       raft.ServerID("1"),
				Suffrage: raft.Voter,
				Address:  raft.ServerAddress("localhost:8001"),
			},
			{
				ID:       raft.ServerID("2"),
				Suffrage: raft.Voter,
				Address:  raft.ServerAddress("localhost:8002"),
			},
			{
				ID:       raft.ServerID("3"),
				Suffrage: raft.Nonvoter,
				Address:  raft.ServerAddress("localhost:8003"),
			},
		},
	}

	cfgByte, err := msgpack.Marshal(existedRaftCfg)
	assert.Nil(t, err)

	err = store.StoreLogs([]*raft.Log{
		testRaftLog(raft.LogConfiguration, 1, cfgByte),
		testRaftLog(raft.LogCommand, 2, cfgByte),
	})
	assert.Nil(t, err)
	store.Close()

	peers := fmt.Sprintf("%s/peers.json", t.TempDir())
	m := NewMockStore(t, "Node-1", 8080)
	err = m.store.genPeersFileFromBolt(raftpath, peers)
	assert.Nil(t, err)

	configuration, err := raft.ReadConfigJSON(peers)
	assert.Nil(t, err)

	assert.Equal(t, existedRaftCfg.Servers, configuration.Servers)
}

func TestRecoverable(t *testing.T) {
	m := NewMockStore(t, "Node-1", 8080)
	tcs := []struct {
		existedServer   []raft.Server
		expectedServers []raft.Server
		recoverable     bool
	}{
		{
			existedServer:   []raft.Server{},
			expectedServers: nil,
			recoverable:     false,
		},
		{
			existedServer: []raft.Server{
				{
					ID:       raft.ServerID("1"),
					Suffrage: raft.Voter,
					Address:  raft.ServerAddress("localhost:8001"),
				},
				{
					ID:       raft.ServerID("2"),
					Suffrage: raft.Voter,
					Address:  raft.ServerAddress("localhost:8002"),
				},
				{
					ID:       raft.ServerID("3"),
					Suffrage: raft.Nonvoter,
					Address:  raft.ServerAddress("localhost:8003"),
				},
			},
			expectedServers: []raft.Server{
				{
					ID:       raft.ServerID("1"),
					Suffrage: raft.Voter,
					Address:  raft.ServerAddress("localhost:8001"),
				},
				{
					ID:       raft.ServerID("2"),
					Suffrage: raft.Voter,
					Address:  raft.ServerAddress("localhost:8002"),
				},
				{
					ID:       raft.ServerID("3"),
					Suffrage: raft.Nonvoter,
					Address:  raft.ServerAddress("localhost:8003"),
				},
			},
			recoverable: true,
		},
		{
			existedServer:   []raft.Server{},
			expectedServers: nil,
			recoverable:     false,
		},
	}

	for _, tc := range tcs {
		m.store.cluster = NewMockCluster(tc.existedServer)
		resServers, recoverable := m.store.recoverable(tc.existedServer)
		assert.Equal(t, tc.expectedServers, resServers)
		assert.Equal(t, tc.recoverable, recoverable)
	}
}

func testBoltStore(t *testing.T, raftdbpath string) *raftbolt.BoltStore {
	fh, err := os.Create(raftdbpath)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Successfully creates and returns a store
	store, err := raftbolt.NewBoltStore(fh.Name())
	assert.Nil(t, err)

	return store
}

func testRaftLog(lt raft.LogType, idx int, data []byte) *raft.Log {
	return &raft.Log{
		Type:  lt,
		Data:  data,
		Index: uint64(idx),
	}
}

type MockCluster struct {
	list map[string]bool
}

func NewMockCluster(servers []raft.Server) *MockCluster {
	list := map[string]bool{}
	for _, s := range servers {
		list[strings.Split(string(s.ID), ":")[0]] = true
	}
	return &MockCluster{list}
}

func (m *MockCluster) AllHostnames() []string {
	ks := []string{}
	for k := range m.list {
		ks = append(ks, k)
	}
	return ks
}

func (m *MockCluster) Alive(ip string) bool {
	return m.list[ip]
}
