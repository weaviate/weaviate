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
	"testing"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack"
)

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

func testRaftLog(idx int, data []byte) *raft.Log {
	return &raft.Log{
		Data:  data,
		Index: uint64(idx),
	}
}

func TestGeneratePeersFileFromBoltBad(t *testing.T) {
	raftpath := fmt.Sprintf("%s/raft.db", t.TempDir())
	store := testBoltStore(t, raftpath)
	err := store.StoreLogs([]*raft.Log{testRaftLog(1, []byte(""))})
	assert.Nil(t, err)
	store.Close()

	logger := NewMockSLog(t)
	s := &Store{log: logger.Logger}
	err = s.generatePeersFileFromBolt(raftpath, fmt.Sprintf("%s/peers.json", t.TempDir()))
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

	err = store.StoreLogs([]*raft.Log{testRaftLog(1, cfgByte)})
	assert.Nil(t, err)
	store.Close()

	peers := fmt.Sprintf("%s/peers.json", t.TempDir())
	logger := NewMockSLog(t)
	s := &Store{log: logger.Logger}
	err = s.generatePeersFileFromBolt(raftpath, peers)
	assert.Nil(t, err)

	configuration, err := raft.ReadConfigJSON(peers)
	assert.Nil(t, err)

	assert.Equal(t, existedRaftCfg.Servers, configuration.Servers)
}
