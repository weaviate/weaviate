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
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/vmihailenco/msgpack"
)

// Peer represents information about a peer in the Raft cluster
type Peer struct {
	Address  string `msgpack:"Address" json:"address"`
	ID       string `msgpack:"ID" json:"id"`
	Suffrage int64  `msgpack:"Suffrage" json:"-"`
	NonVoter bool   `msgpack:"-" json:"non_voter"`
}

// genPeersFileFromBolt checks if there was an existing raft.db and has persisted config
// so it generates peers.jon file to be used for raft.RecoverCluster to recover the cluster on start.
func (st *Store) genPeersFileFromBolt(raftDBPath, peerFilePath string) error {
	if _, err := os.Stat(peerFilePath); err == nil {
		// peers.json file exists, don't overwrite
		return nil
	}
	if _, err := os.Stat(raftDBPath); err != nil {
		// if files doesn't exist we bail out early
		return nil
	}
	bdb, err := raftbolt.NewBoltStore(raftDBPath)
	if err != nil {
		return err
	}
	defer bdb.Close()

	f, err := bdb.FirstIndex()
	if err != nil {
		st.log.Error("can not generate peers file from existed bolt db", err)
		return nil
	}
	l, err := bdb.LastIndex()
	if err != nil {
		st.log.Error("can not generate peers file from existed bolt db", err)
		return nil
	}

	if f == l {
		return nil
	}

	st.log.Info(fmt.Sprintf("found previous config at %s, recovery will be initiated", raftDBPath))

	log := new(raft.Log)
	var peers []Peer
	peersMap := make(map[string]Peer, l)
	for i := f; i <= l; i++ {
		err = bdb.GetLog(i, log)
		if err != nil {
			return err
		}

		if log.Type != raft.LogConfiguration {
			continue
		}

		// Unmarshal the MessagePack data into a map
		var decodedData map[string]interface{}
		err := msgpack.Unmarshal(log.Data, &decodedData)
		if err != nil {
			st.log.Error("unable to unmarshal msgpack from raft.db file err=" + err.Error())
			continue
		}

		// Extract the "Servers" array from the map
		servers, ok := decodedData["Servers"].([]interface{})
		if !ok {
			st.log.Error("failed to extract 'Servers' array")
			continue
		}

		// Iterate over the array and decode each Peer
		for _, server := range servers {
			serverMap, ok := server.(map[string]interface{})
			if !ok {
				st.log.Error("failed to convert server to map")
				continue
			}

			var peer Peer
			if address, ok := serverMap["Address"].(string); ok {
				peer.Address = address
			}
			if id, ok := serverMap["ID"].(string); ok {
				peer.ID = id
			}
			if suffrage, ok := serverMap["Suffrage"].(int64); ok {
				peer.Suffrage = suffrage
				peer.NonVoter = suffrage != int64(raft.Voter)
			}

			existedP := peersMap[peer.Address]
			if reflect.DeepEqual(existedP, peer) {
				continue
			}
			peersMap[peer.Address] = peer
			peers = append(peers, peer)
		}
	}

	if len(peers) == 0 {
		return nil
	}

	configData, err := json.MarshalIndent(peers, "", "  ")
	if err != nil {
		return err
	}

	file, err := os.OpenFile(peerFilePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(configData); err != nil {
		return err
	}

	return nil
}

// configViaPeers checks if the peers.json file exists with the provided path
// if the files exists it creates Raft.Configuration from it
func (st *Store) configViaPeers(peerFilePath string) (raft.Configuration, error) {
	var configuration raft.Configuration
	if _, err := os.Stat(peerFilePath); err != nil {
		// if files doesn't exist we bail out early
		return configuration, nil
	}

	st.log.Info(fmt.Sprintf("found %s file, collect peers from previous config...", peerFilePath))

	configuration, err := raft.ReadConfigJSON(peerFilePath)
	if err != nil {
		return configuration, fmt.Errorf("raft recovery failed to parse peers.json: %w", err)
	}
	if err := os.RemoveAll(peerFilePath); err != nil {
		return configuration, fmt.Errorf("recovery failed to delete %s, please delete manually ,err=%w", peerFilePath, err)
	}

	st.log.Info(fmt.Sprintf("deleted %s file and will proceed with the recovery", peerFilePath))

	st.log.Info("previous raft", "peers", fmt.Sprintf("%v", configuration.Servers))
	return configuration, nil
}

// recoverable validates that the peers.json file servers are is still alive
func (st *Store) recoverable(peers []raft.Server) ([]raft.Server, bool) {
	if len(peers) == 0 {
		return nil, false
	}

	var aliveServes []raft.Server
	serversMap := make(map[string]bool, len(peers))
	// sleep to wait for members to join memberlist
	t := time.NewTicker(st.recoveryTimeout)
	for {
		select {
		case <-t.C:
			return aliveServes, len(aliveServes) > 1
		default:
			st.log.Info("existing members in memberlist", "IPs", fmt.Sprintf("%v", st.cluster.AllHostnames()))
			for _, s := range peers {
				serverID := string(s.ID)
				// ignore not reachable based on their ID to be able to detect if they changed their IP.
				if !st.cluster.Alive(serverID) {
					st.log.Warn("node is not reachable, removing it from subsequent recovery", "ID", serverID)
					continue
				}

				if _, exists := serversMap[serverID]; exists {
					continue
				}
				aliveServes = append(aliveServes, s)
				serversMap[serverID] = true
			}

			if len(aliveServes) == len(peers) {
				return aliveServes, len(aliveServes) > 1
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}
