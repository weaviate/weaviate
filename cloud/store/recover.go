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
	"strings"
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

// generatePeersFileFromBolt checks if there was an existing raft.db and has persisted config
// so it generates peers.jon file to be used for raft.RecoverCluster to recover the cluster on start.
func (st *Store) generatePeersFileFromBolt(raftDBPath, peerFilePath string) error {
	if _, err := os.Stat(raftDBPath); err == nil {
		bdb, err := raftbolt.NewBoltStore(raftDBPath)
		if err != nil {
			return err
		}
		defer bdb.Close()

		f, err := bdb.FirstIndex()
		if err != nil {
			return err
		}
		l, err := bdb.LastIndex()
		if err != nil {
			return err
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
					st.log.Error("Failed to convert server to map")
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
	}
	return nil
}

// configViaPeers checks if the peers.json file exists with the provided path
// if the files exists it creates Raft.Configuration from it
func (st *Store) configViaPeers(peerFilePath string) (raft.Configuration, error) {
	var configuration raft.Configuration

	if _, err := os.Stat(peerFilePath); err == nil {
		st.log.Info(fmt.Sprintf("found %s file, collect peers from previous config...", peerFilePath))

		configuration, err = raft.ReadConfigJSON(peerFilePath)
		if err != nil {
			return configuration, fmt.Errorf("raft recovery failed to parse peers.json: %w", err)
		}
		if err := os.RemoveAll(peerFilePath); err != nil {
			return configuration, fmt.Errorf(fmt.Sprintf("recovery failed to delete %s, please delete manually", peerFilePath), err)
		}

		st.log.Info(fmt.Sprintf("deleted %s file and will proceed with the recovery", peerFilePath))
	}

	st.log.Info("previous raft", "peers", fmt.Sprintf("%v", configuration.Servers))
	return configuration, nil
}

// TODO: at the moment the validation of the provided address happens on the ip level but not ports,
// improvement would be validate also that the provided ip and ports exists in the memberlist.
// also we can make RecoverCluster configurable flag to be passed to allow recovery
// sleep to give chance for other nodes to join.
// recoverable
func (st *Store) recoverable(existedServers []raft.Server, sleepTime time.Duration) ([]raft.Server, bool) {
	if len(existedServers) == 0 {
		st.log.Warn("provided server list is empty, recovery is not possible")
		return nil, false
	}

	time.Sleep(sleepTime)

	st.log.Info("existed members in memeberlist", "IPs", fmt.Sprintf("%v", st.cluster.AllHostnames()))

	aliveNodes := []raft.Server{}
	for _, s := range existedServers {
		ip := strings.Split(string(s.Address), ":")[0]
		if !st.cluster.Alive(ip) {
			st.log.Warn("node is not alive", "IP", ip)
			continue
		}
		aliveNodes = append(aliveNodes, s)
	}

	if len(aliveNodes) == 0 {
		st.log.Warn("no alive nodes found to allow recovery")
		return nil, false
	}

	return aliveNodes, true
}
