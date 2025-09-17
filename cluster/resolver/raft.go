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
	"fmt"
	"net"
	"sync"
	"time"

	raftImpl "github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/log"
)

type raft struct {
	// ClusterStateReader allows the raft to also be used to the current cluster state
	ClusterStateReader

	// RaftPort is the configured RAFT port in the cluster that the resolver will append to the node id.
	RaftPort int
	// IsLocalCluster is the cluster running on a single host machine. This is necessary to ensure that we don't use the
	// same port multiple time when we only have a single underlying machine.
	IsLocalCluster bool
	// NodeNameToPortMap maps a given node name ot a given port. This is useful when running locally so that we can
	// keep in memory which node uses which port.
	NodeNameToPortMap map[string]int
	// LocalName is the name of the local node
	LocalName string
	// LocalAddress is the address of the local node
	LocalAddress string

	notResolvedNodes sync.Map
}

func NewRaft(cfg RaftConfig) *raft {
	return &raft{
		ClusterStateReader: cfg.ClusterStateReader,
		RaftPort:           cfg.RaftPort,
		IsLocalCluster:     cfg.IsLocalHost,
		NodeNameToPortMap:  cfg.NodeNameToPortMap,
		notResolvedNodes:   sync.Map{},
		LocalName:          cfg.LocalName,
		LocalAddress:       cfg.LocalAddress,
	}
}

// ServerAddr resolves server ID to a RAFT address
// it's thread safe see https://github.com/hashicorp/raft/blob/main/net_transport.go#L389-L391
func (a *raft) ServerAddr(id raftImpl.ServerID) (raftImpl.ServerAddress, error) {
	// Get the address from the node id
	if id == raftImpl.ServerID(a.LocalName) {
		return raftImpl.ServerAddress(a.LocalAddress), nil
	}
	addr := a.ClusterStateReader.NodeAddress(string(id))

	// Update the internal notResolvedNodes if the addr if empty, otherwise delete it from the map
	if addr == "" {
		a.notResolvedNodes.Store(id, struct{}{})
		return "", fmt.Errorf("could not resolve server id %s", id)
	}
	a.notResolvedNodes.Delete(id)

	// If we are not running a local cluster we can immediately return, otherwise we need to lookup the port of the node
	// as we can't use the default raft port locally.
	if !a.IsLocalCluster {
		return raftImpl.ServerAddress(fmt.Sprintf("%s:%d", addr, a.RaftPort)), nil
	}
	port, exists := a.NodeNameToPortMap[string(id)]
	if !exists {
		// if does not exist, use the default raft port, self healing from bad config
		port = a.RaftPort
	}
	return raftImpl.ServerAddress(fmt.Sprintf("%s:%d", addr, port)), nil
}

// NewTCPTransport returns a new raft.NetworkTransportConfig that utilizes
// this resolver to resolve addresses based on server IDs.
// This is particularly crucial as K8s assigns new IPs on each node restart.
func (a *raft) NewTCPTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logger *logrus.Logger,
) (*raftImpl.NetworkTransport, error) {
	cfg := &raftImpl.NetworkTransportConfig{
		ServerAddressProvider: a,
		MaxPool:               maxPool,
		Timeout:               timeout,
		Logger:                log.NewHCLogrusLogger("raft-net", logger),
	}
	return raftImpl.NewTCPTransportWithConfig(bindAddr, advertise, cfg)
}

func (a *raft) NotResolvedNodes() map[raftImpl.ServerID]struct{} {
	notResolvedNodes := make(map[raftImpl.ServerID]struct{})
	a.notResolvedNodes.Range(func(key, value any) bool {
		notResolvedNodes[key.(raftImpl.ServerID)] = struct{}{}
		return true
	})
	return notResolvedNodes
}

// AllOtherClusterMembers returns all cluster members discovered via memberlist with their raft addresses
func (a *raft) AllOtherClusterMembers(raftPort int) map[string]string {
	return a.ClusterStateReader.AllOtherClusterMembers(raftPort)
}
