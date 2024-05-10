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
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

// addressResolver resolves server id into an ip
type addressResolver interface {
	// NodeAddress resolves node id into an ip address without the port
	NodeAddress(id string) string
}

type addrResolver struct {
	addressResolver
	RaftPort int
	// IsLocalCluster is cluster running Weaviate from the console in localhost
	IsLocalCluster   bool
	NodeName2PortMap map[string]int

	nodesLock        sync.Mutex
	notResolvedNodes map[raft.ServerID]struct{}
}

func newAddrResolver(cfg *Config) *addrResolver {
	return &addrResolver{
		addressResolver:  cfg.AddrResolver,
		RaftPort:         cfg.RaftPort,
		IsLocalCluster:   cfg.IsLocalHost,
		NodeName2PortMap: cfg.ServerName2PortMap,
		notResolvedNodes: make(map[raft.ServerID]struct{}),
	}
}

// ServerAddr resolves server ID to a RAFT address
func (a *addrResolver) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	addr := a.addressResolver.NodeAddress(string(id))

	a.nodesLock.Lock()
	defer a.nodesLock.Unlock()
	if addr == "" {
		a.notResolvedNodes[id] = struct{}{}
		return "", fmt.Errorf("could not resolve server id %s", id)
	}
	delete(a.notResolvedNodes, id)

	if !a.IsLocalCluster {
		return raft.ServerAddress(fmt.Sprintf("%s:%d", addr, a.RaftPort)), nil
	}

	// This is only necessary for running Weaviate from the console in localhost
	return raft.ServerAddress(fmt.Sprintf("%s:%d", addr, a.NodeName2PortMap[string(id)])), nil
}

// NewTCPTransport returns a new raft.NetworkTransportConfig that utilizes
// this resolver to resolve addresses based on server IDs.
// This is particularly crucial as K8s assigns new IPs on each node restart.
func (a *addrResolver) NewTCPTransport(
	bindAddr string, advertise net.Addr,
	maxPool int, timeout time.Duration, logger *logrus.Logger,
) (*raft.NetworkTransport, error) {
	cfg := &raft.NetworkTransportConfig{
		ServerAddressProvider: a,
		MaxPool:               tcpMaxPool,
		Timeout:               tcpTimeout,
		Logger:                NewHCLogrusLogger("raft-net", logger),
	}

	return raft.NewTCPTransportWithConfig(bindAddr, advertise, cfg)
}
