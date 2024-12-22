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
	"github.com/liutizhong/weaviate/cluster/log"
)

type fqdn struct {
	// RaftPort is the configured RAFT port in the cluster that the resolver will append to the node id.
	RaftPort int
	// IsLocalCluster is the cluster running on a single host machine. This is necessary to ensure that we don't use the
	// same port multiple time when we only have a single underlying machine.
	IsLocalCluster bool
	// NodeNameToPortMap maps a given node name ot a given port. This is useful when running locally so that we can
	// keep in memory which node uses which port.
	NodeNameToPortMap map[string]int
	// TLD is a string that if set well be appended to the node-id using the format <node-id>.<TLD>. This allows
	// weaviate to resolve node id to name in a namespaces environment where node id don't directly translate to an ip
	// at the DNS layer.
	TLD string

	nodesLock        sync.Mutex
	notResolvedNodes map[raftImpl.ServerID]struct{}
}

func NewFQDN(cfg FQDNConfig) *fqdn {
	return &fqdn{
		TLD:               cfg.TLD,
		RaftPort:          cfg.RaftPort,
		IsLocalCluster:    cfg.IsLocalHost,
		NodeNameToPortMap: cfg.NodeNameToPortMap,
		notResolvedNodes:  make(map[raftImpl.ServerID]struct{}),
	}
}

// NodeAddress resolves a node id to an IP address, returns empty string if host is not found.
// This is useful to implement as it is the same interface implemented by cluster state to do memberlist based lookups
func (r *fqdn) NodeAddress(id string) string {
	fqdn := string(id)
	if r.TLD != "" {
		fqdn += "." + r.TLD
	}
	addr, err := net.LookupIP(fqdn)
	if err != nil || len(addr) < 1 {
		return ""
	}
	return addr[0].String()
}

// ServerAddr resolves server ID to a RAFT address
func (r *fqdn) ServerAddr(id raftImpl.ServerID) (raftImpl.ServerAddress, error) {
	// Get the address from the node id
	fqdn := string(id)
	if r.TLD != "" {
		fqdn += "." + r.TLD
	}
	addr, err := net.LookupIP(fqdn)
	r.nodesLock.Lock()
	defer r.nodesLock.Unlock()
	if err != nil || len(addr) < 1 {
		r.notResolvedNodes[id] = struct{}{}
		return raftImpl.ServerAddress(invalidAddr), nil
	}
	delete(r.notResolvedNodes, id)

	// If we are not running a local cluster we can immediately return, otherwise we need to lookup the port of the node
	// as we can't use the default raft port locally.
	if !r.IsLocalCluster {
		return raftImpl.ServerAddress(fmt.Sprintf("%s:%d", addr[0], r.RaftPort)), nil
	}
	return raftImpl.ServerAddress(fmt.Sprintf("%s:%d", addr[0], r.NodeNameToPortMap[string(id)])), nil
}

// NewTCPTransport returns a new fqdn.NetworkTransportConfig that utilizes
// this resolver to resolve addresses based on server IDs.
// This is particularly crucial as K8s assigns new IPs on each node restart.
func (r *fqdn) NewTCPTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logger *logrus.Logger,
) (*raftImpl.NetworkTransport, error) {
	cfg := &raftImpl.NetworkTransportConfig{
		ServerAddressProvider: r,
		MaxPool:               raftTcpMaxPool,
		Timeout:               raftTcpTimeout,
		Logger:                log.NewHCLogrusLogger("raft-net", logger),
	}
	return raftImpl.NewTCPTransportWithConfig(bindAddr, advertise, cfg)
}

func (r *fqdn) NotResolvedNodes() map[raftImpl.ServerID]struct{} {
	return r.notResolvedNodes
}
