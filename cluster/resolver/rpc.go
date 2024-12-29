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
	"strconv"
)

// Rpc implements resolving raft address to their RPC address depending on the configured rpcPort and whether or
// not this is a local cluster.
type Rpc struct {
	isLocalCluster bool
	rpcPort        int
}

// NewRpc returns an implementation of resolver
// isLocalHost is used to determine which remote port to expect given an address (See: rpcResolver.rpcAddressFromRAFT())
// rpcPort is used as the default port on the returned rpcAddresses (see: rpcResolver.Address())
func NewRpc(isLocalHost bool, rpcPort int) *Rpc {
	return &Rpc{isLocalCluster: isLocalHost, rpcPort: rpcPort}
}

// rpcAddressFromRAFT returns the rpc address (rpcAddr:rpcPort) based on raftAddr (raftAddr:raftPort).
// In a real cluster, the RPC port is the same for all nodes. In a local environment, the RPC ports need to be
// different. Specifically, we calculate the RPC port as the RAFT port + 1.
// Returns an error if raftAddr is not parseable.
// Returns an error if raftAddr port is not parse-able as an integer.
func (cl *Rpc) Address(raftAddr string) (string, error) {
	host, port, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", err
	}
	if !cl.isLocalCluster {
		return fmt.Sprintf("%s:%d", host, cl.rpcPort), nil
	}
	iPort, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, iPort+1), nil
}
