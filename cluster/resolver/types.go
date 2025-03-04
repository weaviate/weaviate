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

const (
	invalidAddr = "256.256.256.256:99999999"
)

// ClusterStateReader allows the resolver to compute node-id to ip addresses.
type ClusterStateReader interface {
	// NodeAddress resolves node id into an ip address without the port.
	NodeAddress(id string) string
	// NodeHostname resolves a node id into an ip address with internal cluster api port
	NodeHostname(nodeName string) (string, bool)
	// LocalName returns the local node name
	LocalName() string
}

type RaftConfig struct {
	ClusterStateReader ClusterStateReader
	RaftPort           int
	IsLocalHost        bool
	NodeNameToPortMap  map[string]int
}

type FQDNConfig struct {
	RaftPort          int
	IsLocalHost       bool
	NodeNameToPortMap map[string]int
	TLD               string
}
