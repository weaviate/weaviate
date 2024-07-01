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

// NodeToAddress allows the resolver to compute node-id to ip addresses.
type NodeToAddress interface {
	// NodeAddress resolves node id into an ip address without the port.
	NodeAddress(id string) string
}

type RaftConfig struct {
	NodeToAddress     NodeToAddress
	RaftPort          int
	IsLocalHost       bool
	NodeNameToPortMap map[string]int
}
