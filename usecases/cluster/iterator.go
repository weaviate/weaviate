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

package cluster

import (
	"fmt"
	"math/rand"
)

type NodeIterationStrategy int

const (
	StartRandom NodeIterationStrategy = iota
	StartAfter
)

type NodeIterator struct {
	hostnames []string
	state     int
}

type HostnameSource interface {
	AllNames() []string
}

func NewNodeIterator(nodeNames []string,
	strategy NodeIterationStrategy,
) (*NodeIterator, error) {
	if strategy != StartRandom && strategy != StartAfter {
		return nil, fmt.Errorf("unsupported strategy: %v", strategy)
	}

	startState := 0
	if strategy == StartRandom {
		startState = rand.Intn(len(nodeNames))
	}

	return &NodeIterator{
		hostnames: nodeNames,
		state:     startState,
	}, nil
}

func (n *NodeIterator) SetStartNode(startNode string) {
	for i, node := range n.hostnames {
		if node == startNode {
			n.state = i + 1
			if n.state == len(n.hostnames) {
				n.state = 0
			}
			break
		}
	}
}

func (n *NodeIterator) Next() string {
	curr := n.hostnames[n.state]
	n.state++
	if n.state == len(n.hostnames) {
		n.state = 0
	}

	return curr
}
