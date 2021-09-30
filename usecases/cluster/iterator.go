//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package cluster

import (
	"errors"
	"math/rand"
)

type NodeIterationStrategy int

const (
	StartRandom NodeIterationStrategy = iota
)

type NodeIterator struct {
	hostnames []string
	state     int
}

type HostnameSource interface {
	AllNames() []string
}

func NewNodeIterator(source HostnameSource,
	strategy NodeIterationStrategy) (*NodeIterator, error) {
	if strategy != StartRandom {
		return nil, errors.New("unsupported strategy")
	}

	hostnames := source.AllNames()

	return &NodeIterator{
		hostnames: hostnames,
		state:     rand.Intn(len(hostnames)),
	}, nil
}

func (n *NodeIterator) Next() string {
	curr := n.hostnames[n.state]
	n.state++
	if n.state == len(n.hostnames) {
		n.state = 0
	}

	return curr
}
