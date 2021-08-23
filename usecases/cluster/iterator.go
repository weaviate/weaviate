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
	AllHostnames() []string
}

func NewNodeIterator(source HostnameSource,
	strategy NodeIterationStrategy) (*NodeIterator, error) {
	if strategy != StartRandom {
		return nil, errors.New("unsupported strategy")
	}

	hostnames := source.AllHostnames()

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
