//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"fmt"

	"github.com/pkg/errors"
)

type ConsistencyLevel string

const (
	One    ConsistencyLevel = "ONE"
	Quorum ConsistencyLevel = "QUORUM"
	All    ConsistencyLevel = "ALL"
)

func cLevel(l ConsistencyLevel, n int) int {
	switch l {
	case All:
		return n
	case Quorum:
		return n/2 + 1
	default:
		return 1
	}
}

var (
	errNoReplicaFound = errors.New("no replica found")
	errUnresolvedName = errors.New("unresolved node name")
)

// resolver finds replicas and resolves theirs names
type resolver struct {
	schema shardingState
	nodeResolver
	class string
}

// State returns replicas state
func (r *resolver) State(shardName string, cl ConsistencyLevel) (res rState, err error) {
	res.CLevel = cl
	res.Hosts, res.nodes, err = r.schema.ResolveParentNodes(r.class, shardName)
	if err != nil {
		return res, err
	}
	if res.Len() == 0 {
		return res, errNoReplicaFound
	}

	res.Level, err = res.ConsistencyLevel(cl)
	return res, err
}

// rState replicas state
type rState struct {
	CLevel ConsistencyLevel
	Level  int
	Hosts  []string // successfully resolved names
	nodes  []string // names which could not be resolved
}

// Len returns the number of replica
func (r *rState) Len() int {
	return len(r.Hosts) + len(r.nodes)
}

// ConsistencyLevel returns consistency level when it is satisfied
func (r *rState) ConsistencyLevel(l ConsistencyLevel) (int, error) {
	level := cLevel(l, r.Len())
	if n := len(r.Hosts); level > n {
		return 0, fmt.Errorf("consistency level (%d) > available replicas(%d): %w :%v", level, n, errUnresolvedName, r.nodes)
	}
	return level, nil
}
