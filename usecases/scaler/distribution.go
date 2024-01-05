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

package scaler

import (
	"fmt"

	"github.com/weaviate/weaviate/usecases/sharding"
)

type (
	// ShardDist shard distribution over nodes
	ShardDist map[string][]string
	// nodeShardDist map a node its shard distribution
	nodeShardDist map[string]ShardDist
)

// distributions returns shard distribution for local node as well as remote nodes
func distributions(before, after *sharding.State) (ShardDist, nodeShardDist) {
	localDist := make(ShardDist, len(before.Physical))
	nodeDist := make(map[string]ShardDist)
	for name := range before.Physical {
		newNodes := difference(after.Physical[name].BelongsToNodes, before.Physical[name].BelongsToNodes)
		if before.IsLocalShard(name) {
			localDist[name] = newNodes
		} else {
			belongsTo := before.Physical[name].BelongsToNode()
			dist := nodeDist[belongsTo]
			if dist == nil {
				dist = make(map[string][]string)
				nodeDist[belongsTo] = dist
			}
			dist[name] = newNodes
		}
	}
	return localDist, nodeDist
}

// nodes return node names
func (m nodeShardDist) nodes() []string {
	ns := make([]string, 0, len(m))
	for node := range m {
		ns = append(ns, node)
	}
	return ns
}

// hosts resolve node names into host addresses
func hosts(nodes []string, resolver cluster) ([]string, error) {
	hs := make([]string, len(nodes))
	for i, node := range nodes {
		host, ok := resolver.NodeHostname(node)
		if !ok {
			return nil, fmt.Errorf("%w, %q", ErrUnresolvedName, node)
		}
		hs[i] = host
	}
	return hs, nil
}

// shards return names of all shards
func (m ShardDist) shards() []string {
	ns := make([]string, 0, len(m))
	for node := range m {
		ns = append(ns, node)
	}
	return ns
}

// difference returns elements in xs which doesn't exists in ys
func difference(xs, ys []string) []string {
	m := make(map[string]struct{}, len(ys))
	for _, y := range ys {
		m[y] = struct{}{}
	}
	rs := make([]string, 0, len(ys))
	for _, x := range xs {
		if _, ok := m[x]; !ok {
			rs = append(rs, x)
		}
	}
	return rs
}
