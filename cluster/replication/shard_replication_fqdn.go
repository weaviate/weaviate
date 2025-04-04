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

package replication

import "fmt"

// shardFQDN uniquely identify a shard in a weaviate cluster
type shardFQDN struct {
	// nodeId is the node containing the shard
	nodeId string
	// collectionId is the collection containing the shard
	collectionId string
	// shardId is the id of the shard
	shardId string
}

func newShardFQDN(nodeId, collectionId, shardId string) shardFQDN {
	return shardFQDN{
		nodeId:       nodeId,
		collectionId: collectionId,
		shardId:      shardId,
	}
}

func (s shardFQDN) String() string {
	return fmt.Sprintf("%s/%s/%s", s.nodeId, s.collectionId, s.shardId)
}
