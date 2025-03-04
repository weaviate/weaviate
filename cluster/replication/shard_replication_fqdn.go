package replication

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
