//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package additional

// ReplicationProperties are replication-related handles and configurations which
// allow replication context to pass through different layers of
// abstraction, usually initiated via client requests
type ReplicationProperties struct {
	// ConsistencyLevel indicates how many nodes should
	// respond to a request before it is considered
	// successful. Can be "ONE", "QUORUM", or "ALL"
	//
	// This is only relevant for a replicated
	// class
	ConsistencyLevel string

	// NodeName is the node which is expected to
	// fulfill the request
	NodeName string
}

type AsyncReplicationTargetNodeOverride struct {
	CollectionID         string
	ShardID              string
	SourceNode           string
	TargetNode           string
	UpperTimeBound       int64
	NoDeletionResolution bool
}

type AsyncReplicationTargetNodeOverrides []AsyncReplicationTargetNodeOverride

func (left *AsyncReplicationTargetNodeOverride) Equal(right *AsyncReplicationTargetNodeOverride) bool {
	return left.SourceNode == right.SourceNode && left.TargetNode == right.TargetNode && left.CollectionID == right.CollectionID && left.ShardID == right.ShardID
}

func (overrides AsyncReplicationTargetNodeOverrides) NoDeletionResolution(targetNode string) bool {
	for _, override := range overrides {
		if override.TargetNode == targetNode {
			return override.NoDeletionResolution
		}
	}
	return false
}
