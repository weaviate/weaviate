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

package replication

import (
	"errors"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
)

var (
	ErrAlreadyExists = errors.New("already exists")
	ErrNodeNotFound  = errors.New("node not found")
	ErrClassNotFound = errors.New("class not found")
	ErrShardNotFound = errors.New("shard not found")
)

// ValidateReplicationReplicateShard validates that c is valid given the current state of the schema read using schemaReader
func ValidateReplicationReplicateShard(schemaReader schema.SchemaReader, c *api.ReplicationReplicateShardRequest) error {
	return validateReplicationReplicateShard(schemaReader, c.Uuid, c.SourceNode, c.TargetNode, c.SourceCollection, c.SourceShard)
}

// ValidateReplicationReplicateManyShards validates that c is valid given the current state of the schema read using schemaReader
func ValidateReplicationReplicateManyShards(schemaReader schema.SchemaReader, c *api.ReplicationReplicateManyShardsRequest) error {
	for _, r := range c.Requests {
		if err := validateReplicationReplicateShard(schemaReader, r.Uuid, r.SourceNode, r.TargetNode, r.SourceCollection, r.SourceShard); err != nil {
			return err
		}
	}
	return nil
}

func validateReplicationReplicateShard(schemaReader schema.SchemaReader, uuid strfmt.UUID, sourceNode, targetNode, collection, shard string) error {
	if uuid == "" {
		return fmt.Errorf("uuid is required: %w", ErrBadRequest)
	}
	if sourceNode == targetNode {
		return fmt.Errorf("source and target node are the same: %w", ErrBadRequest)
	}

	classInfo := schemaReader.ClassInfo(collection)
	// ClassInfo doesn't return an error, so the only way to know if the class exist is to check if the Exists
	// boolean is not set to default value
	if !classInfo.Exists {
		return fmt.Errorf("collection %s does not exists: %w", collection, ErrClassNotFound)
	}

	// Ensure source shard replica exists and target replica doesn't already exist
	nodes, err := schemaReader.ShardReplicas(collection, shard)
	if err != nil {
		return err
	}
	var foundSource bool
	var foundTarget bool
	for _, n := range nodes {
		if n == sourceNode {
			foundSource = true
		}
		if n == targetNode {
			foundTarget = true
		}
	}
	if !foundSource {
		return fmt.Errorf("could not find shard %s for collection %s on source node %s: %w", shard, collection, sourceNode, ErrNodeNotFound)
	}
	if foundTarget {
		return fmt.Errorf("shard %s already exist for collection %s on target node %s: %w", shard, collection, sourceNode, ErrAlreadyExists)
	}
	return nil
}
