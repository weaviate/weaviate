package replication

import (
	"fmt"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
)

// ValidateReplicationReplicateShard validates that c is valid given the current state of the schema read using schemaReader
func ValidateReplicationReplicateShard(schemaReader schema.SchemaReader, c *cmd.ReplicationReplicateShardRequest) error {
	classInfo := schemaReader.ClassInfo(c.SourceCollection)
	// ClassInfo doesn't return an error, so the only way to know if the class exist is to check if the Exists
	// boolean is not set to default value
	if !classInfo.Exists {
		return fmt.Errorf("collection %s does not exists", c.SourceCollection)
	}

	// Ensure source shard replica exists and target replica doesn't already exist
	nodes, err := schemaReader.ShardReplicas(c.SourceCollection, c.SourceShard)
	if err != nil {
		return err
	}
	var foundSource bool
	var foundTarget bool
	for _, n := range nodes {
		if n == c.SourceNode {
			foundSource = true
		}
		if n == c.TargetNode {
			foundTarget = true
		}
	}
	if !foundSource {
		return fmt.Errorf("could not find shard %s for collection %s on source node %s", c.SourceShard, c.SourceCollection, c.SourceNode)
	}
	if foundTarget {
		return fmt.Errorf("shard %s already exist for collection %s on target node %s", c.SourceShard, c.SourceCollection, c.SourceNode)
	}
	return nil
}
