//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helper

import (
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

// StartCopyMovement registers a COPY of one shard from sourceNode onto
// targetNode and returns the op id. The replicate handler resolves no short
// class names, so qualifiedClass is the namespace-qualified name.
func StartCopyMovement(t *testing.T, qualifiedClass, shardName, sourceNode, targetNode, key string) strfmt.UUID {
	t.Helper()

	copyType := api.COPY.String()
	resp, err := Client(t).Replication.Replicate(
		replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
			Collection: &qualifiedClass,
			Shard:      &shardName,
			SourceNode: &sourceNode,
			TargetNode: &targetNode,
			Type:       &copyType,
		}),
		CreateAuth(key),
	)
	require.NoError(t, err, "failed to register COPY of %q %s->%s", shardName, sourceNode, targetNode)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.ID)
	return *resp.Payload.ID
}

// ReplicationOpState returns a replication op's current state and the error
// messages it has recorded. The messages say why a refused movement stopped,
// which tells it apart from one that is only slow.
func ReplicationOpState(t *testing.T, opID strfmt.UUID, key string) (string, []string, error) {
	t.Helper()

	details, err := Client(t).Replication.ReplicationDetails(
		replication.NewReplicationDetailsParams().WithID(opID), CreateAuth(key),
	)
	if err != nil {
		return "", nil, err
	}
	if details.Payload == nil || details.Payload.Status == nil {
		return "", nil, nil
	}

	messages := make([]string, 0, len(details.Payload.Status.Errors))
	for _, e := range details.Payload.Status.Errors {
		messages = append(messages, e.Message)
	}
	return details.Payload.Status.State, messages, nil
}

// ShardsForClass returns the sorted names of the shards in each node's shard
// map for a class, loaded or not. A tenant whose shard was refused is absent
// here while the schema still lists it. A node that does not answer lists no
// shards and no error, so an absence alone proves nothing.
func ShardsForClass(t *testing.T, qualifiedClass, key string) ([]string, error) {
	t.Helper()

	verbose := "verbose"
	params := nodes.NewNodesGetClassParams().WithClassName(qualifiedClass).WithOutput(&verbose)
	resp, err := Client(t).Nodes.NodesGetClass(params, CreateAuth(key))
	if err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	names := []string{}
	for _, node := range resp.Payload.Nodes {
		for _, shard := range node.Shards {
			if _, dup := seen[shard.Name]; dup {
				continue
			}
			seen[shard.Name] = struct{}{}
			names = append(names, shard.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}
