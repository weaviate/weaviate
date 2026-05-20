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

package vector_index_restrictions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

// TestRestrictions_MultiNode_AllNodesReturn422 pins that a
// restriction-violating class create returns the typed 422 /
// CONFIG_NOT_ALLOWED response on every node in a 3-node cluster, not
// just the node that happens to be RAFT leader. The concern (per
// Dirk's Slack note 2026-05-20): if the restriction validation error
// is generated only on the leader and propagated back through RAFT,
// the typed *restrictions.ViolationError may not survive RAFT's
// error wire format and would degrade to a generic 500, which the
// client would interpret as a transient server error and retry —
// hiding the misconfiguration from the operator.
//
// The current implementation validates pre-RAFT in
// Handler.AddClass.validateCanAddClass (see usecases/schema/class.go),
// so each node should reject locally before any RAFT round-trip. This
// test verifies that assumption directly by submitting the same
// violating payload to each node's REST endpoint and asserting the
// same typed body shape from each.
//
// If any node returns 500 (or a different shape), the assumption is
// wrong and the typed-error wire format needs to be ported through
// whatever inter-node path the rejection is travelling on (see
// adapters/handlers/rest/clusterapi/shared/indices_payloads.go in PR
// #11342 for an analogous pattern Dirk used for usage-limit errors
// in batch object writes).
func TestRestrictions_MultiNode_AllNodesReturn422(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		WithWeaviateEnv("ALLOWED_VECTOR_INDEX_TYPES", "hfresh,hnsw").
		WithWeaviateEnv("DEFAULT_VECTOR_INDEX", "hfresh").
		Start(ctx)
	require.NoError(t, err, "failed to start 3-node Weaviate testcontainer")
	defer func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate testcontainer: %v", err)
		}
	}()

	// Disallowed vector index type: flat is not in the allow-list.
	// Each iteration uses a unique class name so a 422 on node 1
	// doesn't leak state into node 2's test.
	classBody := func(name string) []byte {
		return []byte(`{
			"class":"` + name + `",
			"vectorizer":"none",
			"vectorIndexType":"flat"
		}`)
	}

	for i := 1; i <= 3; i++ {
		i := i
		t.Run("node_"+nodeLabel(i), func(t *testing.T) {
			uri := "http://" + compose.GetWeaviateNode(i).URI() + "/v1/schema"
			body := classBody("FlatRejectedNode" + nodeLabel(i))
			// Reuse the existing single-node assertion helper —
			// same wire contract is what we're testing.
			parsed := assertRestrictionViolation(t, ctx, uri, body, "vector_index_type", "flat")
			t.Logf("node %d returned typed 422: errorCode=%s restriction=%s value=%s allowed=%v",
				i, parsed.ErrorCode, parsed.Restriction, parsed.Value, parsed.Allowed)
		})
	}
}

// nodeLabel returns a short ASCII label for a node index. Avoids
// strconv.Itoa to keep imports minimal.
func nodeLabel(i int) string {
	return string(rune('0' + i))
}
