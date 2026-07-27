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

package rpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
)

// TestRPCErrorRoundTrip_SchemaBadRequest pins the leader-hop translation: the
// schema FSM's client-fault rejections (drop-vector marker refusal, removal
// gate) must survive server→wire→client and rebuild as an errors.Is-able
// sentinel — unmapped they arrive as codes.Internal and surface as HTTP 500
// instead of 422.
func TestRPCErrorRoundTrip_SchemaBadRequest(t *testing.T) {
	orig := fmt.Errorf("%w: a previous drop of [v1] on \"C\" is still completing; retry",
		clusterSchema.ErrBadRequest)

	wire := toRPCError(orig)
	require.Error(t, wire)

	back := fromRPCError(wire)
	require.ErrorIs(t, back, clusterSchema.ErrBadRequest,
		"the sentinel must be rebuilt on the forwarding node")
}
