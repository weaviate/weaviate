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

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
)

// The consumer matches these refusals by message, since the RPC hop drops error identity.
func TestReplicaAddRefusalsKeepTheirMessageAcrossRPC(t *testing.T) {
	tests := []struct {
		name     string
		sentinel error
	}{
		{name: "not finalizing", sentinel: replicationTypes.ErrAddReplicaOpNotFinalizing},
		{name: "cancellation in flight", sentinel: replicationTypes.ErrOpCancellationInFlight},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed := fromRPCError(toRPCError(fmt.Errorf("op 1: %w", tc.sentinel)))
			require.Contains(t, parsed.Error(), tc.sentinel.Error())
		})
	}
}
