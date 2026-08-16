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

package rest

import (
	"context"
	"errors"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// A DTM that has never answered is a node still bootstrapping, not an outage.
// Refusing there would name a migration nothing has been able to look for.
func TestShardReindexActivityBuilderBootstrapWindow(t *testing.T) {
	var listErr error
	logger, hook := logrustest.NewNullLogger()
	build := newShardReindexActivityBuilder(context.Background(), logger,
		func(context.Context) (map[string][]*distributedtask.Task, error) { return nil, listErr })

	listErr = errors.New("raft not ready")
	require.Nil(t, build(), "a DTM that has never answered must not refuse")
	require.Empty(t, hook.AllEntries(), "and must not be reported as an outage")

	listErr = nil
	require.NotNil(t, build(), "a readable list latches the gate on")
	listErr = errors.New("connection refused")
	require.True(t, build()("Movies", "shard-1"),
		"once DTM has answered, an unreadable list fails closed")
	require.Len(t, hook.AllEntries(), 1, "and that one is the outage")
}
