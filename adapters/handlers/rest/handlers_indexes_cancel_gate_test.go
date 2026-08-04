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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
)

// scriptedCleanupProber answers each node from its own script, repeating the
// last entry once exhausted, and counts the calls.
type scriptedCleanupProber struct {
	mu      sync.Mutex
	script  map[string][]cleanupAnswer
	calls   map[string]int
	queried []string
}

type cleanupAnswer struct {
	up  bool
	err error
}

func (p *scriptedCleanupProber) CleanupInProgress(_ context.Context, node, collection string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.calls == nil {
		p.calls = map[string]int{}
	}
	p.queried = append(p.queried, node+"/"+collection)
	answers := p.script[node]
	i := p.calls[node]
	p.calls[node]++
	if i >= len(answers) {
		if len(answers) == 0 {
			return false, nil
		}
		i = len(answers) - 1
	}
	return answers[i].up, answers[i].err
}

func (p *scriptedCleanupProber) callsFor(node string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[node]
}

func gateHandlers(prober reindexCleanupProber, nodes ...string) (*indexesHandlers, *logrustest.Hook) {
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return &indexesHandlers{
		appState:       &state.State{Logger: logger},
		cluster:        fixedMembership(nodes),
		reindexCleanup: prober,
	}, hook
}

func warned(hook *logrustest.Hook, fragment string) *logrus.Entry {
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, fragment) {
			return e
		}
	}
	return nil
}

// Covers the window described on awaitOwnerCleanupGates: the answer must wait
// for the owners, and must not be blockable by one that cannot reply.
func TestAwaitOwnerCleanupGates(t *testing.T) {
	const (
		local      = "node1"
		owner      = "node2"
		collection = "Movies"
	)
	payload := &db.ReindexTaskPayload{
		Collection: collection,
		UnitToNode: map[string]string{"u1": owner, "u2": local},
	}

	t.Run("waits until the owner has raised its gate", func(t *testing.T) {
		prober := &scriptedCleanupProber{script: map[string][]cleanupAnswer{
			owner: {{up: false}, {up: false}, {up: true}},
		}}
		h, hook := gateHandlers(prober, local, owner)

		h.awaitOwnerCleanupGates(context.Background(), payload, collection, "task-1")

		assert.GreaterOrEqual(t, prober.callsFor(owner), 3,
			"the owner must be re-asked until its gate is up")
		assert.Nil(t, warned(hook, "could not confirm"),
			"the gate came up; nothing is degraded")
	})

	t.Run("does not ask the local node about itself", func(t *testing.T) {
		prober := &scriptedCleanupProber{script: map[string][]cleanupAnswer{
			owner: {{up: true}},
		}}
		h, _ := gateHandlers(prober, local, owner)

		h.awaitOwnerCleanupGates(context.Background(), payload, collection, "task-1")

		assert.Zero(t, prober.callsFor(local),
			"this node raised its own gate synchronously; asking itself over HTTP is pointless")
	})

	t.Run("an unreachable owner is bounded, not fatal", func(t *testing.T) {
		prober := &scriptedCleanupProber{script: map[string][]cleanupAnswer{
			owner: {{err: errors.New("connection refused")}},
		}}
		h, hook := gateHandlers(prober, local, owner)

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		h.awaitOwnerCleanupGates(ctx, payload, collection, "task-1")

		assert.Less(t, time.Since(start), reindexOwnerGateTimeout,
			"a cancel must never be blockable by an unreachable node")
		entry := warned(hook, "could not confirm")
		require.NotNil(t, entry, "the degraded path has to be visible to the operator")
		assert.Equal(t, "reindex_cancel_gate_unconfirmed", entry.Data["audit_event"])
		assert.Contains(t, entry.Data["nodes"], owner)
	})

	t.Run("an owner without the route is not polled at all", func(t *testing.T) {
		prober := &scriptedCleanupProber{script: map[string][]cleanupAnswer{
			owner: {{err: clients.ErrReindexCleanupUnsupported}},
		}}
		h, hook := gateHandlers(prober, local, owner)

		start := time.Now()
		h.awaitOwnerCleanupGates(context.Background(), payload, collection, "task-1")

		assert.Equal(t, 1, prober.callsFor(owner),
			"an older build can never answer; polling it burns the budget for nothing")
		assert.Less(t, time.Since(start), reindexOwnerGatePollInterval,
			"the answer is known immediately")
		require.NotNil(t, warned(hook, "could not confirm"))
	})

	t.Run("no remote owners means no probing", func(t *testing.T) {
		prober := &scriptedCleanupProber{}
		h, hook := gateHandlers(prober, local)

		h.awaitOwnerCleanupGates(context.Background(),
			&db.ReindexTaskPayload{Collection: collection, UnitToNode: map[string]string{"u1": local}},
			collection, "task-1")

		assert.Empty(t, prober.queried)
		assert.Nil(t, warned(hook, "could not confirm"))
	})
}
