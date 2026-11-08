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

package distributedtask

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// sharedSchema is the one schema value every node's cutover writes and the
// FSM's fail apply reads.
type sharedSchema struct {
	mu       sync.Mutex
	landed   bool
	attempts map[string]int
	// landsWithError reports whether a node's n-th attempt commits the write
	// while its proposer sees an error; every other attempt writes nothing.
	landsWithError func(node string, n int) bool
}

// landedDetector answers the fail apply from the shared schema.
type landedDetector struct {
	*barrierRecordingProvider
	schema *sharedSchema
}

func (d *landedDetector) CutoverLanded(*Task) bool {
	d.schema.mu.Lock()
	defer d.schema.mu.Unlock()
	return d.schema.landed
}

func (d *landedDetector) CheckMigrationPropertyUpdate(string, string, []*Task) error { return nil }
func (d *landedDetector) CheckPropertyUpdate(string, string, []*Task) error          { return nil }
func (d *landedDetector) CheckClassMutation(string, []*Task) error                   { return nil }
func (d *landedDetector) CheckTenantMutation(string, []string, []*Task) error        { return nil }

type cutoverWritingProvider struct {
	*barrierRecordingProvider
	node   string
	schema *sharedSchema
}

func (p *cutoverWritingProvider) OnTaskCompleted(task *Task) error {
	if task.Status != TaskStatusSwapping {
		return nil
	}
	s := p.schema
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.landed {
		return nil // a cutover skips a value already at its target
	}
	s.attempts[p.node]++
	if s.landsWithError(p.node, s.attempts[p.node]) {
		s.landed = true
		return errors.New("rpc error after the leader committed")
	}
	return errors.New("no leader")
}

func TestFailApplyFinishesARunWhoseCutoverLanded(t *testing.T) {
	last := maxCompletedCallbackAttempts
	cases := []struct {
		name           string
		nodes          []string
		landsWithError func(node string, n int) bool
		want           TaskStatus
	}{
		{
			"single node, lands on the last attempt",
			[]string{"node-1"},
			func(_ string, n int) bool { return n == last }, TaskStatusFinished,
		},
		{
			"three nodes, node-1 lands on its last attempt",
			[]string{"node-1", "node-2", "node-3"},
			func(node string, n int) bool { return node == "node-1" && n == last }, TaskStatusFinished,
		},
		{"never lands", []string{"node-1"}, func(string, int) bool { return false }, TaskStatusFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBarrierHarness(t, tc.nodes)
			t.Cleanup(h.close)
			schema := &sharedSchema{attempts: map[string]int{}, landsWithError: tc.landsWithError}
			for i, node := range h.nodes {
				h.restartWith(i, &cutoverWritingProvider{barrierRecordingProvider: node.provider, node: node.id, schema: schema})
			}
			h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
				h.namespace: &landedDetector{barrierRecordingProvider: h.nodes[0].provider, schema: schema},
			})

			barrierAddTaskOneUnitPerNode(t, h, "t")
			barrierDriveToPreparing(t, h, "t")
			barrierDriveAllAcksAndAssertSwapping(t, h, "t")
			for range 3 * last {
				h.tickAll()
			}
			require.Equal(t, tc.want, h.getTask(t, "t").Status, "attempts %v", schema.attempts)
		})
	}
}
