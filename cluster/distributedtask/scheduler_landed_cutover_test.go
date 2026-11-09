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
	"github.com/weaviate/weaviate/entities/models"
)

// sharedSchema is the schema value the cutover writes and the FSM's fail
// apply reads.
type sharedSchema struct {
	mu       sync.Mutex
	landed   bool
	attempts int
	// landsOn is the attempt that commits the write while its proposer sees
	// an error; every other attempt writes nothing.
	landsOn int
}

// landedDetector answers the fail apply from the shared schema.
type landedDetector struct {
	schema *sharedSchema
}

func (d *landedDetector) CutoverLanded(*Task, func(string) *models.Class) bool {
	d.schema.mu.Lock()
	defer d.schema.mu.Unlock()
	return d.schema.landed
}

func (d *landedDetector) CheckMigrationPropertyUpdate(string, string, *Task) error { return nil }
func (d *landedDetector) CheckPropertyUpdate(string, string, []*Task) error        { return nil }
func (d *landedDetector) CheckClassMutation(string, []*Task) error                 { return nil }
func (d *landedDetector) CheckTenantMutation(string, []string, []*Task) error      { return nil }

type cutoverWritingProvider struct {
	*barrierRecordingProvider
	schema *sharedSchema
}

func (p *cutoverWritingProvider) OnTaskCompleted(task *Task) error {
	if task.Status != TaskStatusSwapping {
		return nil
	}
	s := p.schema
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.attempts == s.landsOn {
		s.landed = true
		return errors.New("rpc error after the leader committed")
	}
	return errors.New("no leader")
}

func TestFailApplyFinishesARunWhoseCutoverLanded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		landsOn int
		want    TaskStatus
	}{
		{"lands on the last attempt", maxCompletedCallbackAttempts, TaskStatusFinished},
		{"never lands", 0, TaskStatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBarrierHarness(t, []string{"node-1"})
			t.Cleanup(h.close)
			schema := &sharedSchema{landsOn: tc.landsOn}
			h.restartWith(0, &cutoverWritingProvider{barrierRecordingProvider: h.nodes[0].provider, schema: schema})
			h.manager.schemaMutationDetectors = map[string]SchemaMutationDetector{
				h.namespace: &landedDetector{schema: schema},
			}

			barrierAddTaskOneUnitPerNode(t, h, "t")
			barrierDriveToPreparing(t, h, "t")
			barrierDriveAllAcksAndAssertSwapping(t, h, "t")
			for range 3 * maxCompletedCallbackAttempts {
				h.tickAll()
			}
			require.Equal(t, tc.want, h.getTask(t, "t").Status, "attempts %d", schema.attempts)
		})
	}
}
