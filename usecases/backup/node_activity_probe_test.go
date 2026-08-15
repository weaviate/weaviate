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

package backup

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/backup"
)

// slots drives operation state through the same calls the backup subsystem makes.
type slots struct {
	coordinatorBackup  *backupStat
	coordinatorRestore *backupStat
	participantBackup  *backupStat
	participantRestore *backupStat
}

var idle = NodeActivity{Answered: true}

func busyWith(kind, id string) NodeActivity {
	return NodeActivity{Answered: true, Busy: true, Kind: kind, ID: id}
}

func newIdleProbe() *NodeActivityProbe {
	return NewNodeActivityProbe(&Handler{backupper: &backupper{}, restorer: &restorer{}})
}

func newProbeFixture() (*NodeActivityProbe, slots) {
	participant := &Handler{backupper: &backupper{}, restorer: &restorer{}}
	scheduler := &Scheduler{backupper: &coordinator{}, restorer: &coordinator{}}
	probe := NewNodeActivityProbe(participant)
	probe.attachScheduler(scheduler)
	return probe, slots{
		coordinatorBackup:  &scheduler.backupper.lastOp,
		coordinatorRestore: &scheduler.restorer.lastOp,
		participantBackup:  &participant.backupper.lastOp,
		participantRestore: &participant.restorer.lastOp,
	}
}

func hold(stat *backupStat, id string) {
	stat.renew(id, "/somewhere", "bucket", "path")
}

func TestNodeActivityProbe(t *testing.T) {
	tests := []struct {
		name  string
		setUp func(s slots)
		want  NodeActivity
	}{
		{
			name:  "nothing is running",
			setUp: func(s slots) {},
			want:  idle,
		},
		{
			name:  "this node participates in a backup",
			setUp: func(s slots) { hold(s.participantBackup, "b1") },
			want:  busyWith("backup", "b1"),
		},
		{
			name:  "this node participates in a restore",
			setUp: func(s slots) { hold(s.participantRestore, "r1") },
			want:  busyWith("restore", "r1"),
		},
		{
			name:  "this node coordinates a backup",
			setUp: func(s slots) { hold(s.coordinatorBackup, "b2") },
			want:  busyWith("backup", "b2"),
		},
		{
			name:  "this node coordinates a restore",
			setUp: func(s slots) { hold(s.coordinatorRestore, "r2") },
			want:  busyWith("restore", "r2"),
		},
		{
			name: "all four slots hold, the coordinator's backup is reported",
			setUp: func(s slots) {
				hold(s.coordinatorBackup, "b2")
				hold(s.coordinatorRestore, "r2")
				hold(s.participantBackup, "b1")
				hold(s.participantRestore, "r1")
			},
			want: busyWith("backup", "b2"),
		},
		{
			name: "a coordinated restore outranks a participated backup",
			setUp: func(s slots) {
				hold(s.coordinatorRestore, "r2")
				hold(s.participantBackup, "b1")
			},
			want: busyWith("restore", "r2"),
		},
		{
			name: "a participated backup outranks a participated restore",
			setUp: func(s slots) {
				hold(s.participantBackup, "b1")
				hold(s.participantRestore, "r1")
			},
			want: busyWith("backup", "b1"),
		},
		{
			name: "a running backup that moved on from its first status",
			setUp: func(s slots) {
				hold(s.participantBackup, "b1")
				s.participantBackup.set(backup.Transferring)
			},
			want: busyWith("backup", "b1"),
		},
		{
			name: "a cancelled backup still occupies its slot",
			setUp: func(s slots) {
				hold(s.participantBackup, "b1")
				s.participantBackup.set(backup.Cancelled)
			},
			want: busyWith("backup", "b1"),
		},
		{
			// Scheduler.CancelRestore writes Cancelled whether or not it still holds
			// the slot, which is where a released slot with a status comes from.
			name: "a released slot that a late cancel wrote a status to",
			setUp: func(s slots) {
				hold(s.coordinatorRestore, "r2")
				s.coordinatorRestore.reset()
				s.coordinatorRestore.set(backup.Cancelled)
			},
			want: idle,
		},
		{
			name: "a released slot that a late failure wrote a status to",
			setUp: func(s slots) {
				hold(s.participantBackup, "b1")
				s.participantBackup.reset()
				s.participantBackup.setFailed("the coordinator went away")
			},
			want: idle,
		},
		{
			name: "every slot released again",
			setUp: func(s slots) {
				hold(s.coordinatorBackup, "b2")
				hold(s.participantRestore, "r1")
				s.coordinatorBackup.reset()
				s.participantRestore.reset()
			},
			want: idle,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe, s := newProbeFixture()
			tt.setUp(s)

			assert.Equal(t, tt.want, probe.Activity())
		})
	}
}

// Every row here coordinates something: an idle Scheduler reads the same whether
// or not it is attached, so only a busy one proves the registration.
func TestNewSchedulerRegistersWithTheProbe(t *testing.T) {
	tests := []struct {
		name  string
		setUp func(s *Scheduler)
		want  NodeActivity
	}{
		{
			name:  "the scheduler coordinates a backup",
			setUp: func(s *Scheduler) { hold(&s.backupper.lastOp, "b1") },
			want:  busyWith("backup", "b1"),
		},
		{
			name:  "the scheduler coordinates a restore",
			setUp: func(s *Scheduler) { hold(&s.restorer.lastOp, "r1") },
			want:  busyWith("restore", "r1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe := newIdleProbe()

			scheduler := NewScheduler(nil, nil, nil, nil, nil, nil, nil,
				&fakeSchemaManger{}, nil, probe, logrus.New())
			tt.setUp(scheduler)

			assert.Equal(t, tt.want, probe.Activity())
		})
	}
}

func TestNewSchedulerRefusesToBuildWithoutAProbe(t *testing.T) {
	assert.PanicsWithValue(t, "backup: NewScheduler needs a node-activity probe", func() {
		NewScheduler(nil, nil, nil, nil, nil, nil, nil, &fakeSchemaManger{}, nil, nil, logrus.New())
	})
}

func TestNewNodeActivityProbeRefusesToBuildWithoutAParticipant(t *testing.T) {
	assert.PanicsWithValue(t, "backup: NewNodeActivityProbe needs a participant", func() { NewNodeActivityProbe(nil) })
}

// A probe can arrive before the Scheduler is built, which happens well after
// the participant. Answering from the participant slots alone is right then,
// since a node with no Scheduler cannot be coordinating anything.
func TestNodeActivityProbeBeforeSchedulerAttached(t *testing.T) {
	tests := []struct {
		name  string
		setUp func(participantBackup *backupStat)
		want  NodeActivity
	}{
		{
			name:  "nothing is running",
			setUp: func(participantBackup *backupStat) {},
			want:  idle,
		},
		{
			name:  "this node participates in a backup",
			setUp: func(participantBackup *backupStat) { hold(participantBackup, "b1") },
			want:  busyWith("backup", "b1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			participant := &Handler{backupper: &backupper{}, restorer: &restorer{}}
			probe := NewNodeActivityProbe(participant)
			tt.setUp(&participant.backupper.lastOp)

			assert.Equal(t, tt.want, probe.Activity())
		})
	}
}
