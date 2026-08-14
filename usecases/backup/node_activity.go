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
	"fmt"
	"sync"

	"github.com/weaviate/weaviate/entities/clusterprobe"
)

const (
	NodeActivityKindBackup  = "backup"
	NodeActivityKindRestore = "restore"

	// Equal to clusterprobe's log cap, so every id this build accepts renders whole.
	maxNodeActivityIDLen = 128
)

type NodeActivity struct {
	Busy bool
	Kind string
	ID   string
}

type NodeActivityResponse struct {
	Probe string `json:"probe"`
	// A pointer, so an answer that never mentions it is refused, not read as false.
	Busy *bool  `json:"busy"`
	Kind string `json:"kind,omitempty"`
	ID   string `json:"id,omitempty"`
}

func NewNodeActivityResponse(activity NodeActivity) NodeActivityResponse {
	return NodeActivityResponse{
		Probe: clusterprobe.BackupNodeActivityMarker,
		Busy:  &activity.Busy,
		Kind:  activity.Kind,
		ID:    activity.ID,
	}
}

// Activity reports what an answer says; a refusal means "cannot tell", never "node free".
func (r NodeActivityResponse) Activity() (NodeActivity, error) {
	if r.Probe != clusterprobe.BackupNodeActivityMarker {
		return NodeActivity{}, fmt.Errorf("answer is marked %s, want %q: it was not written by the "+
			"node-activity route, so it cannot mean the node is free; check for an HTTP proxy or "+
			"another service on the cluster port", clusterprobe.Loggable(r.Probe),
			clusterprobe.BackupNodeActivityMarker)
	}
	if r.Busy == nil {
		return NodeActivity{}, fmt.Errorf("answer has no %q field, so it cannot mean the node is free", "busy")
	}
	if *r.Busy && r.Kind != NodeActivityKindBackup && r.Kind != NodeActivityKindRestore {
		return NodeActivity{}, fmt.Errorf("answer is busy with kind %s, want %q or %q",
			clusterprobe.Loggable(r.Kind), NodeActivityKindBackup, NodeActivityKindRestore)
	}
	if *r.Busy && r.ID == "" {
		return NodeActivity{}, fmt.Errorf("answer is busy with kind %s but names no operation id",
			clusterprobe.Loggable(r.Kind))
	}
	if len(r.ID) > maxNodeActivityIDLen {
		return NodeActivity{}, fmt.Errorf("answer names an operation id of %d bytes, over the %d "+
			"this build accepts", len(r.ID), maxNodeActivityIDLen)
	}
	if !*r.Busy && (r.Kind != "" || r.ID != "") {
		return NodeActivity{}, fmt.Errorf("answer is not busy but names kind %s and id %s",
			clusterprobe.Loggable(r.Kind), clusterprobe.Loggable(r.ID))
	}
	return NodeActivity{Busy: *r.Busy, Kind: r.Kind, ID: r.ID}, nil
}

type NodeActivityProbe struct {
	participant *Handler

	mu        sync.RWMutex
	scheduler *Scheduler
}

func NewNodeActivityProbe(participant *Handler) *NodeActivityProbe {
	return &NodeActivityProbe{participant: participant}
}

// AttachScheduler adds the coordinator slots. It must run before the Scheduler
// can hold one, i.e. before the Scheduler is reachable by any request: a
// Scheduler that exists but is not attached makes this node report itself idle
// while it coordinates a backup.
func (p *NodeActivityProbe) AttachScheduler(scheduler *Scheduler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scheduler = scheduler
}

type activitySlot struct {
	stat *backupStat
	kind string
}

// Activity reads the four slots under four separate locks and is stale by the
// time it returns, so it is a backstop against a backup already running and
// never a reservation against one about to start.
func (p *NodeActivityProbe) Activity() NodeActivity {
	p.mu.RLock()
	scheduler := p.scheduler
	p.mu.RUnlock()

	slots := make([]activitySlot, 0, 4)
	if scheduler != nil {
		slots = append(slots,
			activitySlot{&scheduler.backupper.lastOp, NodeActivityKindBackup},
			activitySlot{&scheduler.restorer.lastOp, NodeActivityKindRestore})
	}
	slots = append(slots,
		activitySlot{&p.participant.backupper.lastOp, NodeActivityKindBackup},
		activitySlot{&p.participant.restorer.lastOp, NodeActivityKindRestore})

	for _, slot := range slots {
		// renew writes the id and reset clears it, so a non-empty id is exactly
		// "held". The status is no substitute: set and setFailed also write it to a
		// slot that was already released, and this node would then never idle again.
		if id := slot.stat.get().ID; id != "" {
			return NodeActivity{Busy: true, Kind: slot.kind, ID: id}
		}
	}
	return NodeActivity{}
}
