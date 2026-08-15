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

// NodeActivity is what one node said about itself. The zero value is "nobody
// told us anything", which is why Free, and never the absence of Busy, is what
// clears a node: a caller that drops the error still cannot read a refusal as
// an idle node.
type NodeActivity struct {
	Answered bool
	Busy     bool
	Kind     string
	ID       string
}

// Free reports the one verdict that lets a node past a gate on backup activity.
func (a NodeActivity) Free() bool {
	return a.Answered && !a.Busy
}

type NodeActivityResponse struct {
	Probe string `json:"probe"`
	// Node names the writer. Without it an answer from a host that a reassigned
	// member address routed us to is indistinguishable from the node we asked
	// about, and clears that node while it backs up.
	Node string `json:"node"`
	// A pointer, so an answer that never mentions it is refused, not read as false.
	Busy *bool  `json:"busy"`
	Kind string `json:"kind,omitempty"`
	ID   string `json:"id,omitempty"`
}

func NewNodeActivityResponse(node string, activity NodeActivity) NodeActivityResponse {
	// An activity this build could not decide leaves as busy: on this route
	// "unsure" must never travel as "free".
	busy := !activity.Free()
	return NodeActivityResponse{
		Probe: clusterprobe.BackupNodeActivityMarker,
		Node:  node,
		Busy:  &busy,
		Kind:  activity.Kind,
		ID:    activity.ID,
	}
}

// Activity reports what the node named addressed said; a refusal means "cannot
// tell", never "node free".
func (r NodeActivityResponse) Activity(addressed string) (NodeActivity, error) {
	if r.Probe != clusterprobe.BackupNodeActivityMarker {
		return NodeActivity{}, fmt.Errorf("answer is marked %s, want %q: it was not written by the "+
			"node-activity route, so it cannot mean the node is free; check for an HTTP proxy or "+
			"another service on the cluster port", clusterprobe.Loggable(r.Probe),
			clusterprobe.BackupNodeActivityMarker)
	}
	if r.Node != addressed {
		return NodeActivity{}, fmt.Errorf("answer is written by node %s, not by %s which we "+
			"addressed: a node can only speak for itself, so this cannot mean the node is free; "+
			"check for a member address that has been reassigned",
			clusterprobe.Loggable(r.Node), clusterprobe.Loggable(addressed))
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
	return NodeActivity{Answered: true, Busy: *r.Busy, Kind: r.Kind, ID: r.ID}, nil
}

type NodeActivityProbe struct {
	participant *Handler

	mu        sync.RWMutex
	scheduler *Scheduler
}

// NewNodeActivityProbe refuses a nil participant for the same reason
// [NewScheduler] refuses a nil probe: every answer reads the participant slots,
// so a wiring mistake would otherwise surface as a panic inside the handler
// answering a peer instead of stopping this node at startup.
func NewNodeActivityProbe(participant *Handler) *NodeActivityProbe {
	if participant == nil {
		panic("backup: NewNodeActivityProbe needs a participant")
	}
	return &NodeActivityProbe{participant: participant}
}

// attachScheduler adds the coordinator slots. A Scheduler that exists but is not
// attached makes this node report itself idle while it coordinates a backup,
// which is the one answer a caller gating on the probe cannot survive. What
// rules that out is [NewScheduler]: it demands a probe and attaches here. Being
// unexported only stops another package attaching one out of band.
func (p *NodeActivityProbe) attachScheduler(scheduler *Scheduler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scheduler = scheduler
}

type activitySlot struct {
	stat *backupStat
	kind string
}

// Node names the node this probe answers for.
func (p *NodeActivityProbe) Node() string {
	return p.participant.node
}

// Activity reads each slot under its own lock and is stale by the time it
// returns, so it is a backstop against a backup already running and never a
// reservation against one about to start.
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
			return NodeActivity{Answered: true, Busy: true, Kind: slot.kind, ID: id}
		}
	}
	return NodeActivity{Answered: true}
}
