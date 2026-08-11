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

// Kinds reported by [NodeActivity].
const (
	NodeActivityKindBackup  = "backup"
	NodeActivityKindRestore = "restore"
)

// NodeActivity reports the backup or restore this node is currently part of.
type NodeActivity struct {
	Busy bool
	Kind string
	ID   string
}

// NodeActivityResponse is the wire form of [NodeActivity], carried by the
// cluster-internal probe. Busy is a pointer so a payload that never mentions
// it is rejected rather than read as "not busy"; see
// [clusterprobe.BackupNodeActivityMarker].
type NodeActivityResponse struct {
	Probe string `json:"probe"`
	Busy  *bool  `json:"busy"`
	Kind  string `json:"kind,omitempty"`
	ID    string `json:"id,omitempty"`
}

// NewNodeActivityResponse renders activity for the wire.
func NewNodeActivityResponse(activity NodeActivity) NodeActivityResponse {
	return NodeActivityResponse{
		Probe: clusterprobe.BackupNodeActivityMarker,
		Busy:  &activity.Busy,
		Kind:  activity.Kind,
		ID:    activity.ID,
	}
}

// Activity returns the activity a decoded response carries, or an error if the
// payload does not identify itself as a node's own answer.
func (r NodeActivityResponse) Activity() (NodeActivity, error) {
	// Probe, Kind and ID are peer-controlled, so they go through
	// [clusterprobe.Loggable] like every other probe byte that can end up in a log.
	if r.Probe != clusterprobe.BackupNodeActivityMarker {
		return NodeActivity{}, fmt.Errorf("answer is marked %s, want %q: this 200 did not come "+
			"from a Weaviate node, so it cannot mean the node is free; check for an HTTP proxy "+
			"on the cluster port", clusterprobe.Loggable(r.Probe), clusterprobe.BackupNodeActivityMarker)
	}
	if r.Busy == nil {
		return NodeActivity{}, fmt.Errorf("answer has no %q field, so it cannot mean the node is free", "busy")
	}
	// A busy answer's kind and ID are formatted verbatim into the 409 a caller
	// sees, so only the kinds this package emits are accepted.
	if *r.Busy && r.Kind != NodeActivityKindBackup && r.Kind != NodeActivityKindRestore {
		return NodeActivity{}, fmt.Errorf("answer is busy with kind %s, want %q or %q",
			clusterprobe.Loggable(r.Kind), NodeActivityKindBackup, NodeActivityKindRestore)
	}
	// A held slot always carries its ID, so a busy answer without one did not
	// come from this package and would put an empty backup id in that 409.
	if *r.Busy && r.ID == "" {
		return NodeActivity{}, fmt.Errorf("answer is busy with kind %s but names no id",
			clusterprobe.Loggable(r.Kind))
	}
	// Only a held slot has a kind or an ID, so an idle answer naming either did
	// not come from this package and its "not busy" cannot be trusted.
	if !*r.Busy && (r.Kind != "" || r.ID != "") {
		return NodeActivity{}, fmt.Errorf("answer is not busy but names kind %s and id %s",
			clusterprobe.Loggable(r.Kind), clusterprobe.Loggable(r.ID))
	}
	return NodeActivity{Busy: *r.Busy, Kind: r.Kind, ID: r.ID}, nil
}

// NodeActivityProbe answers whether this node is part of a backup or restore
// right now. It keeps no backup state of its own: it reads slots the backup
// subsystem already manages, so nothing here can leak or go stale on a crash.
type NodeActivityProbe struct {
	participant *Handler

	mu        sync.RWMutex
	scheduler *Scheduler
}

func NewNodeActivityProbe(participant *Handler) *NodeActivityProbe {
	return &NodeActivityProbe{participant: participant}
}

// attachScheduler wires in the coordinator slots. [NewScheduler] is the only
// caller, so a Scheduler cannot exist that this probe does not see. Probes may
// arrive before it: answering from participant slots alone is still correct
// then, since a Scheduler-less node can't be coordinating anything.
func (p *NodeActivityProbe) attachScheduler(s *Scheduler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scheduler = s
}

// Activity reports the first held slot, in fixed order (coordinator backup,
// restore, then participant backup, restore) so repeated probes agree.
//
// The slots are read one after another, not as one snapshot: a node handing an
// operation from one slot to another between two reads can answer "not busy"
// without having been idle at any single instant.
func (p *NodeActivityProbe) Activity() NodeActivity {
	p.mu.RLock()
	scheduler := p.scheduler
	p.mu.RUnlock()

	type slot struct {
		stat *backupStat
		kind string
	}
	slots := make([]slot, 0, 4)
	if scheduler != nil {
		if scheduler.backupper != nil {
			slots = append(slots, slot{&scheduler.backupper.lastOp, NodeActivityKindBackup})
		}
		if scheduler.restorer != nil {
			slots = append(slots, slot{&scheduler.restorer.lastOp, NodeActivityKindRestore})
		}
	}
	if p.participant != nil {
		if p.participant.backupper != nil {
			slots = append(slots, slot{&p.participant.backupper.lastOp, NodeActivityKindBackup})
		}
		if p.participant.restorer != nil {
			slots = append(slots, slot{&p.participant.restorer.lastOp, NodeActivityKindRestore})
		}
	}

	for _, s := range slots {
		// renew() writes the ID when it takes the slot and reset() clears it when
		// the op ends, so a non-empty ID is exactly "held". Status can't stand in
		// since a live op sits in any of Transferring/Finalizing/Cancelling.
		if id := s.stat.get().ID; id != "" {
			return NodeActivity{Busy: true, Kind: s.kind, ID: id}
		}
	}
	return NodeActivity{}
}
