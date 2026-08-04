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

import "sync"

// Kinds reported by [NodeActivity].
const (
	NodeActivityKindBackup  = "backup"
	NodeActivityKindRestore = "restore"
)

// NodeActivity reports the backup or restore this node is currently part of.
// It doubles as the JSON payload of the cluster-internal probe.
type NodeActivity struct {
	Busy bool   `json:"busy"`
	Kind string `json:"kind,omitempty"`
	ID   string `json:"id,omitempty"`
}

// NodeActivityProbe answers whether this node is part of a backup or restore
// right now. It owns no state: it reads slots the backup subsystem already
// manages, so nothing here can leak or go stale on a crash.
type NodeActivityProbe struct {
	participant *Handler

	mu        sync.RWMutex
	scheduler *Scheduler
}

func NewNodeActivityProbe(participant *Handler) *NodeActivityProbe {
	return &NodeActivityProbe{participant: participant}
}

// AttachScheduler wires in the coordinator slots once the Scheduler exists.
// Probes may arrive first; answering from participant slots alone is still
// correct then, since a Scheduler-less node can't be coordinating anything.
func (p *NodeActivityProbe) AttachScheduler(s *Scheduler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scheduler = s
}

// Activity reports the first held slot, in fixed order (coordinator backup,
// restore, then participant backup, restore) so repeated probes agree.
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
		// A non-empty ID is the one-to-one inverse of renew() having succeeded.
		// Status cannot stand in: reset() clears it, and a live op sits in any of
		// Transferring, Finalizing or Cancelling.
		if id := s.stat.get().ID; id != "" {
			return NodeActivity{Busy: true, Kind: s.kind, ID: id}
		}
	}
	return NodeActivity{}
}
