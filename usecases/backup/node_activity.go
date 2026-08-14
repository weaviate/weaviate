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
