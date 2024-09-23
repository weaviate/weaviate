//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

// events implement memberlist.EventDelegate interface
// EventDelegate is a simpler delegate that is used only to receive
// notifications about members joining and leaving. The methods in this
// delegate may be called by multiple goroutines, but never concurrently.
// This allows you to reason about ordering.
type events struct {
	delegate *delegate
	raft     *raft.Raft
	localID  string
	voter    bool
	logger   logrus.FieldLogger
}

func (e *events) SetRaft(raft *raft.Raft) {
	e.raft = raft
}

func (e *events) SetVoter(voter bool) {
	e.voter = voter
}

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (e *events) NotifyJoin(*memberlist.Node) {}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (e *events) NotifyLeave(node *memberlist.Node) {
	e.delegate.delete(node.Name)
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (e *events) NotifyUpdate(*memberlist.Node) {}
