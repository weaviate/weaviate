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

package sharding

import (
	"maps"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

const (
	TransitionFreeze   = "freeze"
	TransitionUnfreeze = "unfreeze"
)

const (
	ProgressStarted = "started"
	ProgressDone    = "done"
)

// Transition is a running freeze or unfreeze. Physical.Status stays where it
// started until the transition ends.
type Transition struct {
	Kind   string `json:"kind"`
	Target string `json:"target"`
	// Attempt is the RAFT index that started the transition.
	Attempt      uint64                           `json:"attempt"`
	Participants map[string]TransitionParticipant `json:"participants"`
	// Chosen is the unfreeze's new BelongsToNodes, applied at completion.
	Chosen []string `json:"chosen,omitempty"`
}

type TransitionParticipant struct {
	// Source is the node whose cloud copy an unfreeze participant downloads.
	Source   string `json:"source,omitempty"`
	Progress string `json:"progress"`
	HasData  bool   `json:"hasData,omitempty"`
}

func (t *Transition) DeepCopy() *Transition {
	if t == nil {
		return nil
	}
	cp := *t
	cp.Participants = maps.Clone(t.Participants)
	cp.Chosen = slices.Clone(t.Chosen)
	return &cp
}

func (t *Transition) AllDone() bool {
	for _, p := range t.Participants {
		if p.Progress != ProgressDone {
			return false
		}
	}
	return true
}

// DataNodes are the participants that reported data, sorted.
func (t *Transition) DataNodes() []string {
	var nodes []string
	for _, node := range slices.Sorted(maps.Keys(t.Participants)) {
		if t.Participants[node].HasData {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// RoutingNodes are the nodes a request should reach. An unfreeze routes to its
// chosen nodes from the start: the old nodes dropped the shard at freeze.
func (p *Physical) RoutingNodes() []string {
	if p.Transition != nil && p.Transition.Kind == TransitionUnfreeze {
		return p.Transition.Chosen
	}
	return p.BelongsToNodes
}

func (t *Transition) activityStatus() string {
	if t.Kind == TransitionFreeze {
		return models.TenantActivityStatusFREEZING
	}
	return models.TenantActivityStatusUNFREEZING
}

// Model is the transition as the tenants API reports it.
func (t *Transition) Model() *models.TenantTransition {
	if t == nil {
		return nil
	}
	participants := make([]*models.TenantTransitionParticipant, 0, len(t.Participants))
	for _, node := range slices.Sorted(maps.Keys(t.Participants)) {
		participants = append(participants, &models.TenantTransitionParticipant{
			Node:       node,
			SourceNode: t.Participants[node].Source,
			Progress:   strings.ToUpper(t.Participants[node].Progress),
		})
	}
	return &models.TenantTransition{
		Kind:         strings.ToUpper(t.Kind),
		TargetStatus: t.Target,
		Participants: participants,
	}
}
