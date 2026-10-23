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
	"encoding/json"
	"fmt"
	"slices"

	"github.com/weaviate/weaviate/entities/backup"
)

const BackupTaskNamespace = "backup"

// taskPayload is the DTM task record payload. Concurrent writers derive all
// fields from this payload, never from the local binary's runtime state. The
// plan fields of a dedupe artifact (Version and Dedupe*) come from the
// published global descriptor, not from this payload.
type taskPayload struct {
	ID                              string                            `json:"id"`
	Backend                         string                            `json:"backend"`
	Nodes                           map[string]*backup.NodeDescriptor `json:"nodes"`  // frozen at propose time via groupByShard
	Leader                          string                            `json:"leader"` // always a participant carrying the full class list
	Classes                         []string                          `json:"classes"`
	Users                           []string                          `json:"users,omitempty"`          // nil = whole-cluster user snapshot
	Roles                           []string                          `json:"roles,omitempty"`          // nil = whole-cluster RBAC snapshot
	RoleNamespaces                  []string                          `json:"roleNamespaces,omitempty"` // namespaces includeRoles names; idempotency key only, nodes never upload it
	SkipUsers                       bool                              `json:"skipUsers,omitempty"`
	SkipRoles                       bool                              `json:"skipRoles,omitempty"`
	Compression                     Compression                       `json:"compression"`
	Bucket                          string                            `json:"bucket,omitempty"`       // empty = env default
	Path                            string                            `json:"path,omitempty"`         // empty = env default
	BaseBackupID                    string                            `json:"baseBackupId,omitempty"` // empty = full backup
	ServerVersion                   string                            `json:"serverVersion"`          // proposer's build version for byte-stable descriptors
	CompressionType                 backup.CompressionType            `json:"compressionType"`        // derived from Compression.Level at propose time
	BaseChainDeduped                bool                              `json:"baseChainDeduped,omitempty"`
	DedupeReplicas                  bool                              `json:"dedupeReplicas,omitempty"`
	DedupeConvergenceTimeoutSeconds int                               `json:"dedupeConvergenceTimeoutSeconds,omitempty"` // 0 = default budget
	BuiltInRoleAssignments          []byte                            `json:"builtInRoleAssignments,omitempty"`          // global descriptor only; nodes never upload it
}

func (p *taskPayload) artifactVersion() string {
	if p.BaseChainDeduped {
		return VersionDedupeReplicas
	}
	return Version
}

func (p *taskPayload) descriptorSeed() descriptorSeed {
	return descriptorSeed{
		ID:                     p.ID,
		Nodes:                  p.Nodes,
		ServerVersion:          p.ServerVersion,
		Leader:                 p.Leader,
		BaseBackupID:           p.BaseBackupID,
		Users:                  p.Users,
		Roles:                  p.Roles,
		SkipUsers:              p.SkipUsers,
		SkipRoles:              p.SkipRoles,
		CompressionType:        p.CompressionType,
		BuiltInRoleAssignments: p.BuiltInRoleAssignments,
	}
}

// sameRequestConfig reports whether p and other carry the same client request.
// Request-determined fields define the idempotency key. Derived fields
// (Leader, Nodes, ServerVersion, BuiltInRoleAssignments, CompressionType,
// BaseChainDeduped) are recomputed on retry and may legitimately differ.
func (p *taskPayload) sameRequestConfig(other *taskPayload) bool {
	return p.Backend == other.Backend &&
		sameStringSet(p.Classes, other.Classes) &&
		sameStringSet(p.Users, other.Users) &&
		sameStringSet(p.Roles, other.Roles) &&
		sameStringSet(p.RoleNamespaces, other.RoleNamespaces) &&
		p.SkipUsers == other.SkipUsers &&
		p.SkipRoles == other.SkipRoles &&
		p.Compression == other.Compression &&
		p.Bucket == other.Bucket &&
		p.Path == other.Path &&
		p.BaseBackupID == other.BaseBackupID &&
		p.DedupeReplicas == other.DedupeReplicas &&
		p.DedupeConvergenceTimeoutSeconds == other.DedupeConvergenceTimeoutSeconds
}

// sameStringSet compares order-insensitively: Classes, Users and Roles come
// from server-side selection (ListClasses, lister expansion), whose order is
// not stable across nodes, and RoleNamespaces follows the selector order the
// client sent.
func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func marshalTaskPayload(p *taskPayload) ([]byte, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal backup task payload: %w", err)
	}
	return b, nil
}

func unmarshalTaskPayload(data []byte) (*taskPayload, error) {
	var p taskPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal backup task payload: %w", err)
	}
	return &p, nil
}
