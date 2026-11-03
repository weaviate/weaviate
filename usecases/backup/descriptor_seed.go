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
	"time"

	"github.com/weaviate/weaviate/entities/backup"
)

// descriptorSeed holds the global descriptor fields that the 2PC coordinator
// and the DTM task provider both set. Every global descriptor is built through
// descriptor, so a field added here reaches both paths.
type descriptorSeed struct {
	ID                     string
	Nodes                  map[string]*backup.NodeDescriptor
	ServerVersion          string
	Leader                 string
	BaseBackupID           string
	Users                  []string
	Roles                  []string
	SkipUsers              bool
	SkipRoles              bool
	CompressionType        backup.CompressionType
	BuiltInRoleAssignments []byte
}

func (s descriptorSeed) descriptor(startedAt time.Time, status backup.Status) *backup.DistributedBackupDescriptor {
	return &backup.DistributedBackupDescriptor{
		StartedAt:              startedAt,
		Status:                 status,
		ID:                     s.ID,
		Nodes:                  s.Nodes,
		ServerVersion:          s.ServerVersion,
		Leader:                 s.Leader,
		BaseBackupID:           s.BaseBackupID,
		Users:                  s.Users,
		Roles:                  s.Roles,
		SkipUsers:              s.SkipUsers,
		SkipRoles:              s.SkipRoles,
		CompressionType:        s.CompressionType,
		BuiltInRoleAssignments: s.BuiltInRoleAssignments,
	}
}
