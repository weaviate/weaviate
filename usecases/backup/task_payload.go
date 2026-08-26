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

	"github.com/weaviate/weaviate/entities/backup"
)

const BackupTaskNamespace = "backup"

// taskPayload is the DTM task record payload. Concurrent writers derive all
// fields from this payload, never from the local binary's runtime state.
type taskPayload struct {
	ID              string                            `json:"id"`
	Backend         string                            `json:"backend"`
	Nodes           map[string]*backup.NodeDescriptor `json:"nodes"`  // frozen at propose time via groupByShard
	Leader          string                            `json:"leader"` // always a participant carrying the full class list
	Classes         []string                          `json:"classes"`
	Users           []string                          `json:"users,omitempty"` // nil = whole-cluster user snapshot
	Roles           []string                          `json:"roles,omitempty"` // nil = whole-cluster RBAC snapshot
	Compression     Compression                       `json:"compression"`
	Bucket          string                            `json:"bucket,omitempty"`       // empty = env default
	Path            string                            `json:"path,omitempty"`         // empty = env default
	BaseBackupID    string                            `json:"baseBackupId,omitempty"` // empty = full backup
	ServerVersion   string                            `json:"serverVersion"`          // proposer's build version for byte-stable descriptors
	CompressionType backup.CompressionType            `json:"compressionType"`        // derived from Compression.Level at propose time
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
