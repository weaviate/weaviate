//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backup

import (
	"path"
	"sync"
	"time"
)

type Status string

type State struct {
	SnapshotID string
	InProgress bool
}

type ShardMetadata struct {
	DocIDCounter      []byte `json:"docIdCounter"`
	PropLengthTracker []byte `json:"propLengthTracker"`
	ShardVersion      []byte `json:"shardVersion"`
}

type SnapshotFile struct {
	Class string `json:"class"` // Name of class to which the file belongs
	Node  string `json:"node"`  // Name of node to which the file belongs
	Shard string `json:"shard"` // Name of shard to which the file belongs
	Path  string `json:"path"`  // Relative paths to files in the snapshot
}

type Snapshot struct {
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`

	ID            string                    `json:"id"`        // User created snapshot id
	ClassName     string                    `json:"className"` // DB class name, also selected by user
	Status        string                    `json:"status"`    // "STARTED|TRANSFERRING|TRANSFERRED|SUCCESS|FAILED"
	Files         []SnapshotFile            `json:"files"`
	ShardMetadata map[string]*ShardMetadata `json:"shardMetadata"`
	ShardingState []byte                    `json:"shardingState"`
	Schema        []byte                    `json:"schema"`
	ServerVersion string                    `json:"serverVersion"`
	Error         string                    `json:"error"`

	// so shard-level snapshotting can be safely parallelized
	sync.Mutex `json:"-"`
}

func NewSnapshot(className, id string, startedAt time.Time) *Snapshot {
	return &Snapshot{
		ClassName:     className,
		ID:            id,
		StartedAt:     startedAt,
		ShardMetadata: make(map[string]*ShardMetadata),
	}
}

func BuildSnapshotPath(basePath, className, id string) string {
	return path.Join(basePath, "snapshots", className, id) + ".json"
}
