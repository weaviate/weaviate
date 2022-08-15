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

package snapshots

import (
	"encoding/json"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Status string

const (
	StatusStarted  Status = "started"
	StatusCreated  Status = "created"
	StatusReleased Status = "released"
)

type State struct {
	SnapshotID string
	InProgress bool
}

type ShardMetadata struct {
	DocIDCounter      []byte `json:"docIdCounter"`
	PropLengthTracker []byte `json:"propLengthTracker"`
	ShardVersion      []byte `json:"shardVersion"`
}

type Snapshot struct {
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`

	ID            string                    `json:"id"`        // User created snapshot id
	ClassName     string                    `json:"className"` // DB class name, also selected by user
	Status        Status                    `json:"status"`    // "STARTED|RUNNING|FINISHED|FAILED"
	Files         []string                  `json:"files"`     // Relative paths to files in the snapshot
	ShardMetadata map[string]*ShardMetadata `json:"shardMetadata"`
	ShardingState []byte                    `json:"shardingState"`
	Schema        []byte                    `json:"schema"`
	ServerVersion string                    `json:"serverVersion"`

	// so shard-level snapshotting can be safely parallelized
	sync.Mutex `json:"-"`
}

func New(className, id string, startedAt time.Time) *Snapshot {
	return &Snapshot{
		ClassName:     className,
		ID:            id,
		StartedAt:     startedAt,
		ShardMetadata: make(map[string]*ShardMetadata),
	}
}

func (snap *Snapshot) WriteToDisk(basePath string) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return errors.Wrap(err, "write snapshot to disk")
	}

	snapPath := BuildSnapshotPath(basePath, snap.ClassName, snap.ID)

	// ensure that the snapshot directory exists
	if err := os.MkdirAll(path.Dir(snapPath), os.ModePerm); err != nil {
		return errors.Wrap(err, "write snapshot to disk")
	}

	if err := os.WriteFile(snapPath, b, os.ModePerm); err != nil {
		return errors.Wrap(err, "write snapshot to disk")
	}

	return nil
}

func (snap *Snapshot) RemoveFromDisk(basePath string) error {
	snapPath := BuildSnapshotPath(basePath, snap.ClassName, snap.ID)

	if err := os.Remove(snapPath); err != nil {
		return errors.Wrapf(err,
			"failed to remove snapshot from disk, at %s", snapPath)
	}

	return nil
}

func ReadFromDisk(basePath, className, id string) (*Snapshot, error) {
	snapPath := BuildSnapshotPath(basePath, className, id)

	contents, err := os.ReadFile(snapPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read snapshot from disk")
	}

	var snap Snapshot
	if err := json.Unmarshal(contents, &snap); err != nil {
		return nil, errors.Wrap(err,
			"failed to unmarshal snapshot disk contents")
	}

	return &snap, nil
}

func BuildSnapshotPath(basePath, className, id string) string {
	return path.Join(basePath, "snapshots", className, id) + ".json"
}
