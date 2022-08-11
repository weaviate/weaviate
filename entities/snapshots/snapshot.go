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

	ID            string                    `json:"id"`
	ClassName     string                    `json:"className"`
	Status        Status                    `json:"status"`
	Files         []string                  `json:"files"`
	ShardMetadata map[string]*ShardMetadata `json:"shardMetadata"`
	ShardingState []byte                    `json:"shardingState"`
	Schema        []byte                    `json:"schema"`
	ServerVersion string                    `json:"serverVersion"`

	// so shard-level snapshotting can be safely parallelized
	sync.Mutex `json:"-"`
}

func New(id string, startedAt time.Time) *Snapshot {
	return &Snapshot{
		ID:            id,
		Status:        StatusStarted,
		StartedAt:     startedAt,
		ShardMetadata: make(map[string]*ShardMetadata),
	}
}

func (snap *Snapshot) WriteToDisk(basePath string) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return errors.Wrap(err, "write snapshot to disk")
	}

	snapPath := BuildSnapshotPath(snap.ID, basePath)

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
	snapPath := BuildSnapshotPath(snap.ID, basePath)

	if err := os.Remove(snapPath); err != nil {
		return errors.Wrapf(err,
			"failed to remove snapshot from disk, at %s", snapPath)
	}

	return nil
}

func ReadFromDisk(id, basePath string) (*Snapshot, error) {
	snapPath := BuildSnapshotPath(id, basePath)

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

func BuildSnapshotPath(id, basePath string) string {
	return path.Join(basePath, "snapshots", id) + ".json"
}
