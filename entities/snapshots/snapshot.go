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

type Snapshot struct {
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`

	ID            string                    `json:"id"`
	Status        Status                    `json:"status"`
	Files         []string                  `json:"files"`
	BasePath      string                    `json:"basePath"`
	ShardMetadata map[string]*ShardMetadata `json:"shardMetadata"`
	ShardingState []byte                    `json:"shardingState"`
	Schema        []byte                    `json:"schema"`
	ServerVersion string                    `json:"serverVersion"`

	// so shard-level snapshotting can be safely parallelized
	sync.Mutex `json:"-"`
}

func (snap *Snapshot) WriteToDisk() error {
	b, err := json.Marshal(snap)
	if err != nil {
		return errors.Wrap(err, "write snapshot to disk")
	}

	snapPath := path.Join(snap.BasePath, "snapshots")

	if err := os.MkdirAll(snapPath, os.ModePerm); err != nil {
		return errors.Wrap(err, "write snapshot to disk")
	}

	snapPath = path.Join(snapPath, snap.ID) + ".json"

	if err := os.WriteFile(snapPath, b, os.ModePerm); err != nil {
		return errors.Wrap(err, "write snapshot to disk")
	}

	return nil
}

func New(id string, startedAt time.Time, basePath string) *Snapshot {
	return &Snapshot{
		ID:            id,
		Status:        StatusStarted,
		StartedAt:     startedAt,
		BasePath:      basePath,
		ShardMetadata: make(map[string]*ShardMetadata),
	}
}

func ReadFromDisk(id, basePath string) (*Snapshot, error) {
	snapPath := path.Join(basePath, "snapshots", id) + ".json"

	contents, err := os.ReadFile(snapPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read snapshot from disk")
	}

	var snap Snapshot
	if err := json.Unmarshal(contents, &snap); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal snapshot disk contents")
	}

	return &snap, nil
}

type ShardMetadata struct {
	DocIDCounter      []byte `json:"docIdCounter"`
	PropLengthTracker []byte `json:"propLengthTracker"`
	ShardVersion      []byte `json:"shardVersion"`
}

type State struct {
	SnapshotID string
	InProgress bool
}

// type Backup struct {
// 	Events []BackupEvent
// }

// type BackupEvent struct {
// 	Time time.Time
// 	Msg  string
// }
