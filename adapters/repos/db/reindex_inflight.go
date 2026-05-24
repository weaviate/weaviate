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

package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
)

// ErrBackupBlockedByInFlightReindex is returned when a backup attempt
// races a runtime-reindex on the same shard. The DTM unit driving the
// migration is not part of the backup payload, so a captured tracker
// dir cannot be safely restored.
var ErrBackupBlockedByInFlightReindex = errors.New("backup blocked: runtime-reindex in flight on this shard")

// InFlightReindexTracker describes a migration tracker dir that is
// between markStarted and markTidied.
type InFlightReindexTracker struct {
	DirName    string // bare `.migrations/<...>` entry
	Prefix     string // strategy prefix without `_<gen>` suffix
	Generation int    // per-node `_<N>` suffix, >= 1

	Started   bool
	Reindexed bool
	Tidied    bool
}

// String produces a one-line summary suitable for log fields / error
// messages: "<dir> [started=true reindexed=false tidied=false]".
func (t InFlightReindexTracker) String() string {
	return fmt.Sprintf("%s [started=%v reindexed=%v tidied=%v]",
		t.DirName, t.Started, t.Reindexed, t.Tidied)
}

// inFlightReindexTrackers returns trackers with started.mig present and
// neither tidied.mig nor merged.mig. merged.mig counts as complete
// because FinalizeCompletedMigrations promotes it on restart. Sorted by
// DirName. Missing migrations dir returns (nil, nil).
func inFlightReindexTrackers(lsmPath string) ([]InFlightReindexTracker, error) {
	if lsmPath == "" {
		return nil, nil
	}
	migsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read migrations dir %q: %w", migsDir, err)
	}

	out := make([]InFlightReindexTracker, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		prefix, gen, ok := reindex.ParseMigrationDirName(name)
		if !ok {
			continue
		}
		dirPath := filepath.Join(migsDir, name)
		started := reindex.FileExistsInDir(dirPath, "started.mig")
		if !started {
			continue
		}
		tidied := reindex.FileExistsInDir(dirPath, "tidied.mig")
		if tidied {
			continue
		}
		if reindex.FileExistsInDir(dirPath, "merged.mig") {
			continue
		}
		out = append(out, InFlightReindexTracker{
			DirName:    name,
			Prefix:     prefix,
			Generation: gen,
			Started:    true,
			Reindexed:  reindex.FileExistsInDir(dirPath, "reindexed.mig"),
			Tidied:     false,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].DirName < out[j].DirName })
	return out, nil
}

// refuseIfReindexInFlight is the inactive-shard counterpart to
// [Shard.HaltForTransfer]'s in-flight check.
func (i *Index) refuseIfReindexInFlight(shardName string) error {
	lsmPath := shardPathLSM(i.path(), shardName)
	trackers, err := inFlightReindexTrackers(lsmPath)
	if err != nil {
		return fmt.Errorf("check in-flight reindex state for shard %q: %w", shardName, err)
	}
	return reindexInFlightError(shardName, trackers)
}

// reindexInFlightError wraps [ErrBackupBlockedByInFlightReindex] with
// a human-readable tracker list. Returns nil when trackers is empty.
func reindexInFlightError(shardName string, trackers []InFlightReindexTracker) error {
	if len(trackers) == 0 {
		return nil
	}
	parts := make([]string, len(trackers))
	for i, t := range trackers {
		parts[i] = t.String()
	}
	return fmt.Errorf(
		"%w: shard %q has %d active tracker(s): %s; retry after the migration finishes (poll GET /v1/schema/<class> until indexes are 'ready') or cancel it via PUT /v1/schema/<class>/indexes/<prop> {\"<indexType>\":{\"cancel\":true}}",
		ErrBackupBlockedByInFlightReindex,
		shardName,
		len(trackers),
		strings.Join(parts, ", "),
	)
}
