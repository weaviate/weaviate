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
)

// ErrBackupBlockedByInFlightReindex is the sentinel returned when a
// backup attempt races a runtime-reindex on the same shard
// (0-weaviate-issues#215). Concurrent backup + reindex hits a WAL
// close race (~30% failures) AND a hardlink ENOENT race (~10%); even
// when both miss, the captured state mixes pre-swap registrations
// with post-swap sentinels and isn't reachable on restore (the DTM
// unit isn't in the payload, so the half-migration never finishes).
// Until the architectural fix lands (pause iteration during halt or
// capture DTM state in the payload), the safe move is refusal.
var ErrBackupBlockedByInFlightReindex = errors.New("backup blocked: runtime-reindex in flight on this shard")

// InFlightReindexTracker carries the fields the structured error
// builder needs to name the blocking migration. One per directory
// between [markStarted] and [markTidied]. Pure value type.
type InFlightReindexTracker struct {
	DirName    string // bare `.migrations/<...>` entry
	Prefix     string // strategy prefix without `_<gen>` suffix
	Generation int    // per-node `_<N>` suffix, >= 1

	// Sentinel snapshot at scan time. Started==true and Tidied==false
	// for every returned entry (the scanner filters on both).
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

// inFlightReindexTrackers returns trackers with `started.mig` present
// and neither `tidied.mig` nor `merged.mig`. `merged.mig` counts as
// complete: [FinalizeCompletedMigrations] promotes merged-but-not-
// tidied on restart, so capturing one in a backup is safe. Trackers
// without started.mig are pre-iteration scratch — skipped.
// Sorted by DirName for stable error messages. Missing migrations
// dir → (nil, nil).
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
		prefix, gen, ok := parseMigrationDirName(name)
		if !ok {
			// Legacy pre-generation dir or operator surgery; defensive skip.
			continue
		}
		dirPath := filepath.Join(migsDir, name)
		started := fileExistsInDir(dirPath, "started.mig")
		if !started {
			continue
		}
		tidied := fileExistsInDir(dirPath, "tidied.mig")
		if tidied {
			continue
		}
		// `merged.mig` ≡ complete: FinalizeCompletedMigrations on the
		// restored cluster promotes it cleanly on next startup.
		if fileExistsInDir(dirPath, "merged.mig") {
			continue
		}
		out = append(out, InFlightReindexTracker{
			DirName:    name,
			Prefix:     prefix,
			Generation: gen,
			Started:    true,
			Reindexed:  fileExistsInDir(dirPath, "reindexed.mig"),
			Tidied:     false,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].DirName < out[j].DirName })
	return out, nil
}

// refuseIfReindexInFlight is the inactive-shard counterpart to
// [Shard.HaltForTransfer]'s in-flight check: a COLD/INACTIVE shard
// deactivated mid-migration can't slip into the backup payload
// either — the DTM unit driving it isn't captured, so the restored
// cluster would inherit the orphan tracker + sidecar bucket.
func (i *Index) refuseIfReindexInFlight(shardName string) error {
	lsmPath := shardPathLSM(i.path(), shardName)
	trackers, err := inFlightReindexTrackers(lsmPath)
	if err != nil {
		return fmt.Errorf("check in-flight reindex state for shard %q: %w", shardName, err)
	}
	return reindexInFlightError(shardName, trackers)
}

// reindexInFlightError wraps [ErrBackupBlockedByInFlightReindex] with
// a human-readable tracker list. errors.Is on the sentinel is
// preserved so REST handlers map to a structured HTTP status without
// string matching. Returns nil when trackers is empty — callers can
// use it as a one-shot gate.
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
