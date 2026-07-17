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
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/entities/diskio"
)

// rangeableIncompleteSentinelPrefix names the durable per-property markers that
// record "this shard's rangeable index for this property is known-incomplete".
// The marker lives beside the migration trackers in the shard's .migrations dir
// and persists a not-ready verdict across boots. It exists to close the
// partial-population hole in [Shard.reconcileRangeableReadinessAfterInit]: that
// gate seeds readiness=false only while the rangeable bucket is EMPTY, but a
// bucket the client has since partially repopulated (post-gate writes are
// schema+existence gated, not readiness gated) passes HasAnyData on the next
// boot and would be served as ready while its history is still missing. The
// durable marker makes the verdict outlive the fooled heuristic until an
// explicit rebuild clears it. See weaviate/0-weaviate-issues#335.
const rangeableIncompleteSentinelPrefix = "rangeable_incomplete_"

// rangeableIncompleteSentinelDir is the shard's migration dir, shared with the
// reindex trackers ([markInFlightRangeableMigrationsNotReady] scans it). The
// marker files are ignored there: that scan only processes migration-tracker
// directories, never files.
func (s *Shard) rangeableIncompleteSentinelDir() string {
	return filepath.Join(s.pathLSM(), ".migrations")
}

// rangeableIncompleteSentinelPath returns the marker path for a property.
// Property names are regex-limited to [_A-Za-z][_0-9A-Za-z]* by schema
// validation (entities/schema/validation.go PropertyNameRegex), so propName is
// always a safe single filename component — no sanitization needed.
func (s *Shard) rangeableIncompleteSentinelPath(propName string) string {
	return filepath.Join(s.rangeableIncompleteSentinelDir(),
		rangeableIncompleteSentinelPrefix+propName+".mig")
}

// rangeableIncompleteSentinelExists reports whether a durable not-ready verdict
// was persisted for propName on a prior boot.
func (s *Shard) rangeableIncompleteSentinelExists(propName string) bool {
	_, err := os.Stat(s.rangeableIncompleteSentinelPath(propName))
	return err == nil
}

// writeRangeableIncompleteSentinel durably persists the not-ready verdict for
// propName. The marker file's content is empty (its existence is the signal);
// the file and its parent dir are fsync'd so the verdict survives a power cut,
// matching the migration-sentinel durability discipline (weaviate/weaviate#12159).
func (s *Shard) writeRangeableIncompleteSentinel(propName string) error {
	dir := s.rangeableIncompleteSentinelDir()
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}
	f, err := os.OpenFile(s.rangeableIncompleteSentinelPath(propName),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// fsync the parent dir so the new directory entry itself is durable.
	return diskio.Fsync(dir)
}

// removeRangeableIncompleteSentinel clears the durable verdict for propName.
// Called after an explicit rebuild repopulates the rangeable index
// (FilterableToRangeableStrategy.OnMigrationComplete). Idempotent: a missing
// marker is not an error.
func (s *Shard) removeRangeableIncompleteSentinel(propName string) error {
	if err := os.Remove(s.rangeableIncompleteSentinelPath(propName)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	// fsync the parent dir so the removal is durable and the marker cannot
	// reappear after a crash.
	return diskio.Fsync(s.rangeableIncompleteSentinelDir())
}
