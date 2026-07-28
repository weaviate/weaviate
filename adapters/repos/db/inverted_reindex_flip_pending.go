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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
)

// pendingFlipFile holds a shard's flip-pending records. It sits directly
// under <lsm>/.migrations/ as a plain file: every scanner of that directory
// iterates sub-directories only, so the marker is inert to all of them, and
// the backup file lists pick it up along with the tracker sentinels.
const pendingFlipFile = "flip_pending.mig"

// PendingFlip records that an enable-* migration already promoted a
// property's canonical bucket while the cluster-wide schema flag is still
// false (weaviate/0-weaviate-issues#319). Nothing else survives a restart:
// disk alone can't tell that bucket apart from what a deleted index leaves
// behind, which [propertyDeleteIndexHelper] removes on sight.
//
// One property can hold one record per index type at the same time. The
// submit-time conflict rule ([ReindexProvider.CheckConflict]) rejects a
// second migration only while the first is still ACTIVE, so an
// enable-filterable that reached FAILED after swapping on this shard leaves
// its flag false and its record live, and an enable-searchable on the same
// property is then accepted and swaps alongside it. Everything keyed off a
// record therefore keys off (prop, indexType), never the property alone.
type PendingFlip struct {
	Prop string `json:"prop"`
	// IndexType is the canonical inverted-index discriminator,
	// "filterable" or "searchable".
	IndexType string `json:"indexType"`
	// Tokenization is the enable-searchable target. Empty means "use the
	// live schema's tokenization": the recovery payload it is read from is
	// best-effort and absent on some paths.
	Tokenization string `json:"tokenization,omitempty"`
}

// overlay is the analyzer override this record stands for.
func (f PendingFlip) overlay() inverted.PropertyOverlay {
	switch f.IndexType {
	case "filterable":
		return inverted.PropertyOverlay{ForceFilterable: true}
	case "searchable":
		return inverted.PropertyOverlay{ForceSearchable: true, Tokenization: f.Tokenization}
	}
	return inverted.PropertyOverlay{}
}

// satisfiedByLiveSchema reports whether the live property already has the flag
// (and tokenization) this record forces, i.e. the flip landed here and the
// record is obsolete.
//
// On the record rather than on an overlay: collapsing an overlay to a single
// boolean is only sound while it carries one forced flag, and a merged entry
// ([Shard.forceIndexOverlay]) can carry two that land at different times.
func (f PendingFlip) satisfiedByLiveSchema(prop *models.Property) bool {
	return forcesNoIndex(unsatisfiedForceIndexOverlay(f.overlay(), prop))
}

// pendingFlipKey identifies a record by the tuple it is unique on.
type pendingFlipKey struct {
	prop      string
	indexType string
}

func (f PendingFlip) key() pendingFlipKey {
	return pendingFlipKey{prop: f.Prop, indexType: f.IndexType}
}

// pendingFlipLookup answers "does (prop, indexType) have a
// swapped-but-not-flipped migration on this shard?".
type pendingFlipLookup map[pendingFlipKey]struct{}

// pendingFlipShield reports which buckets the nonexistent-property-index sweep
// must leave alone. Its zero value protects everything: a caller that has not
// run scanPendingFlips cannot present itself as having looked and found none,
// which is the only mistake here that costs data (weaviate/0-weaviate-issues#438).
type pendingFlipShield struct {
	scanned bool
	flips   pendingFlipLookup
}

// newPendingFlipShield records that the scan ran, so an empty result means
// "provably no pending flip" rather than "never asked".
func newPendingFlipShield(flips []PendingFlip) pendingFlipShield {
	out := make(pendingFlipLookup, len(flips))
	for _, f := range flips {
		out[f.key()] = struct{}{}
	}
	return pendingFlipShield{scanned: true, flips: out}
}

// protects reports whether the bucket for (propName, indexType) must be kept.
func (s pendingFlipShield) protects(propName, indexType string) bool {
	if !s.scanned {
		return true
	}
	_, ok := s.flips[pendingFlipKey{prop: propName, indexType: indexType}]
	return ok
}

// scanPendingFlips reports every swapped-but-not-flipped enable-* migration
// visible on disk: persisted records plus tracker dirs a finalize pass has
// not consumed yet. Read-only by construction, so it is safe to call before
// finalize creates .migrations — contrast [fileReindexTracker.init], which
// MkdirAlls and so cannot be used here.
//
// The second return value forwards [readPendingFlips]'s unreadable flag.
func scanPendingFlips(lsmPath string, logger logrus.FieldLogger) ([]PendingFlip, bool) {
	persisted, unreadable := readPendingFlips(lsmPath, logger)
	return mergePendingFlips(persisted, pendingFlipTrackers(lsmPath)), unreadable
}

// readPendingFlips returns the records persisted under lsmPath. Unreadable or
// malformed content yields nil records and unreadable=true: startup must not
// block on it, but it must not treat it as "no migration is pending" either.
//
// A missing file is the ordinary case and reports unreadable=false. The two
// states have to stay distinguishable at every call site: absent means there
// is provably no pending flip, present-but-unparseable means something was
// recorded and we cannot tell what.
func readPendingFlips(lsmPath string, logger logrus.FieldLogger) ([]PendingFlip, bool) {
	path := filepath.Join(lsmPath, ".migrations", pendingFlipFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false
		}
		if logger != nil {
			logger.WithField("path", path).
				Warnf("reindex: unable to read flip-pending records; a property mid-migration may lose its bucket: %v", err)
		}
		return nil, true
	}
	var flips []PendingFlip
	if err := json.Unmarshal(data, &flips); err != nil {
		if logger != nil {
			logger.WithField("path", path).
				Warnf("reindex: malformed flip-pending records; a property mid-migration may lose its bucket: %v", err)
		}
		return nil, true
	}
	return flips, false
}

// writePendingFlips replaces the persisted record set; an empty set removes
// the file. Write-temp-then-rename plus fsyncing both the temp file and the
// directory survives a power cut, not just a process crash — from the second
// restart on, this file is the only evidence that the canonical bucket must
// be kept, since [FinalizeCompletedMigrations] has already consumed the
// tracker dir it was derived from.
func writePendingFlips(lsmPath string, flips []PendingFlip) error {
	dir := filepath.Join(lsmPath, ".migrations")
	target := filepath.Join(dir, pendingFlipFile)
	if len(flips) == 0 {
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", target, err)
		}
		return syncDir(dir)
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	data, err := json.Marshal(flips)
	if err != nil {
		return fmt.Errorf("marshal flip-pending records: %w", err)
	}
	tmp := target + ".tmp"
	if err := writeFileDurable(tmp, data); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return fmt.Errorf("rename %s to %s: %w", tmp, target, err)
	}
	return syncDir(dir)
}

// writeFileDurable writes data to path and fsyncs it before returning, so the
// contents are on stable storage rather than only in the page cache.
func writeFileDurable(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

// syncDir fsyncs a directory so a rename or unlink inside it is durable. A
// missing directory is not an error: there is nothing left to persist.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open %s: %w", dir, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", dir, err)
	}
	return nil
}

// pendingFlipTrackers derives records from the enable-* tracker dirs that
// have completed their swap (tidied.mig / merged.mig) but not been finalized
// yet. Covers the first restart, where the marker does not exist because
// [FinalizeCompletedMigrations] has not run in this process.
func pendingFlipTrackers(lsmPath string) []PendingFlip {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return nil
	}
	var out []PendingFlip
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		namespace, _, ok := parseMigrationDirName(entry.Name())
		if !ok {
			continue
		}
		indexType, ok := enableMigrationIndexType(namespace)
		if !ok {
			continue
		}
		migDir := filepath.Join(migrationsDir, entry.Name())
		if !fileExistsInDir(migDir, "tidied.mig") && !fileExistsInDir(migDir, "merged.mig") {
			continue
		}
		out = append(out, pendingFlipsForMigration(migDir, indexType)...)
	}
	return out
}

// pendingFlipsForMigration builds one record per property of an enable-*
// tracker dir. Property names come from properties.mig, which every strategy
// writes; the tokenization is best-effort from the recovery payload.
func pendingFlipsForMigration(migDir, indexType string) []PendingFlip {
	props, err := readMigrationProps(migDir)
	if err != nil {
		return nil
	}
	tokenization := readRecoveryTargetTokenization(migDir)
	out := make([]PendingFlip, 0, len(props))
	for _, prop := range props {
		out = append(out, PendingFlip{
			Prop:         prop,
			IndexType:    indexType,
			Tokenization: tokenization,
		})
	}
	return out
}

// enableMigrationIndexType maps a migration namespace to the inverted-index
// type an enable-* migration promotes. Other strategies return false: they
// only run on properties the schema already flags as indexed, so there is no
// swap-vs-flip window on the write path.
func enableMigrationIndexType(namespace string) (string, bool) {
	switch {
	case strings.HasPrefix(namespace, MigrationDirPrefixEnableFilterable):
		return "filterable", true
	case strings.HasPrefix(namespace, MigrationDirPrefixEnableSearchable):
		return "searchable", true
	}
	return "", false
}

// readRecoveryTargetTokenization extracts the migration's target tokenization
// from the tracker's payload.mig. Best-effort: the payload is absent on some
// paths, and an empty result simply leaves the live tokenization in force.
func readRecoveryTargetTokenization(migDir string) string {
	data, err := os.ReadFile(filepath.Join(migDir, reindexRecoveryPayloadFile))
	if err != nil {
		return ""
	}
	var rec struct {
		Payload struct {
			TargetTokenization string `json:"targetTokenization"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return ""
	}
	return rec.Payload.TargetTokenization
}

// mergePendingFlips folds next over prev keyed by (prop, indexType), with
// next winning. Order is stable so the persisted file does not churn.
func mergePendingFlips(prev, next []PendingFlip) []PendingFlip {
	if len(next) == 0 {
		return prev
	}
	out := make([]PendingFlip, 0, len(prev)+len(next))
	at := make(map[pendingFlipKey]int, len(prev)+len(next))
	add := func(f PendingFlip) {
		if i, ok := at[f.key()]; ok {
			out[i] = f
			return
		}
		at[f.key()] = len(out)
		out = append(out, f)
	}
	for _, f := range prev {
		add(f)
	}
	for _, f := range next {
		add(f)
	}
	return out
}

// livePendingFlips drops records that no longer describe a swap-vs-flip
// window: the property left the schema, its index flag flipped, or its
// canonical bucket is gone (an index DELETE inside the window removes it).
// That filter is what retires a record without a hook on either path.
func livePendingFlips(lsmPath string, flips []PendingFlip, class *models.Class) []PendingFlip {
	kept := make([]PendingFlip, 0, len(flips))
	for _, flip := range flips {
		prop := propertyByName(class, flip.Prop)
		if prop == nil || flip.satisfiedByLiveSchema(prop) {
			continue
		}
		bucketName, ok := mainBucketForPropertyIndex(flip.Prop, flip.IndexType)
		if !ok || !fileExists(filepath.Join(lsmPath, bucketName)) {
			continue
		}
		kept = append(kept, flip)
	}
	return kept
}

// resolvePendingFlips reconciles persisted records with what this startup's
// finalize pass promoted, drops records the live schema has caught up with,
// persists the rest, and arms the write overlay for them.
//
// Must run after [FinalizeCompletedMigrations] and before bucket loading /
// [Shard.NotifyReady], so buckets open and writes are never analyzed under
// the pre-flip schema.
func (s *Shard) resolvePendingFlips(promoted []PendingFlip, class *models.Class) []PendingFlip {
	if class == nil {
		return nil
	}
	lsmPath := s.pathLSM()
	persisted, unreadable := readPendingFlips(lsmPath, s.index.logger)
	kept := livePendingFlips(lsmPath, mergePendingFlips(persisted, promoted), class)

	if unreadable {
		// Writing here would replace records we could not read with the ones
		// this startup happens to know about, and a parseable file is what
		// re-enables the sweep [NewShard] skipped. Leaving it keeps the skip
		// in force on every later restart too.
		s.index.logger.WithField("shard", s.name).
			WithField("path", filepath.Join(lsmPath, ".migrations", pendingFlipFile)).
			Error("reindex: keeping the unreadable flip-pending marker; the nonexistent-property-index " +
				"sweep stays disabled for this shard until the file is removed by hand")
	} else if err := writePendingFlips(lsmPath, kept); err != nil {
		s.index.logger.WithField("shard", s.name).
			Errorf("reindex: failed to persist flip-pending records; another restart before the schema flip would drop the migrated bucket: %v", err)
	}
	for _, flip := range kept {
		s.SetForceIndexOverlay(flip.Prop, flip.overlay())
	}
	return kept
}

// dropPendingFlipRecords retires the persisted records of props for one
// index type. Called once that migration's cluster-wide flip commits,
// because the records are what a restart re-arms from: a stale one would
// keep forcing an index the migration no longer owns, and would shield a
// bucket the next index DELETE wants gone.
//
// Scoped to indexType because a property can carry a record per index type
// (see [PendingFlip]): retiring the filterable record when the searchable
// migration flipped would hand a bucket whose own flip is still pending to
// the [propertyDeleteIndexHelper] sweep, and its sidecars are already gone.
func dropPendingFlipRecords(lsmPath string, props []string, indexType string, logger logrus.FieldLogger) {
	existing, _ := readPendingFlips(lsmPath, logger)
	if len(existing) == 0 {
		return
	}
	drop := make(map[pendingFlipKey]struct{}, len(props))
	for _, p := range props {
		drop[pendingFlipKey{prop: p, indexType: indexType}] = struct{}{}
	}
	kept := make([]PendingFlip, 0, len(existing))
	for _, flip := range existing {
		if _, ok := drop[flip.key()]; !ok {
			kept = append(kept, flip)
		}
	}
	if len(kept) == len(existing) {
		return
	}
	if err := writePendingFlips(lsmPath, kept); err != nil {
		logger.WithField("path", lsmPath).
			Errorf("reindex: failed to retire flip-pending records after the schema flip: %v", err)
	}
}

func propertyByName(class *models.Class, propName string) *models.Property {
	for _, prop := range class.Properties {
		if prop != nil && prop.Name == propName {
			return prop
		}
	}
	return nil
}
