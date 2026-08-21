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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
)

// migrationMirrorDisarmer disarms one record's double-write mirror for one
// property. The actor that disarms is never the actor that armed — a
// successor's engine, this module, terminal cleanup — which is why the
// handles cannot live on the arming task instance. Disarming a pair that is
// not armed is a no-op: every edge here is re-derived at each load.
type migrationMirrorDisarmer interface {
	DisarmMigrationMirror(key MigrationRecordKey, prop string)
}

// migrationStagedBucketCloser shuts a record's staged buckets down before
// their directory is removed. Removing an open bucket's directory leaves
// mmaps, in-flight compactions and a registry entry behind.
type migrationStagedBucketCloser interface {
	ShutdownStagedBuckets(ctx context.Context, key MigrationRecordKey, prop string) error
}

// migrationReconcileDeps are the facts and collaborators reconciliation needs
// from outside itself. The shard supplies them when this module is wired into
// shard load.
type migrationReconcileDeps struct {
	// LocalTasks is this node's own applied view of the reindex namespace, not
	// the leader's. Reading it needs no round-trip and cannot block a load.
	//
	// The second result reports whether the map could be read at all. It
	// matters because an unreadable map and an empty one are opposite facts:
	// an absent task is terminal and licenses a discard, so a source that is
	// merely not installed yet would read as "every task is gone".
	LocalTasks func() ([]*distributedtask.Task, bool)

	// Class is the locally applied schema for the migrated collection, or nil
	// when the collection is not in it.
	Class func() *models.Class

	Mirror  migrationMirrorDisarmer
	Buckets migrationStagedBucketCloser
}

// migrationReconciler is the state machine's single load-time owner. One pass
// per shard load, before any bucket opens — it renames directories — and
// before any question is answered. It is the only place external facts enter
// and the only place disk may override the record.
type migrationReconciler struct {
	store   *MigrationRecordStore
	lsmPath string
	logger  logrus.FieldLogger
	deps    migrationReconcileDeps
}

func newMigrationReconciler(store *MigrationRecordStore, lsmPath string,
	logger logrus.FieldLogger, deps migrationReconcileDeps,
) *migrationReconciler {
	return &migrationReconciler{store: store, lsmPath: lsmPath, logger: logger, deps: deps}
}

// migrationVerdict is what the cluster says should become of a migration
// whose disposition is not decidable from disk alone.
type migrationVerdict uint8

const (
	// migrationVerdictLeave means a live task still owns this migration, or
	// the evidence is too thin to act on. Doing nothing is always safe.
	migrationVerdictLeave migrationVerdict = iota
	migrationVerdictCommit
	migrationVerdictDiscard
)

// Reconcile runs one pass over this shard's records.
func (r *migrationReconciler) Reconcile(ctx context.Context) error {
	if err := r.store.Load(); err != nil {
		return fmt.Errorf("load migration records: %w", err)
	}

	// An unreadable record's property list is exactly what could not be read,
	// so there is no way to scope the withholding to the properties it stands
	// on. Withholding shard-wide is the only sound reading of "never retire,
	// never witness" when the subject is unknown.
	frozen := len(r.store.Unreadable()) > 0
	if frozen {
		r.logger.WithField("path", r.store.Dir()).Warnf(
			"%d migration record(s) not understood: withholding every destructive and promoting action on this shard",
			len(r.store.Unreadable()))
	}

	records := r.store.Records()
	if !frozen {
		r.retireSuperseded(ctx, records)
		records = r.store.Records()
	}

	for _, rec := range records {
		if err := r.reconcileOne(ctx, rec, records, frozen); err != nil {
			// One migration must not be able to keep a shard from loading.
			r.logger.WithField("record", rec.Subject().Key.String()).Errorf("reconcile migration record: %v", err)
		}
	}
	return nil
}

func (r *migrationReconciler) reconcileOne(ctx context.Context, rec MigrationRecord,
	all []MigrationRecord, frozen bool,
) error {
	switch typed := rec.(type) {
	case MigrationRecordIterating:
		return r.reconcileUncommitted(ctx, typed, frozen)
	case MigrationRecordIterated:
		return r.reconcileIterated(ctx, typed, frozen)
	case MigrationRecordMerged:
		return r.reconcileMerged(ctx, typed, all, frozen)
	case MigrationRecordSwapped:
		return r.reconcileSwapped(ctx, typed, all, frozen)
	case MigrationRecordPromoted:
		return r.reconcilePromoted(ctx, typed, frozen)
	default:
		return fmt.Errorf("no reconciliation for record variant %T", rec)
	}
}

// reconcileUncommitted handles Iterating: the rebuild is in flight, nothing is
// staged completely, and the canonical bucket has never stopped being primary.
func (r *migrationReconciler) reconcileUncommitted(ctx context.Context, rec MigrationRecord, frozen bool) error {
	if frozen {
		return nil
	}
	subject := rec.Subject()
	switch verdict, why := r.verdict(subject); verdict {
	case migrationVerdictDiscard:
		return r.discard(ctx, subject, why)
	case migrationVerdictCommit:
		// The cluster considers the task done while this shard never finished
		// its rebuild. Nothing here can complete it and nothing may be
		// deleted on the strength of a guess.
		r.logger.WithField("record", subject.Key.String()).Warnf(
			"migration is %s locally but the cluster reports it committed (%s)", rec.State(), why)
		return nil
	case migrationVerdictLeave:
		return nil
	default:
		return fmt.Errorf("unhandled verdict %d", verdict)
	}
}

// reconcileIterated adds the one reverse edge in the machine: a record can
// outrun its data, and resuming from a checkpoint against data that is gone
// would silently skip every object at or below the stale key.
func (r *migrationReconciler) reconcileIterated(ctx context.Context, rec MigrationRecordIterated, frozen bool) error {
	subject := rec.Subject()
	if !frozen {
		if verdict, why := r.verdict(subject); verdict == migrationVerdictDiscard {
			return r.discard(ctx, subject, why)
		}
	}

	// Every directory the record owns has to be present at Iterated: the
	// rebuild wrote into the sidecars and the mirror into the staged ones.
	// A missing one means the rebuild never reached disk or was reclaimed,
	// and a resume from the checkpoint would then swap in a bucket holding
	// only the objects that happen to sort above the stale key.
	for _, dir := range migrationOwnedDirs(subject) {
		if r.dirExists(dir) {
			continue
		}
		r.logger.WithField("record", subject.Key.String()).Warnf(
			"rebuilt data at %q is gone; restarting the rebuild from the beginning", dir)
		return r.store.Put(NewMigrationRecordIterating(subject, MigrationCheckpoint{}))
	}
	return r.reconcileUncommitted(ctx, rec, frozen)
}

// reconcileMerged owns the machine's one external-fact edge. The staged data
// is complete; whether it should ever become live is a cluster fact the
// record deliberately does not hold.
func (r *migrationReconciler) reconcileMerged(ctx context.Context, rec MigrationRecordMerged,
	all []MigrationRecord, frozen bool,
) error {
	if frozen {
		return nil
	}
	subject := rec.Subject()
	verdict, why := r.verdict(subject)

	switch verdict {
	case migrationVerdictLeave:
		return nil
	case migrationVerdictDiscard:
		// Provably safe here: the flip record is written ahead of the first
		// flip, so a Merged record proves no flip was decided, and before the
		// flip every acknowledged write landed in the canonical bucket
		// natively. This removes only the staged copy.
		return r.discard(ctx, subject, why)
	case migrationVerdictCommit:
	default:
		return fmt.Errorf("unhandled verdict %d", verdict)
	}

	swapped, err := r.commitMerged(subject, why)
	if err != nil {
		return err
	}
	return r.reconcileSwapped(ctx, swapped, all, frozen)
}

// commitMerged writes the flip decision and nothing else. The verdict is
// durable before anything acts on it, so a crash between the two resumes from
// Swapped and re-runs idempotent directory work instead of re-deciding a
// question whose inputs may have changed.
func (r *migrationReconciler) commitMerged(subject MigrationSubject, why string) (MigrationRecordSwapped, error) {
	displaced := make(map[string]string, len(subject.Properties))
	for _, prop := range subject.Properties {
		displaced[prop] = subject.CanonicalDirs[prop]
	}
	swapped := NewMigrationRecordSwapped(subject, slices.Clone(subject.Properties), displaced)
	if err := r.store.Put(swapped); err != nil {
		return swapped, fmt.Errorf("record the flip decision: %w", err)
	}
	r.logger.WithField("record", subject.Key.String()).Infof("committing merged migration: %s", why)
	return swapped, nil
}

// ReconcileAfterTaskMap re-runs the dispositions that are a cluster fact. A
// shard loaded while this node was still catching up on RAFT read the task map
// as unavailable and decided nothing that depends on it. A shard that is not
// multi-tenant is never loaded again in this process, and the next restart
// repeats the same ordering — so without a second pass a migration the cluster
// finished serves pre-migration data forever, and one the cluster abandoned
// keeps its staged copy forever.
//
// The two arms differ in when they act. The commit arm only records the
// decision: promotion renames a directory whose buckets are open by now, so it
// waits for the next load. The discard arm acts immediately, which is safe
// only because this pass is installed before the scheduler resumes any local
// unit — see the discard branch below.
//
// Only the verdict decides here. The reverse edge from Iterated is deliberately
// left to a load: it clears the checkpoint, and a live task iterating right now
// would resume from the beginning.
//
// It reads the records the shard already holds rather than re-reading disk:
// the engine writes through the same store, and a reload here would drop what
// it published since the load.
func (r *migrationReconciler) ReconcileAfterTaskMap(ctx context.Context) {
	if len(r.store.Unreadable()) > 0 {
		return
	}
	for _, rec := range r.store.Records() {
		if rec.PointerSwapped() {
			continue
		}
		subject := rec.Subject()
		verdict, why := r.verdict(subject)
		switch {
		case verdict == migrationVerdictDiscard:
			// This removes directories a unit would be writing through a raw
			// bucket pointer, and a task's cluster status goes terminal
			// independently of when the local unit exits. What makes it safe
			// is that the task source is installed before Scheduler.Start, so
			// no unit has been resumed in this process yet.
			if err := r.discard(ctx, subject, why); err != nil {
				r.logger.WithField("record", subject.Key.String()).Errorf(
					"discard a migration once the task map became readable: %v", err)
			}
		case verdict == migrationVerdictCommit && rec.State() == MigrationStateMerged:
			if _, err := r.commitMerged(subject, why); err != nil {
				r.logger.WithField("record", subject.Key.String()).Errorf(
					"commit a merged migration once the task map became readable: %v", err)
				continue
			}
			r.logger.WithField("record", subject.Key.String()).Info(
				"the staged data is the data; the next shard load promotes it onto the canonical name")
		}
	}
}

// reconcileSwapped promotes. Every arm is decided by probing the handles the
// record already carries, never by parsing a directory name.
func (r *migrationReconciler) reconcileSwapped(ctx context.Context, rec MigrationRecordSwapped,
	all []MigrationRecord, frozen bool,
) error {
	if frozen {
		return nil
	}
	subject := rec.Subject()
	settled := true

	for _, prop := range subject.Properties {
		// A superseded property is retired by the relation. Probing it here
		// would read a successor's removal as a promotion that already ran.
		if migrationPropertySuperseded(all, subject, prop) {
			continue
		}

		staged, canonical := subject.StagedDirs[prop], subject.CanonicalDirs[prop]
		if staged == "" || canonical == "" {
			settled = false
			r.logger.WithField("record", subject.Key.String()).Errorf(
				"cannot promote property %q: the record names no staged or canonical directory for it", prop)
			continue
		}

		displaced, _ := rec.DisplacedDir(prop)
		promoted, err := r.promoteProperty(subject, prop, staged, canonical, displaced)
		if err != nil {
			settled = false
			r.logger.WithField("record", subject.Key.String()).Errorf("promote property %q: %v", prop, err)
			continue
		}
		if !promoted {
			settled = false
		}
	}

	if !settled {
		return nil
	}
	return r.store.Put(NewMigrationRecordPromoted(subject, rec.Flipped(), rec.displacedDirs))
}

// promoteProperty is the handle probe. Every destructive arm is guarded by the
// presence of the staged directory, and that guard is the whole soundness
// argument: within a record's chain only the promotion rename removes the
// staged directory, so a missing one proves the canonical name already holds
// the renamed data. Nothing here inspects a directory's contents — three
// strategies pre-create an empty canonical bucket when the migration arms, so
// an empty canonical directory is the normal armed state and means nothing.
func (r *migrationReconciler) promoteProperty(subject MigrationSubject, prop, staged, canonical, displaced string) (bool, error) {
	if !r.dirExists(staged) {
		if r.dirExists(canonical) {
			return true, nil
		}
		// A record must not promote a subject that does not exist. Restore is
		// the one known producer: it materializes a class tree file by file,
		// so empty directories vanish, and its in-flight interlock does not
		// cover the committed-awaiting-promotion window.
		r.logger.WithField("record", subject.Key.String()).Errorf(
			"property %q has neither its staged directory %q nor its canonical directory %q; preserving the record and promoting nothing",
			prop, staged, canonical)
		return false, nil
	}

	// The flip pushed this directory aside, and displaced directories have
	// exactly one owner: the record that displaced them. It is usually the
	// canonical path, but a predecessor that flipped and never promoted still
	// holds its live data at a staged name, so it can be that instead.
	if displaced != "" && displaced != canonical && r.dirExists(displaced) {
		if err := os.RemoveAll(r.path(displaced)); err != nil {
			return false, fmt.Errorf("remove displaced directory %q: %w", displaced, err)
		}
	}
	if r.dirExists(canonical) {
		if err := os.RemoveAll(r.path(canonical)); err != nil {
			return false, fmt.Errorf("remove displaced directory %q: %w", canonical, err)
		}
	}
	return true, r.rename(staged, canonical)
}

// reconcilePromoted is the closure sweep. The record outlives its data: under
// opaque naming only its owned-dirs list can attribute a leftover from a
// retirement step that partly failed.
func (r *migrationReconciler) reconcilePromoted(ctx context.Context, rec MigrationRecordPromoted, frozen bool) error {
	if frozen {
		return nil
	}
	subject := rec.Subject()

	remaining := r.reclaimOwnedDirs(subject)
	if len(remaining) > 0 {
		r.logger.WithField("record", subject.Key.String()).Warnf(
			"%d directory/directories of a promoted migration could not be reclaimed yet", len(remaining))
		return nil
	}

	class := r.deps.Class()
	if class == nil {
		return nil
	}
	// Sweeping before the effect is visible would delete the answer to the one
	// question a promoted record still exists to answer.
	if !migrationEffectSatisfied(class, subject) {
		return nil
	}
	r.removeTrackerDir(subject)
	return r.store.Remove(subject.Key)
}

// verdict consults the two external facts, in the order that makes the second
// unnecessary whenever the first is conclusive.
func (r *migrationReconciler) verdict(subject MigrationSubject) (migrationVerdict, string) {
	task, mapReadable := r.findTask(subject)
	if !mapReadable {
		return migrationVerdictLeave, "this node's task map cannot be read yet"
	}
	if task != nil {
		switch task.Status {
		case distributedtask.TaskStatusFinished:
			return migrationVerdictCommit, "owning task finished"
		case distributedtask.TaskStatusCancelled, distributedtask.TaskStatusFailed:
			return migrationVerdictDiscard, fmt.Sprintf("owning task %s", task.Status)
		default:
			// Active, or a status this build does not recognize. The task that
			// discovery finds in flight resumes the migration itself.
			return migrationVerdictLeave, fmt.Sprintf("owning task is %s", task.Status)
		}
	}

	// A task absent from the applied task map is terminal: tasks are added
	// before any unit runs and only cleanup removes them, and cleanup runs
	// only on terminal tasks. So the migration's own schema effect decides.
	class := r.deps.Class()
	if class == nil {
		return migrationVerdictLeave, "collection is not in the locally applied schema"
	}
	if migrationEffectSatisfied(class, subject) {
		return migrationVerdictCommit, "owning task is gone and the schema shows its effect"
	}
	return migrationVerdictDiscard, "owning task is gone and the schema does not show its effect"
}

// findTask matches on ID and version together: the same task ID re-submitted
// is a different run, and its outcome says nothing about this record's.
func (r *migrationReconciler) findTask(subject MigrationSubject) (*distributedtask.Task, bool) {
	if r.deps.LocalTasks == nil {
		return nil, false
	}
	tasks, readable := r.deps.LocalTasks()
	if !readable {
		return nil, false
	}
	for _, task := range tasks {
		if task != nil && task.ID == subject.TaskID && task.Version == subject.Key.TaskVersion {
			return task, true
		}
	}
	return nil, true
}

// discard is the cancel edge. The order is load-bearing: a still-armed mirror
// whose staged bucket has been removed fails the next user write, because a
// mirror copy that fails fails the write with it.
func (r *migrationReconciler) discard(ctx context.Context, subject MigrationSubject, why string) error {
	r.logger.WithField("record", subject.Key.String()).Infof("discarding staged migration data: %s", why)

	ec := errorcompounder.New()
	for _, prop := range subject.Properties {
		ec.Add(r.disarmAndClose(ctx, subject.Key, prop))
	}
	if err := ec.ToError(); err != nil {
		return err
	}
	if remaining := r.reclaimOwnedDirs(subject); len(remaining) > 0 {
		return fmt.Errorf("%d owned directory/directories survived the discard", len(remaining))
	}
	r.removeTrackerDir(subject)
	return r.store.Remove(subject.Key)
}

// disarmAndClose reports a shutdown failure rather than logging it away: every
// caller removes the property's directory next, and removing an open bucket's
// directory leaves mmaps, in-flight compactions and a registry entry behind.
func (r *migrationReconciler) disarmAndClose(ctx context.Context, key MigrationRecordKey, prop string) error {
	if r.deps.Mirror != nil {
		r.deps.Mirror.DisarmMigrationMirror(key, prop)
	}
	if r.deps.Buckets == nil {
		return nil
	}
	if err := r.deps.Buckets.ShutdownStagedBuckets(ctx, key, prop); err != nil {
		return fmt.Errorf("shut down staged buckets of %s for property %q: %w", key.String(), prop, err)
	}
	return nil
}

// removeTrackerDir removes the migration's own directory under .migrations,
// which holds the recovery payload. It is deliberately not one of the owned
// dirs: those are bucket directories at the LSM root, and the Iterated probe
// reads their presence as proof the rebuild reached disk, which this one says
// nothing about.
func (r *migrationReconciler) removeTrackerDir(subject MigrationSubject) {
	if subject.TrackerDir == "" {
		return
	}
	path := filepath.Join(r.lsmPath, migrationsDir, subject.TrackerDir)
	if err := os.RemoveAll(path); err != nil {
		r.logger.WithField("dir", path).Errorf("remove migration directory: %v", err)
	}
}

// reclaimOwnedDirs removes every directory the record created and reports the
// ones that survived. The canonical directory is never among them.
func (r *migrationReconciler) reclaimOwnedDirs(subject MigrationSubject) []string {
	var remaining []string
	for _, dir := range migrationOwnedDirs(subject) {
		if err := os.RemoveAll(r.path(dir)); err != nil {
			r.logger.WithField("dir", dir).Errorf("remove migration directory: %v", err)
		}
		if r.dirExists(dir) {
			remaining = append(remaining, dir)
		}
	}
	return remaining
}

// migrationOwnedDirs lists what the migration created, which is what a sweep
// may reclaim. Canonical and displaced directories are deliberately absent:
// the canonical name predates the migration and, after promotion, holds the
// live data.
func migrationOwnedDirs(subject MigrationSubject) []string {
	dirs := make([]string, 0, len(subject.StagedDirs)+len(subject.SidecarDirs))
	for _, prop := range subject.Properties {
		if dir := subject.StagedDirs[prop]; dir != "" {
			dirs = append(dirs, dir)
		}
	}
	dirs = append(dirs, subject.SidecarDirs...)
	return dirs
}

func (r *migrationReconciler) path(dir string) string { return filepath.Join(r.lsmPath, dir) }

func (r *migrationReconciler) dirExists(dir string) bool {
	if dir == "" {
		return false
	}
	info, err := os.Stat(r.path(dir))
	return err == nil && info.IsDir()
}

func (r *migrationReconciler) rename(from, to string) error {
	if err := os.Rename(r.path(from), r.path(to)); err != nil {
		return fmt.Errorf("promote %q to %q: %w", from, to, err)
	}
	return nil
}
