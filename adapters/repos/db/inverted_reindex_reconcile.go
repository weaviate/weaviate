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
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type migrationMirrorDisarmer interface {
	DisarmMigrationMirror(key MigrationRecordKey, prop string)
}

// Must run before the directory is removed, or mmaps and compactions leak.
// Takes directories rather than a record, so the caller closes only what it has
// already decided it may remove: a shutdown deregisters the bucket and nothing
// reopens it before the next shard load, so closing a directory the caller then
// leaves in place stops that data serving with no record left to answer for it.
type migrationStagedBucketCloser interface {
	ShutdownStagedBucketsAt(ctx context.Context, dirs []string) error
}

type migrationReconcileDeps struct {
	LocalTasks func() ([]*distributedtask.Task, bool)

	SealUnit func(distributedtask.TaskDescriptor, string) (func(), bool)

	Class func() *models.Class

	Mirror  migrationMirrorDisarmer
	Buckets migrationStagedBucketCloser
}

type migrationReconciler struct {
	store      *MigrationRecordStore
	lsmPath    string
	wedgedKeys map[MigrationRecordKey]bool
	logger     logrus.FieldLogger
	deps       migrationReconcileDeps
}

func newMigrationReconciler(store *MigrationRecordStore, lsmPath string,
	logger logrus.FieldLogger, deps migrationReconcileDeps,
) *migrationReconciler {
	return &migrationReconciler{store: store, lsmPath: lsmPath, logger: logger, deps: deps}
}

type migrationVerdict uint8

const (
	migrationVerdictLeave migrationVerdict = iota
	migrationVerdictCommit
	migrationVerdictDiscard
	// Keep the record and its data, and stop asking: the leader has answered
	// that the owning task is gone and no schema reading will ever confirm or
	// deny this migration, so every later pass reaches the same answer.
	migrationVerdictWedge
)

const migrationWedgeRemedy = "Submit a new migration covering every property this record names; " +
	"once its flip is durable it supersedes this record, and the next shard load " +
	"reclaims the record and its directories."

// Supersession keys on the canonical directory name, which is the very thing
// this record does not name, so a new migration could never take it over.
const migrationWedgeRemedyNoCanonical = "Once you have confirmed which directory holds " +
	"the property's data, remove this record by hand; no new migration can supersede a " +
	"record that names no canonical directory."

// migrationReportedNames orders, deduplicates and caps a property list for a
// log line. A property list is user-chosen and unbounded, so a caller that logs
// one through this helper pairs the names with its own count field rather than
// formatting the list whole.
func migrationReportedNames(names []string) []string {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return reportedShardNames(set)
}

// Counts the record against this pass and no further. A refusal, a filesystem
// fault and a cancellation all land here, and the periodic pass is the only
// thing that retries any of them.
func (r *migrationReconciler) countWedged(key MigrationRecordKey) {
	if r.wedgedKeys == nil {
		r.wedgedKeys = map[MigrationRecordKey]bool{}
	}
	r.wedgedKeys[key] = true
}

// The reconciler lives for one pass, so a wedge it diagnosed also goes to the
// store, which is what the periodic pass asks whether this shard has anything
// left to decide. Only for a record no pass on this build could advance: a
// record that merely did not settle this time has to be asked again.
func (r *migrationReconciler) markWedged(key MigrationRecordKey) {
	r.countWedged(key)
	r.store.MarkWedged(key)
}

func (r *migrationReconciler) wedged(subject MigrationSubject, remedy, format string, args ...any) {
	r.markWedged(subject.Key)
	r.logger.WithField("record", subject.Key.String()).
		WithField("property_count", len(subject.Props)).
		Errorf(format+" "+remedy, args...)
}

// Debug, not Info: a Leave repeats on every shard load for the life of the
// record, and only one that never resolves is worth finding.
func (r *migrationReconciler) leftStanding(subject MigrationSubject, why string) {
	r.logger.WithField("record", subject.Key.String()).Debugf(
		"leaving the migration record standing: %s", why)
}

// Error, not Warn: one wedged record is cleared by resubmitting one migration,
// while this withholds every promoting and destructive action of the pass for
// the life of the shard.
func (r *migrationReconciler) freezeNotice() {
	unreadable := r.store.Unreadable()
	// A store-scope fault read no record, so a count and a per-file remedy would
	// both name something that does not exist.
	for _, u := range unreadable {
		if u.Scope == MigrationRecordFaultStore {
			r.logger.WithField("path", r.store.Dir()).Errorf(
				"the migration record store cannot answer for this shard (%s): withholding every "+
					"destructive and promoting action this migration-record pass takes here until the cause is cleared",
				u.Reason)
			return
		}
	}
	r.logger.WithField("path", r.store.Dir()).Errorf(
		"%d migration record(s) not understood: withholding every destructive and promoting action "+
			"this migration-record pass takes on this shard. This is normally a downgrade: run the build "+
			"that wrote them, or remove the files the record store named once you have confirmed what they claim",
		len(unreadable))
}

func (r *migrationReconciler) WedgedCount() int { return len(r.wedgedKeys) }

func (r *migrationReconciler) Reconcile(ctx context.Context) error {
	if err := r.store.Load(); err != nil {
		// The load's own fault is recorded as unreadable, so the freeze this
		// return causes is announced on this path too.
		r.freezeNotice()
		return fmt.Errorf("load migration records: %w", err)
	}

	someRecordsUnreadable := len(r.store.Unreadable()) > 0
	if someRecordsUnreadable {
		r.freezeNotice()
	}

	r.wedgedKeys = nil
	r.RetireSuperseded(ctx)

	for _, rec := range r.store.Records() {
		// The pass renames and removes whole property indexes, and it runs on
		// the RAFT apply loop, where every further apply queues behind it.
		if err := ctx.Err(); err != nil {
			r.logger.Errorf("stopping the migration-record pass on this shard: %v", err)
			return nil
		}
		if err := r.reconcileOne(ctx, rec, someRecordsUnreadable); err != nil {
			// One migration must not be able to keep a shard from loading. Not
			// marked on the store: a refused reclaim, a directory that would not
			// go and a cancelled activation all arrive here, and the record is
			// kept precisely so a later pass retries them.
			r.countWedged(rec.Subject().Key)
			r.logger.WithField("record", rec.Subject().Key.String()).Errorf("reconcile migration record: %v", err)
		}
	}
	return nil
}

// someUnreadable: an unreadable record's properties count as unreadable, so
// withholding must cover the whole shard, not just those.
func (r *migrationReconciler) reconcileOne(ctx context.Context, rec MigrationRecord,
	someUnreadable bool,
) error {
	switch typed := rec.(type) {
	case MigrationRecordIterating, MigrationRecordIterated:
		return r.reconcileUncommitted(ctx, rec, someUnreadable)
	case MigrationRecordMerged:
		return r.reconcileMerged(ctx, typed, someUnreadable)
	case MigrationRecordSwapped:
		return r.reconcileSwapped(ctx, typed, someUnreadable)
	case MigrationRecordPromoted:
		return r.reconcilePromoted(ctx, typed, someUnreadable)
	default:
		return fmt.Errorf("no reconciliation for record variant %T", rec)
	}
}

// Discard runs before the reverse edge: restarting when directories are
// gone would arm a live mirror nothing will ever recreate them for.
func (r *migrationReconciler) reconcileUncommitted(ctx context.Context, rec MigrationRecord,
	someUnreadable bool,
) error {
	subject := rec.Subject()

	if someUnreadable {
		_, err := r.restartIfRebuiltDataGone(subject)
		return err
	}

	verdict, why := r.localVerdict(subject)
	switch verdict {
	case migrationVerdictDiscard:
		return r.discard(ctx, subject, why)
	case migrationVerdictCommit:
		// Above the restart edge, not below it: a migration the cluster
		// committed can never be finished here, so restarting its rebuild
		// would restart it on every load and the record would never terminate.
		r.wedged(subject, migrationWedgeRemedy,
			"migration is %s locally but the cluster reports it committed (%s), so no load here can finish it. "+
				"Properties: %s.",
			rec.State(), why, strings.Join(migrationReportedNames(subject.Properties()), ", "))
		return nil
	case migrationVerdictLeave:
	default:
		return fmt.Errorf("unhandled verdict %d", verdict)
	}

	restarted, err := r.restartIfRebuiltDataGone(subject)
	if err != nil || restarted {
		return err
	}
	r.leftStanding(subject, why)
	return nil
}

// A missing owned directory means the rebuild never reached disk, so
// resuming would swap in an incomplete bucket.
func (r *migrationReconciler) restartIfRebuiltDataGone(subject MigrationSubject) (bool, error) {
	for _, dir := range migrationOwnedDirs(subject) {
		there, err := r.dirExists(dir)
		if err != nil {
			return false, err
		}
		if there {
			continue
		}

		restarted := subject
		restarted.IterationCutoff = migrationHorizonEverything

		r.logger.WithField("record", subject.Key.String()).Warnf(
			"rebuilt data at %q is gone; restarting the rebuild from the beginning and re-covering the horizon the mirror held", dir)
		return true, r.store.Put(NewMigrationRecordIterating(restarted, MigrationCheckpoint{}))
	}
	return false, nil
}

func (r *migrationReconciler) reconcileMerged(ctx context.Context, rec MigrationRecordMerged,
	someUnreadable bool,
) error {
	if someUnreadable {
		return nil
	}
	subject := rec.Subject()
	verdict, why := r.localVerdict(subject)

	switch verdict {
	case migrationVerdictLeave:
		r.leftStanding(subject, why)
		return nil
	case migrationVerdictDiscard:
		return r.discard(ctx, subject, why)
	case migrationVerdictCommit:
	default:
		return fmt.Errorf("unhandled verdict %d", verdict)
	}

	swapped, err := r.commitMerged(subject, why)
	if err != nil {
		return err
	}
	return r.reconcileSwapped(ctx, swapped, someUnreadable)
}

// Skips a superseded property the way promoteSealed does: retirement deletes
// that property's staged directory, and demanding it back here would wedge the
// record for good.
func (r *migrationReconciler) commitMerged(subject MigrationSubject,
	why string,
) (MigrationRecordSwapped, error) {
	all := r.store.Records()
	props := subject.Properties()
	for _, prop := range props {
		if migrationPropertySuperseded(all, subject, prop) {
			continue
		}
		staged, canonical := subject.Props[prop].Staged, subject.Props[prop].Canonical
		if staged == "" || canonical == "" {
			return MigrationRecordSwapped{}, fmt.Errorf(
				"refusing to commit the flip: property %q names no staged or canonical directory, "+
					"so the flip it would record could never be promoted", prop)
		}
		there, err := r.dirExists(staged)
		if err != nil {
			return MigrationRecordSwapped{}, err
		}
		if !there {
			return MigrationRecordSwapped{}, fmt.Errorf(
				"refusing to commit the flip: property %q names no staged directory %q to promote", prop, staged)
		}
	}

	displaced := make(map[string]string, len(props))
	for _, prop := range props {
		displaced[prop] = subject.Props[prop].Canonical
	}
	swapped := NewMigrationRecordSwapped(subject, props, displaced)
	if err := r.store.Put(swapped); err != nil {
		return swapped, fmt.Errorf("record the flip decision: %w", err)
	}
	r.logger.WithField("record", subject.Key.String()).Infof("committing merged migration: %s", why)
	return swapped, nil
}

// ReconcileWithClusterTasks settles the dispositions a shard load withheld.
// The leader's task list is an argument so a shard load never blocks on it.
func (r *migrationReconciler) ReconcileWithClusterTasks(ctx context.Context, tasks []*distributedtask.Task) {
	if len(r.store.Unreadable()) > 0 {
		return
	}
	for _, rec := range r.store.Records() {
		if ctx.Err() != nil {
			return
		}
		subject := rec.Subject()
		if rec.FlipDecided() || r.store.Wedged(subject.Key) {
			continue
		}
		verdict, why := r.clusterVerdict(subject, tasks)
		switch {
		case verdict == migrationVerdictWedge:
			r.wedged(subject, migrationWedgeRemedy, "%s.", why)
		case verdict == migrationVerdictDiscard:
			if err := r.discard(ctx, subject, why); err != nil {
				r.logger.WithField("record", subject.Key.String()).Errorf(
					"discard a migration the leader's task list settled: %v", err)
			}
		case verdict == migrationVerdictCommit && rec.State() == MigrationStateMerged:
			if _, err := r.commitMerged(subject, why); err != nil {
				r.logger.WithField("record", subject.Key.String()).Errorf(
					"commit a merged migration the leader's task list settled: %v", err)
				continue
			}
			r.logger.WithField("record", subject.Key.String()).Info(
				"the staged data is the data; the next shard load promotes it onto the canonical name")
		}
	}
	monitoring.GetMetrics().AddMigrationRecordsWedged(r.WedgedCount(), 0)
}

// As destructive as discard (removes directories before renaming), so
// sealed the same way.
func (r *migrationReconciler) reconcileSwapped(ctx context.Context, rec MigrationRecordSwapped,
	someUnreadable bool,
) error {
	if someUnreadable {
		return nil
	}
	return r.withSealedUnit(rec.Subject(), "its promotion", func() error {
		return r.promoteSealed(ctx, rec)
	})
}

func (r *migrationReconciler) promoteSealed(ctx context.Context, rec MigrationRecordSwapped) error {
	subject := rec.Subject()
	all := r.store.Records()
	settled := true
	promotedAny := false
	// One line per record, not one per property: a single I/O fault fails every
	// property, and a failure between the rename and its sync yields two.
	promoting := errorcompounder.New()
	var noHandles []string
	unretired := errorcompounder.New()

	for _, prop := range subject.Properties() {
		// Between Put(started) and the rename there is no check: those two are
		// what make an interrupted promotion readable, so they stay together.
		if err := ctx.Err(); err != nil {
			return err
		}
		if migrationPropertySuperseded(all, subject, prop) {
			if retired, why := r.supersededPropertyIsRetired(all, subject, prop); !retired {
				settled = false
				unretired.Addf("%s", why)
			}
			continue
		}

		staged, canonical := subject.Props[prop].Staged, subject.Props[prop].Canonical
		if staged == "" || canonical == "" {
			settled = false
			noHandles = append(noHandles, prop)
			continue
		}

		displaced, _ := rec.DisplacedDir(prop)
		updated, promoted, err := r.promoteProperty(rec, prop,
			promotionDirs{staged: staged, canonical: canonical, displaced: displaced})
		rec = updated
		if err != nil {
			settled = false
			promoting.AddWrapf(err, "promote property %q", prop)
			continue
		}
		if !promoted {
			settled = false
			continue
		}
		promotedAny = true
	}

	if len(noHandles) > 0 {
		r.wedged(subject, migrationWedgeRemedyNoCanonical,
			"cannot promote %d propert(y/ies): the record names no staged or canonical directory for %s.",
			len(noHandles), strings.Join(migrationReportedNames(noHandles), ", "))
	}
	if err := promoting.ToErrorLimited(maxReportedErrors); err != nil {
		r.logger.WithField("record", subject.Key.String()).
			WithField("property_count", len(subject.Props)).
			Errorf("promote the properties of a migration: %v", err)
	}

	// One line for the record, not one per property. An unretired property keeps
	// the record Swapped, so the next pass asks the same question of the same
	// properties and would repeat itself for as long as the record stands.
	if reported := unretired.ToErrorLimited(maxReportedErrors); reported != nil {
		r.logger.WithField("record", subject.Key.String()).Warnf(
			"%d superseded propert%s of this record still hold their staged directory, so retirement "+
				"has not run for them; keeping the record, which is what lets the next load retry: %v",
			unretired.Len(), pluralY(unretired.Len()), reported)
	}

	// A Promoted record is read as "this rename happened". A pass that renamed
	// nothing must not write one; a fully superseded record is retired instead.
	if !settled || !promotedAny {
		return nil
	}
	return r.store.Put(NewMigrationRecordPromoted(subject, rec.Flipped(), rec.displacedDirs))
}

// supersededPropertyIsRetired reports whether retirement has taken the staged
// directory of a superseded property, and why it could not tell when it has
// not. The reason goes back to the caller rather than into a log line: the
// caller asks once per property of a record that stays Swapped until every
// answer is yes, so a line here repeats per property on every pass.
func (r *migrationReconciler) supersededPropertyIsRetired(all []MigrationRecord,
	subject MigrationSubject, prop string,
) (bool, string) {
	staged := subject.Props[prop].Staged
	if migrationRetirementLeavesStagedDir(all, subject, staged) {
		return true, ""
	}
	there, err := r.dirExists(staged)
	if err != nil {
		return false, fmt.Sprintf("property %q: staged directory %q could not be read: %v", prop, staged, err)
	}
	if there {
		return false, fmt.Sprintf("property %q: staged directory %q is still on disk", prop, staged)
	}
	return true, ""
}

// pluralY renders the "y"/"ies" tail of "property" for a count.
func pluralY(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

// Directory presence can't prove a rename ran (canonical is pre-created
// empty either way), so the record brackets it with a start/finish write.
func (r *migrationReconciler) promoteProperty(rec MigrationRecordSwapped,
	prop string, dirs promotionDirs,
) (MigrationRecordSwapped, bool, error) {
	subject := rec.Subject()
	staged, canonical, displaced := dirs.staged, dirs.canonical, dirs.displaced
	switch rec.PromotionOf(prop) {
	case migrationPromotionFinished:
		return r.confirmPromotionSurvives(rec, prop, canonical)
	case migrationPromotionLost:
		r.wedged(subject, migrationWedgeRemedy,
			"property %q was promoted onto %q and that directory is gone; preserving the record and promoting nothing.",
			prop, canonical)
		return rec, false, nil
	default:
	}
	stagedThere, err := r.dirExists(staged)
	if err != nil {
		return rec, false, err
	}
	if !stagedThere {
		return r.settleInterruptedPromotion(rec, prop, staged, canonical)
	}

	// Dormant here: every flip this build writes displaces the canonical name
	// itself, and the cutover PR is what writes a different one.
	if displaced != "" && displaced != canonical {
		if cleared, err := r.clearForPromotion(subject, displaced, "the displaced directory"); err != nil || !cleared {
			return rec, false, err
		}
	}
	if cleared, err := r.clearForPromotion(subject, canonical, "the canonical directory"); err != nil || !cleared {
		return rec, false, err
	}

	started := rec.WithPromotionAt(prop, migrationPromotionStarted)
	if err := r.store.Put(started); err != nil {
		return rec, false, fmt.Errorf(
			"record the promotion of property %q before renaming %q onto %q: %w", prop, staged, canonical, err)
	}
	rec = started

	if err := r.rename(staged, canonical); err != nil {
		return r.abandonPromotion(rec, prop, staged), false, err
	}

	finished := rec.WithPromotionAt(prop, migrationPromotionFinished)
	if err := r.store.Put(finished); err != nil {
		r.logger.WithField("record", subject.Key.String()).Errorf(
			"record the finished promotion of property %q: %v", prop, err)
		return rec, true, nil
	}
	return finished, true, nil
}

func (r *migrationReconciler) clearForPromotion(subject MigrationSubject, dir, what string) (cleared bool, err error) {
	there, err := r.dirExists(dir)
	if err != nil {
		return false, err
	}
	if !there {
		return true, nil
	}
	// The directory is still there, so the flip never moved the pointer off it
	// and it is the copy the shard has been writing to. A boot that could not
	// arm the mirror means the staged copy missed those writes.
	if subject.Unmirrored {
		r.wedged(subject, migrationWedgeRemedy,
			"cannot promote: %s %q still holds this property's data and a boot took writes into it with "+
				"no double-write mirror armed, so the staged copy is behind it; promoting nothing.",
			what, dir)
		return false, nil
	}
	if err := r.removeDir(r.lsmPath, dir, what+" the promotion replaces"); err != nil {
		return false, err
	}
	return true, nil
}

func (r *migrationReconciler) confirmPromotionSurvives(rec MigrationRecordSwapped,
	prop, canonical string,
) (MigrationRecordSwapped, bool, error) {
	there, err := r.dirExists(canonical)
	if err != nil {
		return rec, false, err
	}
	if there {
		return rec, true, nil
	}
	r.wedged(rec.Subject(), migrationWedgeRemedy,
		"property %q was promoted onto %q and that directory is gone, so the data this migration renamed onto it is gone; "+
			"preserving the record and promoting nothing.", prop, canonical)
	lost := rec.WithPromotionAt(prop, migrationPromotionLost)
	if err := r.store.Put(lost); err != nil {
		r.logger.WithField("record", rec.Subject().Key.String()).Errorf(
			"record that the promoted directory of property %q is gone: %v", prop, err)
		return rec, false, nil
	}
	return lost, false, nil
}

func (r *migrationReconciler) settleInterruptedPromotion(rec MigrationRecordSwapped,
	prop, staged, canonical string,
) (MigrationRecordSwapped, bool, error) {
	subject := rec.Subject()
	if rec.PromotionOf(prop) != migrationPromotionStarted {
		r.wedged(subject, migrationWedgeRemedy,
			"property %q lost its staged directory %q to something that is not its promotion, which never started; "+
				"preserving the record and promoting nothing.", prop, staged)
		return rec, false, nil
	}
	canonicalThere, err := r.dirExists(canonical)
	if err != nil {
		return rec, false, err
	}
	if !canonicalThere {
		r.wedged(subject, migrationWedgeRemedy,
			"property %q has neither its staged directory %q nor its canonical directory %q, so its rename never ran; "+
				"preserving the record and promoting nothing.", prop, staged, canonical)
		abandoned := rec.WithPromotionAbandoned(prop)
		if err := r.store.Put(abandoned); err != nil {
			r.logger.WithField("record", subject.Key.String()).Errorf(
				"take back the started promotion of property %q whose rename never ran: %v", prop, err)
			return rec, false, nil
		}
		return abandoned, false, nil
	}
	finished := rec.WithPromotionAt(prop, migrationPromotionFinished)
	if err := r.store.Put(finished); err != nil {
		return rec, false, fmt.Errorf("record the finished promotion of property %q: %w", prop, err)
	}
	return finished, true, nil
}

// [diskio.RenameAndSync] renames before it syncs, so a sync error can follow a
// rename that already ran. Taking the mark back then makes every later pass
// read the promoted canonical directory as unpromoted.
func (r *migrationReconciler) abandonPromotion(rec MigrationRecordSwapped, prop, staged string) MigrationRecordSwapped {
	stagedThere, err := r.dirExists(staged)
	if err != nil {
		r.logger.WithField("record", rec.Subject().Key.String()).Errorf(
			"cannot tell whether the staged directory %q of property %q survived its failed rename, "+
				"so its started promotion mark is left standing: %v", staged, prop, err)
		return rec
	}
	if !stagedThere {
		return rec
	}
	abandoned := rec.WithPromotionAbandoned(prop)
	if err := r.store.Put(abandoned); err != nil {
		r.logger.WithField("record", rec.Subject().Key.String()).Errorf(
			"take back the started promotion of property %q after its rename failed: %v", prop, err)
		return rec
	}
	return abandoned
}

// One value, not three strings: two of these reach os.RemoveAll and the
// third holds the only copy, so swapping any two at a call site deletes data.
type promotionDirs struct {
	staged    string
	canonical string
	displaced string
}

func (r *migrationReconciler) reconcilePromoted(ctx context.Context, rec MigrationRecordPromoted,
	someUnreadable bool,
) error {
	if someUnreadable {
		return nil
	}
	return r.withSealedUnit(rec.Subject(), "its closure sweep", func() error {
		return r.reconcilePromotedSealed(ctx, rec)
	})
}

func (r *migrationReconciler) reconcilePromotedSealed(ctx context.Context, rec MigrationRecordPromoted) error {
	subject := rec.Subject()
	all := r.store.Records()

	if err := r.repromoteWhatTheRecordOutran(ctx, all, subject); err != nil {
		return err
	}

	remaining := r.reclaimOwnedDirs(ctx, subject)
	if len(remaining) > 0 {
		r.logger.WithField("record", subject.Key.String()).Warnf(
			"%d directory/directories of a promoted migration could not be reclaimed yet", len(remaining))
		return nil
	}

	class := r.deps.Class()
	if class == nil {
		r.logger.WithField("record", subject.Key.String()).Warn(
			"a promoted migration's collection is not in the schema, so nothing here can confirm its effect; keeping the record")
		return nil
	}
	if effect, missing := migrationEffectStatus(class, subject); effect == migrationEffectPending {
		r.logger.WithField("record", subject.Key.String()).WithField("properties_pending", len(missing)).Warn(
			"this shard promoted a migration whose effect is not in the schema; keeping the record, " +
				"which is what answers for the property until the effect lands")
		return nil
	}
	if older, sole := migrationSoleSupersessorOf(all, subject); sole {
		r.logger.WithField("record", subject.Key.String()).Warnf(
			"keeping this promoted migration's record: it is the only thing superseding record %s, "+
				"and removing it would let the next load promote that record's older staged data over "+
				"the directory this one's data lives in. The next pass retires that record first", older)
		return nil
	}
	if err := r.removeTrackerDir(subject); err != nil {
		return err
	}
	return r.store.Remove(subject.Key)
}

func (r *migrationReconciler) repromoteWhatTheRecordOutran(ctx context.Context, all []MigrationRecord,
	subject MigrationSubject,
) error {
	var ambiguous, repromoted []string
	// One line per record, not one per property: this runs on every load of a
	// promoted record, and one interrupted pass leaves every property behind.
	// Deferred, so a rename that fails partway still reports what it ran.
	defer func() {
		if len(repromoted) == 0 {
			return
		}
		r.logger.WithField("record", subject.Key.String()).
			WithField("property_count", len(subject.Props)).
			Errorf("%d propert(y/ies) recorded as promoted still hold their data at the staged name: %s; "+
				"re-running the promotion", len(repromoted), strings.Join(migrationReportedNames(repromoted), ", "))
	}()

	for _, prop := range subject.Properties() {
		if err := ctx.Err(); err != nil {
			return err
		}
		staged, canonical := subject.Props[prop].Staged, subject.Props[prop].Canonical
		if staged == "" || canonical == "" {
			continue
		}
		if migrationPropertySuperseded(all, subject, prop) {
			continue
		}
		stagedThere, err := r.dirExists(staged)
		if err != nil {
			return err
		}
		if !stagedThere {
			continue
		}
		canonicalThere, err := r.dirExists(canonical)
		if err != nil {
			return err
		}
		if canonicalThere {
			ambiguous = append(ambiguous, prop)
			continue
		}

		repromoted = append(repromoted, prop)
		if err := r.rename(staged, canonical); err != nil {
			return err
		}
	}
	if len(ambiguous) > 0 {
		r.wedged(subject, migrationWedgeRemedy,
			"%d propert(y/ies) recorded as promoted hold a directory at both their staged and canonical names: %s; "+
				"nothing here can tell which one the promotion produced, so the record and both directories are preserved.",
			len(ambiguous), strings.Join(migrationReportedNames(ambiguous), ", "))
		return fmt.Errorf(
			"%d property/properties hold a directory at both their staged and canonical names: %s; "+
				"nothing here can tell which one the promotion produced, so the record and both directories are preserved",
			len(ambiguous), strings.Join(migrationReportedNames(ambiguous), ", "))
	}
	return nil
}

const migrationTaskMapUnreadable = "this node's task map cannot be read yet"

// Not installed and installed-but-not-applied-yet read the same here: neither
// licenses a decision.
func (r *migrationReconciler) localTasks() ([]*distributedtask.Task, bool) {
	if r.deps.LocalTasks == nil {
		return nil, false
	}
	return r.deps.LocalTasks()
}

func (r *migrationReconciler) localVerdict(subject MigrationSubject) (migrationVerdict, string) {
	tasks, readable := r.localTasks()
	if !readable {
		return migrationVerdictLeave, migrationTaskMapUnreadable
	}
	return r.verdictFrom(subject, tasks, taskListMayLag)
}

func (r *migrationReconciler) clusterVerdict(subject MigrationSubject, tasks []*distributedtask.Task) (migrationVerdict, string) {
	local, readable := r.localTasks()
	if !readable {
		return migrationVerdictLeave, migrationTaskMapUnreadable
	}
	if task := findMigrationTask(subject, local); task != nil {
		return migrationVerdictForTask(task)
	}
	return r.verdictFrom(subject, tasks, taskListIsComplete)
}

func (r *migrationReconciler) sealUnit(subject MigrationSubject) (func(), bool) {
	if r.deps.SealUnit == nil {
		return func() {}, false
	}
	return r.deps.SealUnit(
		distributedtask.TaskDescriptor{ID: subject.TaskID, Version: subject.Key.TaskVersion},
		subject.Key.UnitID)
}

// A live worker keeps writing through pointers taken before its phase
// began, into what the teardown would remove; decline and retry next pass.
// The per-unit orphan audit takes the same seal separately.
func (r *migrationReconciler) withSealedUnit(subject MigrationSubject, what string, run func() error) error {
	release, sealed := r.sealUnit(subject)
	if !sealed {
		r.logger.WithField("record", subject.Key.String()).Infof(
			"a local unit of this migration is still running, so %s waits for the next pass", what)
		return nil
	}
	defer release()
	return run()
}

type taskListCompleteness bool

const (
	taskListMayLag     taskListCompleteness = false
	taskListIsComplete taskListCompleteness = true
)

// verdictFrom consults the two external facts, in an order that skips the
// second whenever the first is conclusive.
func (r *migrationReconciler) verdictFrom(subject MigrationSubject, tasks []*distributedtask.Task,
	completeness taskListCompleteness,
) (migrationVerdict, string) {
	if task := findMigrationTask(subject, tasks); task != nil {
		return migrationVerdictForTask(task)
	}

	class := r.deps.Class()
	if class == nil {
		return migrationVerdictLeave, "collection is not in the locally applied schema"
	}
	if migrationEffectConfirmsCommit(class, subject) {
		return migrationVerdictCommit, "owning task is gone and the schema shows its effect"
	}
	if migrationEffectIsNeverObservable(subject.MigrationType) {
		why := fmt.Sprintf("the schema never shows the effect of a %s migration, "+
			"so it can neither confirm nor deny this one", subject.MigrationType)
		if completeness == taskListMayLag {
			return migrationVerdictLeave, why
		}
		// Every later pass reads this same answer, so leaving the record
		// standing costs a leader query and a shard walk a minute, forever.
		return migrationVerdictWedge, why + ", and the leader's list no longer holds the owning task"
	}
	if effect, _ := migrationEffectStatus(class, subject); effect == migrationEffectUnobservable {
		return migrationVerdictLeave, "the locally applied schema does not hold every property this record names yet"
	}
	if completeness == taskListMayLag {
		return migrationVerdictLeave, "this node can see neither the owning task nor its effect, which a node still applying its log cannot tell from a task that is gone"
	}
	return migrationVerdictDiscard, "owning task is gone and the schema does not show its effect"
}

func migrationVerdictForTask(task *distributedtask.Task) (migrationVerdict, string) {
	switch task.Status {
	case distributedtask.TaskStatusFinished:
		return migrationVerdictCommit, "owning task finished"
	case distributedtask.TaskStatusCancelled, distributedtask.TaskStatusFailed:
		return migrationVerdictDiscard, fmt.Sprintf("owning task %s", task.Status)
	default:
		// Active, or a status this build does not recognize.
		return migrationVerdictLeave, fmt.Sprintf("owning task is %s", task.Status)
	}
}

func findMigrationTask(subject MigrationSubject, tasks []*distributedtask.Task) *distributedtask.Task {
	for _, task := range tasks {
		if task != nil && task.ID == subject.TaskID && task.Version == subject.Key.TaskVersion {
			return task
		}
	}
	return nil
}

// Can't wait for a live worker instead: shard load runs on the RAFT apply
// loop, and waiting there would stall the whole cluster.
func (r *migrationReconciler) discard(ctx context.Context, subject MigrationSubject, why string) error {
	return r.withSealedUnit(subject, "the discard of its staged data", func() error {
		return r.discardSealed(ctx, subject, why)
	})
}

func (r *migrationReconciler) discardSealed(ctx context.Context, subject MigrationSubject, why string) error {
	r.logger.WithField("record", subject.Key.String()).Infof("discarding staged migration data: %s", why)
	return r.reclaimRecordAndDirs(ctx, subject)
}

// The record answers for every directory it names, so it goes last of all.
//
// Disarms every property's mirror first: the directories go next, and the one
// removed is exactly where a still-armed mirror sends its next copy, whose
// failure fails the user's write with it.
func (r *migrationReconciler) reclaimRecordAndDirs(ctx context.Context, subject MigrationSubject) error {
	if r.deps.Mirror != nil {
		for _, prop := range subject.Properties() {
			r.deps.Mirror.DisarmMigrationMirror(subject.Key, prop)
		}
	}
	if remaining := r.reclaimOwnedDirs(ctx, subject); len(remaining) > 0 {
		return fmt.Errorf("%d owned directory/directories survived", len(remaining))
	}
	if err := r.removeTrackerDir(subject); err != nil {
		return err
	}
	return r.store.Remove(subject.Key)
}

func (r *migrationReconciler) closeStagedBuckets(ctx context.Context, dirs ...string) error {
	if r.deps.Buckets == nil {
		return nil
	}
	if err := r.deps.Buckets.ShutdownStagedBucketsAt(ctx, dirs); err != nil {
		return fmt.Errorf("shut down the staged buckets at %s: %w",
			strings.Join(migrationReportedNames(dirs), ", "), err)
	}
	return nil
}

// Reports a failed removal, which leaves a directory nothing else attributes:
// the caller must keep its record so the next load retries.
func (r *migrationReconciler) removeTrackerDir(subject MigrationSubject) error {
	if err := r.removeDir(r.migrationsPath(), subject.TrackerDir, "the migration's tracker directory"); err != nil {
		r.logger.WithField("dir", subject.TrackerDir).Errorf("%v", err)
		return err
	}
	return nil
}

// A directory a later-versioned record claims as displaced is that claimer's
// only copy; removing it here loses the successor's data.
// One line per record, not one per directory: a single filesystem fault fails
// the removal and the confirming stat of every directory the record names.
func (r *migrationReconciler) reclaimOwnedDirs(ctx context.Context,
	subject MigrationSubject,
) []string {
	var remaining []string
	owned := migrationOwnedDirs(subject)
	all := r.store.Records()
	reclaiming := errorcompounder.New()
	for _, dir := range owned {
		if err := ctx.Err(); err != nil {
			reclaiming.Add(err)
			remaining = append(remaining, dir)
			break
		}
		if migrationDirClaimedAsDisplaced(all, subject, dir) {
			continue
		}
		if err := r.closeStagedBuckets(ctx, dir); err != nil {
			reclaiming.Add(err)
			remaining = append(remaining, dir)
			continue
		}
		reclaiming.Add(r.removeDir(r.lsmPath, dir, "a migration directory"))
		there, err := r.dirExists(dir)
		if err != nil {
			reclaiming.AddWrapf(err, "confirm migration directory %q is gone", dir)
		}
		if there || err != nil {
			remaining = append(remaining, dir)
		}
	}
	if err := reclaiming.ToErrorLimited(maxReportedErrors); err != nil {
		r.logger.WithField("record", subject.Key.String()).
			WithField("directory_count", len(owned)).
			Errorf("reclaim the directories of a migration: %v", err)
	}
	return remaining
}

func migrationOwnedDirs(subject MigrationSubject) []string {
	dirs := make([]string, 0, 2*len(subject.Props))
	for _, prop := range subject.Properties() {
		dirs = append(dirs, migrationOwnCopyDirs(subject, prop)...)
	}
	return dirs
}

// Refused here, not at each caller: joining an empty or escaping handle
// onto root resolves to root itself, and callers remove or rename the result.
func (r *migrationReconciler) path(root, dir, what string) (string, error) {
	if !migrationHandleIsOneElement(dir) {
		return "", fmt.Errorf("refusing to act on %s %q: it does not name a single directory under %q",
			what, dir, root)
	}
	return filepath.Join(root, dir), nil
}

func (r *migrationReconciler) removeDir(root, dir, what string) error {
	if dir == "" {
		return nil
	}
	path, err := r.path(root, dir, what)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove %s %q: %w", what, dir, err)
	}
	return nil
}

func (r *migrationReconciler) migrationsPath() string {
	return filepath.Join(r.lsmPath, migrationsDir)
}

func (r *migrationReconciler) dirExists(dir string) (bool, error) {
	if dir == "" {
		return false, nil
	}
	path, err := r.path(r.lsmPath, dir, "a recorded directory")
	if err != nil {
		return false, err
	}
	// Any stat failure besides ENOENT must stop the caller, or a promotion
	// probe could take "cannot see it" as proof a rename already ran.
	there, err := diskio.DirExists(path)
	if err != nil {
		return false, fmt.Errorf("stat migration directory %q: %w", path, err)
	}
	return there, nil
}

// The Promoted record written on this rename's strength is durable, so the
// rename must be too, or a crash leaves it naming a path that was never made.
func (r *migrationReconciler) rename(from, to string) error {
	fromPath, err := r.path(r.lsmPath, from, "the directory to promote")
	if err != nil {
		return err
	}
	toPath, err := r.path(r.lsmPath, to, "the name to promote onto")
	if err != nil {
		return err
	}
	if err := diskio.RenameAndSync(fromPath, toPath); err != nil {
		return fmt.Errorf("promote %q to %q: %w", from, to, err)
	}
	return nil
}
