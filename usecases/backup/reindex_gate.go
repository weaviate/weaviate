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

package backup

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/reindex"
)

const maxNamedClasses = 5

// Every sentinel a reindex gate refuses with. One list, so the any-member and
// all-members classifiers below can never disagree about what a refusal is.
var reindexGateSentinels = []error{
	backup.ErrReindexInFlight,
	backup.ErrBackupBlockedByInFlightReindex,
	backup.ErrReindexActivityUndetermined,
	backup.ErrBackupReindexActivityUndetermined,
	backup.ErrReindexOverlapCheckUnanswerable,
}

// Any refusal in the chain; allReindexRefusals is the stricter form.
func isReindexRefusal(err error) bool {
	for _, sentinel := range reindexGateSentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// allReindexRefusals reports whether every member of err is a reindex-gate refusal, so a
// refusal joined with a permanent failure is never answered as retryable. It recurses
// structurally and tests sentinels shallowly: errors.Is at a wrapper descends into a join
// underneath it and answers "any member", and one fmt.Errorf is enough to hit that.
func allReindexRefusals(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		members := joined.Unwrap()
		for _, member := range members {
			if !allReindexRefusals(member) {
				return false
			}
		}
		return len(members) > 0
	}
	if slices.Contains(reindexGateSentinels, err) {
		return true
	}
	return allReindexRefusals(errors.Unwrap(err))
}

// Ranks concurrent refusals so the reported one is deterministic: an observed
// migration tells the operator what to wait for, an undetermined one does not.
// The configuration refusal outranks both: it is the only one that does not
// clear on its own.
func refusalRank(err error) int {
	if errors.Is(err, backup.ErrReindexActivityUndetermined) ||
		errors.Is(err, backup.ErrBackupReindexActivityUndetermined) {
		return 0
	}
	if errors.Is(err, backup.ErrReindexOverlapCheckUnanswerable) {
		return 2
	}
	return 1
}

// reasonSafeText rewords the one phrase a coordinator reads as an operator
// abort. The coordinator relabels a participant's FAILED to CANCELLED when
// context.Canceled's text appears anywhere in the published reason, and a
// CANCELLED backup id can be re-posted, so a reason that merely quotes a
// cancel would let a torn capture be silently overwritten by a clean one.
func reasonSafeText(text string) string {
	return strings.ReplaceAll(text, context.Canceled.Error(), "a canceled context")
}

// A commit-time overlap verdict keeps the id spent even under an operator
// abort: a capture exists and may be torn, and CANCELLED would make the id
// re-postable over it. Every other refusal loses to the abort, because the
// operation context is the abort signal itself.
func publishAsCancelled(err, ctxErr error) bool {
	if errors.Is(err, backup.ErrReindexOverlappedBackup) ||
		errors.Is(err, backup.ErrReindexOverlapUndetermined) {
		return false
	}
	return errors.Is(ctxErr, context.Canceled) ||
		(!isReindexRefusal(err) && errors.Is(err, context.Canceled))
}

func backupRefusedByParticipant(classes []string) error {
	return fmt.Errorf(
		"%w: runtime-reindex work is in progress on %s; retry after it finishes. %s",
		backup.ErrBackupBlockedByInFlightReindex, blockedSubject(classes), reindex.ClusterMigrationRemedy())
}

func restoreRefusedByParticipant(classes []string) error {
	return fmt.Errorf(
		"restore blocked: %w: runtime-reindex work is in progress on %s; retry after it finishes. %s",
		backup.ErrReindexInFlight, blockedSubject(classes), reindex.ClusterMigrationRemedy())
}

// Only the refusal kind survives the hop, so a multi-class request names no individual collection: the participant never said which one is migrating.
func blockedSubject(classes []string) string {
	if len(classes) == 1 {
		return quoteClassList(classes)
	}
	return "at least one of " + quoteClassList(classes)
}

func backupUndeterminedByParticipant() error {
	return fmt.Errorf("%w; retry once the cluster is reachable",
		backup.ErrBackupReindexActivityUndetermined)
}

func restoreUndeterminedByParticipant() error {
	return fmt.Errorf("%w; retry once the cluster is reachable",
		backup.ErrReindexActivityUndetermined)
}

// The refusing node's text is forwarded whole: it is a configuration answer,
// so rebuilding it from the requested classes would name collections that
// have nothing to do with the cause and drop the settings to change.
func overlapCheckUnanswerableByParticipant(text string) error {
	if text == "" {
		return backup.ErrReindexOverlapCheckUnanswerable
	}
	return backup.ReindexOverlapCheckError{Msg: backup.CancelSafeText(text)}
}

func quoteClassList(classes []string) string {
	if len(classes) == 0 {
		return "the collections being restored"
	}
	// Sorted: the caller's list arrives in map order, and the same restore must be refused with the same words on every retry.
	named := slices.Sorted(slices.Values(classes))
	if len(named) > maxNamedClasses {
		named = named[:maxNamedClasses]
	}
	quoted := make([]string, 0, len(named))
	for _, class := range named {
		quoted = append(quoted, fmt.Sprintf("%q", class))
	}
	out := strings.Join(quoted, ", ")
	if remaining := len(classes) - len(named); remaining > 0 {
		out = fmt.Sprintf("%s and %d more", out, remaining)
	}
	return out
}
