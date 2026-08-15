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

// Any refusal sentinel anywhere in the chain. onlyReindexRefusals in
// handler.go is the stricter form: every element of a join must refuse.
func isReindexRefusal(err error) bool {
	return errors.Is(err, backup.ErrReindexInFlight) ||
		errors.Is(err, backup.ErrBackupBlockedByInFlightReindex) ||
		errors.Is(err, backup.ErrReindexActivityUndetermined)
}

// refusalRank orders the refusals two nodes can report at once. A node that
// observed a migration tells the operator what to wait for; one that could
// not read the task list tells them nothing they can act on. Without this the
// answer is whichever goroutine reached the slot first.
func refusalRank(err error) int {
	if errors.Is(err, backup.ErrReindexActivityUndetermined) {
		return 0
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

// The kind does not say whether the peer saw a live task or a local
// cleanup hold, so the rebuilt words claim neither.
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

// The kind is all that survives the hop, so the participant reported that
// something in the request is migrating, never which one. Naming a list
// asserts a migration on each of them.
func blockedSubject(classes []string) string {
	if len(classes) == 1 {
		return quoteClassList(classes)
	}
	return "at least one of " + quoteClassList(classes)
}

// Nothing was observed, so the rebuilt message names no collection and
// promises no migration will end.
func restoreUndeterminedByParticipant() error {
	return fmt.Errorf("%w; retry once the cluster is reachable",
		backup.ErrReindexActivityUndetermined)
}

func quoteClassList(classes []string) string {
	if len(classes) == 0 {
		return "the collections being restored"
	}
	// Sorted so the same restore is refused with the same words on every
	// retry: the caller's list arrives in map order.
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
