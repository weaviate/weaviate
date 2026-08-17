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

// Every sentinel a reindex gate refuses with. One list, so the any-member and all-members
// classifiers below cannot disagree about what counts as a refusal.
var reindexGateSentinels = []error{
	backup.ErrReindexInFlight,
	backup.ErrBackupBlockedByInFlightReindex,
	backup.ErrReindexActivityUndetermined,
	backup.ErrBackupReindexActivityUndetermined,
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
// refusal joined with a permanent failure is never answered as retryable. It walks the
// join itself and compares each leaf by value: errors.Is at a wrapper descends into a
// join underneath it and answers "any member", and one fmt.Errorf is enough to hit that.
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
func refusalRank(err error) int {
	if errors.Is(err, backup.ErrReindexActivityUndetermined) ||
		errors.Is(err, backup.ErrBackupReindexActivityUndetermined) {
		return 0
	}
	return 1
}

// A refusal whose cause wraps a RAFT-client cancel must not be reported as an operator abort; a cancellation carrying no refusal is one.
func publishAsCancelled(err, ctxErr error) bool {
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

// Only the refusal kind is rebuilt here; the participant's own text is discarded so a peer cannot dictate wording. A multi-class request therefore names no individual collection.
func blockedSubject(classes []string) string {
	if len(classes) == 1 {
		return quoteClassList(classes)
	}
	return "at least one of " + quoteClassList(classes)
}

// The sentinel is the caller's, because canCommitErrFromResponse already knows which
// operation it is rebuilding and the two texts differ in nothing else.
func undeterminedByParticipant(sentinel error) error {
	return fmt.Errorf("%w; retry once the cluster is reachable", sentinel)
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
