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
)

const maxNamedClasses = 5

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

// Never an operator abort, even when the cause wraps a RAFT-client cancel.
func publishAsCancelled(err, ctxErr error) bool {
	return !isReindexRefusal(err) &&
		(errors.Is(err, context.Canceled) || errors.Is(ctxErr, context.Canceled))
}

func backupRefusedByParticipant(classes []string) error {
	return fmt.Errorf(
		"%w: a runtime-reindex is in flight on %s; retry after the migration finishes",
		backup.ErrBackupBlockedByInFlightReindex, blockedSubject(classes))
}

func restoreRefusedByParticipant(classes []string) error {
	return fmt.Errorf(
		"restore blocked: %w: a runtime-reindex is in flight on %s; retry after the migration finishes",
		backup.ErrReindexInFlight, blockedSubject(classes))
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
