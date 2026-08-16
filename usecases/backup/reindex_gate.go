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

// Any refusal in the chain; onlyReindexRefusals in handler.go is the stricter form.
func isReindexRefusal(err error) bool {
	return errors.Is(err, backup.ErrReindexInFlight) ||
		errors.Is(err, backup.ErrBackupBlockedByInFlightReindex) ||
		errors.Is(err, backup.ErrReindexActivityUndetermined)
}

// Ranks concurrent refusals so the reported one is deterministic: an observed
// migration tells the operator what to wait for, an undetermined one does not.
func refusalRank(err error) int {
	if errors.Is(err, backup.ErrReindexActivityUndetermined) {
		return 0
	}
	return 1
}

// Only the operation context marks an operator abort; a refusal whose cause wraps a RAFT-client cancel must not be reported as one.
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

// Only the refusal kind survives the hop, so a multi-class request names no individual collection: the participant never said which one is migrating.
func blockedSubject(classes []string) string {
	if len(classes) == 1 {
		return quoteClassList(classes)
	}
	return "at least one of " + quoteClassList(classes)
}

func restoreUndeterminedByParticipant() error {
	return fmt.Errorf("%w; retry once the cluster is reachable",
		backup.ErrReindexActivityUndetermined)
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
