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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// unknownStateRefusal stands in for adapters/repos/db's refusal for an
// unreachable leader, which this package cannot construct: its text never
// renders the sentinel, and the sentinel is reachable only through
// Unwrap. Both are held to [backup.CauseFirstRefusal], so the stand-in
// cannot drift into a shape the real one does not have.
type unknownStateRefusal struct{ cause error }

var _ backup.CauseFirstRefusal = unknownStateRefusal{}

func (e unknownStateRefusal) Error() string {
	return fmt.Sprintf("backup blocked: the cluster leader could not be reached, "+
		"so runtime-reindex state is unknown for every shard on this node: %v", e.cause)
}

func (e unknownStateRefusal) Unwrap() []error {
	return []error{backup.ErrBackupBlockedByInFlightReindex, e.cause}
}

// TestReindexRefusalSurvivesTheCanCommitRPC pins that a refusal whose
// message does not contain the sentinel still classifies as a reindex
// block. The participant decides the kind with errors.Is, the kind is
// what crosses the wire, and the coordinator rebuilds the error from it —
// so a refusal that stopped matching would come back as a generic
// cannot-commit instead.
func TestReindexRefusalSurvivesTheCanCommitRPC(t *testing.T) {
	tests := []struct {
		name    string
		refusal error
	}{
		{
			name:    "unreachable leader, sentinel only under Unwrap",
			refusal: unknownStateRefusal{cause: errors.New("list DTM tasks: leader not found")},
		},
		{
			name: "genuine reindex, sentinel wrapped in the text",
			refusal: fmt.Errorf("%w: shard %q (collection %q) has an active runtime-reindex task in DTM",
				backup.ErrBackupBlockedByInFlightReindex, "shard-7", "MyClass"),
		},
		{
			name: "many shards joined",
			refusal: errors.Join(
				fmt.Errorf("%w: shard-1", backup.ErrBackupBlockedByInFlightReindex),
				fmt.Errorf("%w: shard-2", backup.ErrBackupBlockedByInFlightReindex),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind := classifyCanCommitErr(tt.refusal)
			require.Equal(t, CanCommitErrInFlightReindex, kind,
				"a reindex refusal must not degrade to a generic cannot-commit")

			// What the participant actually puts on the wire.
			resp := &CanCommitResponse{Err: tt.refusal.Error(), ErrKind: kind}
			rebuilt := canCommitErrFromResponse(resp)

			require.True(t, errors.Is(rebuilt, backup.ErrBackupBlockedByInFlightReindex),
				"the coordinator's error must still match the sentinel")
			require.Contains(t, rebuilt.Error(), tt.refusal.Error(),
				"the participant's own words must reach the caller")
		})
	}
}

// TestClassifyCanCommitErr_UnrelatedErrorStaysGeneric guards the other
// direction: the classification must not widen to everything.
func TestClassifyCanCommitErr_UnrelatedErrorStaysGeneric(t *testing.T) {
	require.Equal(t, CanCommitErrCannotCommit,
		classifyCanCommitErr(errors.New("disk full")))
}

// TestReindexRefusalRebuildKeepsTheParticipantsFirstLine pins how the
// coordinator rebuilds a participant refusal. The genuine text keeps the
// prefix it has always had; the unknown-state text is not given a first
// line that contradicts its second.
func TestReindexRefusalRebuildKeepsTheParticipantsFirstLine(t *testing.T) {
	sentinel := backup.ErrBackupBlockedByInFlightReindex.Error()

	genuine := fmt.Sprintf("node1/MyClass: %s: shard %q (collection %q) has an active runtime-reindex task in DTM",
		sentinel, "shard-7", "MyClass")
	unknown := "backup blocked: the cluster leader could not be reached, so runtime-reindex " +
		"state is unknown for every shard on this node: list DTM tasks: leader not found"

	t.Run("genuine keeps the prefix byte for byte", func(t *testing.T) {
		rebuilt := canCommitErrFromResponse(&CanCommitResponse{
			Err: genuine, ErrKind: CanCommitErrInFlightReindex,
		})
		require.Equal(t, sentinel+": "+genuine, rebuilt.Error())
		require.True(t, errors.Is(rebuilt, backup.ErrBackupBlockedByInFlightReindex))
	})

	t.Run("unknown state is not prefixed with a claim it denies", func(t *testing.T) {
		rebuilt := canCommitErrFromResponse(&CanCommitResponse{
			Err: unknown, ErrKind: CanCommitErrInFlightReindex,
		})
		require.False(t, strings.HasPrefix(rebuilt.Error(), sentinel),
			"the degraded path must not reintroduce the claim that a reindex is in flight")
		require.NotContains(t, rebuilt.Error(), sentinel)
		require.Equal(t, unknown, rebuilt.Error(), "the participant's words reach the caller unchanged")
		require.True(t, errors.Is(rebuilt, backup.ErrBackupBlockedByInFlightReindex),
			"matching must survive the rebuild")
	})
}
