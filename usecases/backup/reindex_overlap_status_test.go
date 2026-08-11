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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// Pins: an operator abort during the overlap lookup must publish as CANCELLED,
// not FAILED for a reindex that never happened.
func TestAbortDuringOverlapCheckReportsCancelledNotAReindex(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())
	sourcer.reindexOverlapFn = func(context.Context) error {
		// The abort lands while the lookup's RAFT query is in flight.
		cancel()
		return fmt.Errorf("cannot rule out a runtime-reindex during this backup: "+
			"the cluster task manager could not be queried: %w", context.Canceled)
	}

	slot := &fakeStatusSlot{}

	err := runOverlapBackup(ctx, backend, sourcer, slot, backupID, nil, time.Now().UTC())
	require.ErrorIs(t, err, context.Canceled)

	storedStatus, storedReason := backend.getMetaStatus()
	require.Equal(t, backup.Cancelled, storedStatus)
	require.NotContains(t, storedReason, "reindex",
		"an aborted backup must not be blamed on a migration that never ran")

	require.NotContains(t, slot.statuses, backup.Failed,
		"the status API must not report FAILED for a backup the operator aborted")
	require.Equal(t, backup.Cancelled, slot.last(),
		"the last status an operator can poll has to be the cancellation they caused")
}

// Pins: the refusal must still flip the slot to FAILED even when the meta
// write itself fails, so it never stays stuck at Transferring.
func TestOverlapRefusalStaysTerminalWhenTheMetaWriteFails(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).
		Return(errors.New("object storage unreachable"))

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())
	sourcer.reindexOverlapErr = fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
		backup.ErrBackupSpannedReindex, "Movies")

	// Real slot on a real backupper, not a stub, to exercise what OnStatus serves.
	bp := &backupper{}
	slot := &bp.lastOp
	prevID, _ := slot.renew(backupID, "bucket/backups/1", "", "")
	require.Empty(t, prevID)

	err := runOverlapBackup(context.Background(), backend, sourcer, slot, backupID, nil, time.Now().UTC())
	require.ErrorIs(t, err, backup.ErrBackupSpannedReindex)
	require.Contains(t, err.Error(), "a runtime-reindex overlapped this backup",
		"with the descriptor unwritten, the returned error is the only place the reason survives")
	require.Contains(t, err.Error(), "object storage unreachable",
		"the meta write failure has to stay visible next to the refusal")

	st := slot.get()
	require.Equal(t, backup.Failed, st.Status,
		"a refusal left at Transferring reads as a backup that is still running")
	require.Contains(t, st.Err, "a runtime-reindex overlapped this backup",
		"the reason has to name the refusal, not the storage error that hid it")

	// And the wire response a polling coordinator reads carries the same text.
	handler := &Handler{backupper: bp}
	res := handler.OnStatus(context.Background(), &StatusRequest{Method: OpCreate, ID: backupID})
	require.Equal(t, backup.Failed, res.Status)
	require.Contains(t, res.Err, "a runtime-reindex overlapped this backup",
		"the reason must survive the hop to the coordinator, which latches this answer")
	// TestHandlerOnStatusServesTheReasonAfterTheCreateGoroutineExits covers the
	// post-release poll.
}

// undeterminedOverlapErr is a refusal that never observed an overlap: both
// sentinels reachable via errors.Is, neither text printed.
type undeterminedOverlapErr struct{ cause error }

func (e undeterminedOverlapErr) Error() string {
	return "cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried"
}

func (e undeterminedOverlapErr) Unwrap() []error {
	return []error{backup.ErrBackupSpannedReindex, backup.ErrReindexOverlapUndetermined, e.cause}
}

// Pins the two rules TestBackupStatRemembersAFailureBeyondTheSlot does not
// reach: a poll that names no backup at all matches nothing, and another
// backup starting does not erase the failed one's reason. The stand-in reason
// and the cancelled-slot rule are pinned by TestBackupStatPublishIfOwned.
func TestRememberedFailureAnswersOnlyTheBackupThatFailed(t *testing.T) {
	const (
		failedID = "backup-a"
		otherID  = "backup-b"
		reason   = "a runtime-reindex overlapped this backup"
	)

	t.Run("an empty id matches nothing, including a slot that never failed", func(t *testing.T) {
		var slot backupStat
		prevID, _ := slot.renew(failedID, "bucket/backups/a", "", "")
		require.Empty(t, prevID)

		gotReason, gotFound := slot.rememberedFailure("")
		require.False(t, gotFound)
		require.Empty(t, gotReason)
	})

	t.Run("a different backup starting does not erase the failed one's reason", func(t *testing.T) {
		var slot backupStat
		prevID, _ := slot.renew(failedID, "bucket/backups/a", "", "")
		require.Empty(t, prevID)
		slot.setFailed(reason)
		slot.reset()
		prevID, _ = slot.renew(otherID, "bucket/backups/b", "", "")
		require.Empty(t, prevID)

		gotReason, gotFound := slot.rememberedFailure(failedID)
		require.True(t, gotFound)
		require.Equal(t, reason, gotReason)
	})
}

// runOverlapBackup drives one commit through the uploader with the given
// backend, sourcer and status slot.
func runOverlapBackup(ctx context.Context, backend *fakeBackend, sourcer *fakeSourcer,
	slot statusPublisher, backupID string, classes []string, startedAt time.Time,
) error {
	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, slot, logger)
	desc := backup.BackupDescriptor{ID: backupID, StartedAt: startedAt}
	return uploader.all(ctx, classes, &desc, nil, "", "")
}

// TestOverlapRefusalKeepsItsSentinelsThroughTheUploader pins what the two
// overlap sentinels are for: telling an operator whether a migration was seen
// or merely could not be ruled out. Both fail the backup, so only errors.Is on
// the way out separates "go look at the task that ran" from "there may be no
// task to find".
func TestOverlapRefusalKeepsItsSentinelsThroughTheUploader(t *testing.T) {
	const backupID = "1"
	any := mock.Anything
	classes := []string{"Movies"}
	// Far enough back that "now" cannot be mistaken for it.
	startedAt := time.Now().UTC().Add(-time.Hour)

	tests := []struct {
		name string
		// refusal is what the overlap check answers with.
		refusal   error
		wantIs    []error
		wantNotIs []error
		// wantStoredHint and wantNotStoredHint are what the stored reason has
		// to say, and what it must not claim.
		wantStoredHint    string
		wantNotStoredHint string
	}{
		{
			name:      "no overlap",
			wantNotIs: []error{backup.ErrBackupSpannedReindex, backup.ErrReindexOverlapUndetermined},
		},
		{
			name: "an overlap the check observed",
			refusal: fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
				backup.ErrBackupSpannedReindex, "Movies"),
			wantIs:         []error{backup.ErrBackupSpannedReindex},
			wantNotIs:      []error{backup.ErrReindexOverlapUndetermined},
			wantStoredHint: "was migrated while this backup was being captured",
		},
		{
			// The cause is a cancellation from the RAFT client, not from this
			// backup's own context: published as CANCELLED it would read as an
			// operator abort that never happened.
			name:              "an overlap the check could not rule out, carrying a foreign cancellation",
			refusal:           undeterminedOverlapErr{cause: context.Canceled},
			wantIs:            []error{backup.ErrBackupSpannedReindex, backup.ErrReindexOverlapUndetermined},
			wantStoredHint:    "cannot rule out a runtime-reindex during this backup",
			wantNotStoredHint: "overlapped this backup",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)
			sourcer := &fakeSourcer{}
			sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())
			sourcer.reindexOverlapErr = tc.refusal

			slot := &fakeStatusSlot{}
			err := runOverlapBackup(context.Background(), backend, sourcer, slot,
				backupID, classes, startedAt)

			// The answer alone says nothing about which window the check was asked
			// about, and asking about the commit instant would read a migration
			// inside the capture window as clean.
			askedClasses, askedSince, calls := sourcer.lastOverlapQuery()
			require.Equal(t, 1, calls, "every commit has to consult the overlap check exactly once")
			require.Equal(t, startedAt, askedSince,
				"the check must be asked about the capture start, not the commit instant")
			require.Equal(t, classes, askedClasses,
				"the check must be asked about the classes this backup captured")

			for _, sentinel := range tc.wantIs {
				require.ErrorIs(t, err, sentinel)
			}
			for _, sentinel := range tc.wantNotIs {
				require.NotErrorIs(t, err, sentinel)
			}
			storedStatus, storedReason := backend.getMetaStatus()
			if tc.refusal == nil {
				require.NoError(t, err)
				require.Equal(t, backup.Success, storedStatus)
				return
			}
			require.Equal(t, backup.Failed, storedStatus,
				"a refusal published as CANCELLED reads as an operator abort that never happened")
			require.Contains(t, storedReason, tc.wantStoredHint,
				"the stored reason has to say which of the two refusals this was")
			if tc.wantNotStoredHint != "" {
				require.NotContains(t, storedReason, tc.wantNotStoredHint,
					"the check never got an answer, so it must not report a migration it did not see")
			}
			require.NotContains(t, slot.statuses, backup.Cancelled, "nobody cancelled this backup")
			require.Equal(t, backup.Failed, slot.last(),
				"the last status an operator can poll has to be the failure")
		})
	}
}
