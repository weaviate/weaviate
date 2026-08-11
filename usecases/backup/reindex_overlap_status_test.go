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

// Pins: a status poll must never observe FAILED with an empty reason, since
// the status and the reason are written by two separate steps.
func TestOverlapRefusalPublishesTheReasonBeforeFailedBecomesObservable(t *testing.T) {
	const backupID, wantReason = "1", "a runtime-reindex overlapped this backup"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)
	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())
	sourcer.reindexOverlapErr = fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
		backup.ErrBackupSpannedReindex, "Movies")

	// What an observer polling at the instant of the FAILED transition reads.
	var sawFailed bool
	slot := &fakeStatusSlot{onChange: func(st backup.Status) {
		if st != backup.Failed {
			return
		}
		sawFailed = true
		_, reason := backend.getMetaStatus()
		require.Contains(t, reason, wantReason, "a status poll in this window reads FAILED without a reason")
	}}

	require.ErrorIs(t, runOverlapBackup(context.Background(), backend, sourcer, slot, backupID, nil,
		time.Now().UTC()), backup.ErrBackupSpannedReindex)
	storedStatus, storedReason := backend.getMetaStatus()
	require.Equal(t, backup.Failed, storedStatus)
	require.Contains(t, storedReason, wantReason)
	require.True(t, sawFailed, "the refused backup has to end up observably FAILED")
}

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
	// TestRememberedFailureSurvivesTheProductionSlotRelease covers the post-release poll.
}

// Pins: a foreign context.Canceled from the RAFT client (not the backup's own
// context) must publish as FAILED, not as an operator cancellation.
func TestOverlapRefusalCarryingAForeignCancellationPublishesAsFailed(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())
	sourcer.reindexOverlapFn = func(context.Context) error {
		// The backup's own context is untouched here.
		return undeterminedOverlapErr{cause: context.Canceled}
	}

	slot := &fakeStatusSlot{}

	ctx := context.Background()
	err := runOverlapBackup(ctx, backend, sourcer, slot, backupID, nil, time.Now().UTC())
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "the backup's own context was never cancelled")

	storedStatus, storedReason := backend.getMetaStatus()
	require.Equal(t, backup.Failed, storedStatus,
		"a refusal published as CANCELLED reads as an operator abort that never happened")
	require.Contains(t, storedReason, "cannot rule out a runtime-reindex during this backup",
		"the refusal must keep the text that says why the backup was failed")
	require.NotContains(t, storedReason, "overlapped this backup",
		"the check never got an answer, so it must not report a migration it did not see")

	require.NotContains(t, slot.statuses, backup.Cancelled,
		"nobody cancelled this backup")
	require.Equal(t, backup.Failed, slot.last(),
		"the last status an operator can poll has to be the failure")
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

// Pins: the overlap check must be asked about the whole capture window, not
// the commit instant, or a migration inside the window would read as clean.
func TestOverlapCheckIsAskedAboutTheCaptureWindowNotTheCommitInstant(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())

	// Far enough back that "now" cannot be mistaken for it.
	startedAt := time.Now().UTC().Add(-time.Hour)
	classes := []string{"Movies"}

	require.NoError(t, runOverlapBackup(context.Background(), backend, sourcer,
		&fakeStatusSlot{}, backupID, classes, startedAt))

	askedClasses, askedSince, calls := sourcer.lastOverlapQuery()
	require.Equal(t, 1, calls, "every commit has to consult the overlap check exactly once")
	require.Equal(t, startedAt, askedSince,
		"the check must be asked about the capture start, not the commit instant")
	require.Equal(t, classes, askedClasses,
		"the check must be asked about the classes this backup captured")
}

// Pins: the remembered failure must answer only for the backup that failed,
// never for another id polling the same slot.
func TestRememberedFailureAnswersOnlyTheBackupThatFailed(t *testing.T) {
	const (
		failedID = "backup-a"
		otherID  = "backup-b"
		reason   = "a runtime-reindex overlapped this backup"
	)

	tests := []struct {
		name string
		// setup drives the slot into the state the poll then reads.
		setup      func(s *backupStat)
		pollID     string
		wantFound  bool
		wantReason string
	}{
		{
			name:       "the failed backup's own poll gets the reason",
			setup:      func(s *backupStat) { s.setFailed(reason) },
			pollID:     failedID,
			wantFound:  true,
			wantReason: reason,
		},
		{
			name:      "another backup's poll is not answered with this failure",
			setup:     func(s *backupStat) { s.setFailed(reason) },
			pollID:    otherID,
			wantFound: false,
		},
		{
			name:      "an empty id matches nothing, including a slot that never failed",
			setup:     func(s *backupStat) {},
			pollID:    "",
			wantFound: false,
		},
		{
			name:      "a slot that ended without failing remembers nothing",
			setup:     func(s *backupStat) { s.set(backup.Success) },
			pollID:    failedID,
			wantFound: false,
		},
		{
			name: "a retry under the same id is not answered with the earlier attempt's failure",
			setup: func(s *backupStat) {
				s.setFailed(reason)
				s.reset()
				prevID, _ := s.renew(failedID, "bucket/backups/a", "", "")
				require.Empty(t, prevID)
			},
			pollID:    failedID,
			wantFound: false,
		},
		{
			name: "a different backup starting does not erase the failed one's reason",
			setup: func(s *backupStat) {
				s.setFailed(reason)
				s.reset()
				prevID, _ := s.renew(otherID, "bucket/backups/b", "", "")
				require.Empty(t, prevID)
			},
			pollID:     failedID,
			wantFound:  true,
			wantReason: reason,
		},
		{
			name:       "a failure with no reason is remembered by its stand-in",
			setup:      func(s *backupStat) { s.setFailed("") },
			pollID:     failedID,
			wantFound:  true,
			wantReason: failureWithoutReason,
		},
		{
			name: "a cancelled backup is not turned into a failure",
			setup: func(s *backupStat) {
				s.set(backup.Cancelled)
				s.setFailed(reason)
			},
			pollID:    failedID,
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var slot backupStat
			prevID, _ := slot.renew(failedID, "bucket/backups/a", "", "")
			require.Empty(t, prevID)
			tc.setup(&slot)

			gotReason, gotFound := slot.rememberedFailure(tc.pollID)
			require.Equal(t, tc.wantFound, gotFound)
			require.Equal(t, tc.wantReason, gotReason)
		})
	}
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
