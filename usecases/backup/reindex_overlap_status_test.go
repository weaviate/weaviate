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

// The observable status and the stored failure reason are written by two
// different steps, and GET /v1/backups/{backend}/{id} reads both. A poll landing
// between them would report FAILED with an empty reason, which is the headline
// failure of this feature reported as nothing at all.
func TestOverlapRefusalPublishesTheReasonBeforeFailedBecomesObservable(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())
	sourcer.reindexOverlapErr = fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
		backup.ErrBackupSpannedReindex, "Movies")

	// What an observer polling at the instant of each status change would read.
	type observation struct {
		status backup.Status
		reason string
	}
	var observed []observation
	slot := &fakeStatusSlot{onChange: func(st backup.Status) {
		_, reason := backend.getMetaStatus()
		observed = append(observed, observation{status: st, reason: reason})
	}}

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, slot, logger)

	desc := backup.BackupDescriptor{ID: backupID, StartedAt: time.Now().UTC()}
	err := uploader.all(context.Background(), nil, &desc, nil, "", "")
	require.ErrorIs(t, err, backup.ErrBackupSpannedReindex)

	storedStatus, storedReason := backend.getMetaStatus()
	require.Equal(t, backup.Failed, storedStatus)
	require.Contains(t, storedReason, "a runtime-reindex overlapped this backup")

	var sawFailed bool
	for _, o := range observed {
		if o.status != backup.Failed {
			continue
		}
		sawFailed = true
		require.Contains(t, o.reason, "a runtime-reindex overlapped this backup",
			"a status poll in this window reads FAILED without a reason")
	}
	require.True(t, sawFailed, "the refused backup has to end up observably FAILED")
}

// An operator abort cancels the same context the commit-time overlap lookup
// runs on, so the lookup comes back with the cancellation instead of an answer.
// That must stay a cancellation: reporting FAILED with "a runtime-reindex
// overlapped this backup" names a migration that never happened, and leaves the
// status API disagreeing with the stored descriptor.
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
		// The abort lands while the lookup's RAFT query is in flight, so the
		// query fails with the cancellation and the lookup reports it as
		// "cannot rule out a runtime-reindex".
		cancel()
		return fmt.Errorf("cannot rule out a runtime-reindex during this backup: "+
			"the cluster task manager could not be queried: %w", context.Canceled)
	}

	slot := &fakeStatusSlot{}

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, slot, logger)

	desc := backup.BackupDescriptor{ID: backupID, StartedAt: time.Now().UTC()}
	err := uploader.all(ctx, nil, &desc, nil, "", "")
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

// The refusal's status flip waits for the meta write, so object storage failing
// that write is exactly when the flip is at risk of being skipped. Leaving the
// slot at Transferring publishes a finished operation as still running, and the
// operator loses the reason for the one failure mode this feature adds.
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

	// The real slot on a real backupper, not a stub: this is what OnStatus
	// serves from, so the whole path from the refusal to the wire is exercised.
	bp := &backupper{}
	slot := &bp.lastOp
	require.Empty(t, slot.renew(backupID, "bucket/backups/1", "", ""))

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, slot, logger)

	desc := backup.BackupDescriptor{ID: backupID, StartedAt: time.Now().UTC()}
	err := uploader.all(context.Background(), nil, &desc, nil, "", "")

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

	// The operation returns and releases the slot within a millisecond of
	// setting the reason, which is what backupper.backup's deferred reset does.
	// A realistic poll lands after that, and with the descriptor unwritten
	// there is nothing else left to read the reason from.
	slot.reset()
	res = handler.OnStatus(context.Background(), &StatusRequest{Method: OpCreate, ID: backupID})
	require.Equal(t, backup.Failed, res.Status,
		"once the slot is released the poll must still see a failure, not a backup that might be running")
	require.Contains(t, res.Err, "a runtime-reindex overlapped this backup",
		"a reason that evaporates with the slot is a reason no operator ever reads")
}

// A cancellation that is not the caller's is not an abort. The lookup is a
// leader-forwarded RAFT query, and a client that gives up on its own derived
// context hands back an error carrying context.Canceled while this backup's
// context is still live. That is the lookup failing to answer, which is what
// the check fails closed on, so it has to publish as FAILED with the refusal
// text — not as an operator cancellation.
func TestOverlapRefusalCarryingAForeignCancellationPublishesAsFailed(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())
	sourcer.reindexOverlapFn = func(context.Context) error {
		// The backup's own context is untouched; the cancellation belongs to
		// whatever the RAFT client ran the query on.
		return undeterminedOverlapErr{cause: context.Canceled}
	}

	slot := &fakeStatusSlot{}

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, slot, logger)

	ctx := context.Background()
	desc := backup.BackupDescriptor{ID: backupID, StartedAt: time.Now().UTC()}
	err := uploader.all(ctx, nil, &desc, nil, "", "")
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

// undeterminedOverlapErr is the shape the storage layer gives a commit-time
// refusal that never observed an overlap: both sentinels reachable through
// errors.Is, neither of their texts printed.
type undeterminedOverlapErr struct{ cause error }

func (e undeterminedOverlapErr) Error() string {
	return "cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried"
}

func (e undeterminedOverlapErr) Unwrap() []error {
	return []error{backup.ErrBackupSpannedReindex, backup.ErrReindexOverlapUndetermined, e.cause}
}

// The refusal is the only thing an operator gets, and the two kinds of refusal
// call for different next steps. "A migration ran during your backup" sends them
// to the task list; for a check that could not read the task list there is
// nothing there to find, and the honest answer is to fix what made it
// unreadable and back up again.
func TestOverlapRefusalDistinguishesAnObservedMigrationFromAnUnansweredCheck(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	tests := []struct {
		name       string
		overlapErr error
		wantReason string
		wantAbsent string
	}{
		{
			name: "the check saw the migration",
			overlapErr: fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
				backup.ErrBackupSpannedReindex, "Movies"),
			wantReason: "a runtime-reindex overlapped this backup",
			wantAbsent: "cannot rule out",
		},
		{
			name:       "the check could not answer",
			overlapErr: undeterminedOverlapErr{cause: errors.New("leader unreachable")},
			wantReason: "cannot rule out a runtime-reindex during this backup",
			wantAbsent: "overlapped this backup",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)

			sourcer := &fakeSourcer{}
			sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())
			sourcer.reindexOverlapErr = tc.overlapErr

			logger, _ := test.NewNullLogger()
			store := nodeStore{objectStore{backend: backend, backupId: backupID}}
			uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID,
				&fakeStatusSlot{}, logger)

			desc := backup.BackupDescriptor{ID: backupID, StartedAt: time.Now().UTC()}
			err := uploader.all(context.Background(), nil, &desc, nil, "", "")
			require.ErrorIs(t, err, backup.ErrBackupSpannedReindex,
				"both refusals stay classifiable as this check's")

			storedStatus, storedReason := backend.getMetaStatus()
			require.Equal(t, backup.Failed, storedStatus, "both refusals fail the backup")
			require.Contains(t, storedReason, tc.wantReason)
			require.NotContains(t, storedReason, tc.wantAbsent,
				"the refusal must not claim more than the check established")
		})
	}
}

// The check is a backstop only because it is asked about the whole capture
// window. Handed the commit instant instead, it would ask "is a reindex running
// right now", which is the question the pre-capture gates already answered, and
// a migration that started and finished inside the window would read as clean.
func TestOverlapCheckIsAskedAboutTheCaptureWindowNotTheCommitInstant(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID,
		&fakeStatusSlot{}, logger)

	// Far enough back that "now" cannot be mistaken for it.
	startedAt := time.Now().UTC().Add(-time.Hour)
	desc := backup.BackupDescriptor{ID: backupID, StartedAt: startedAt}
	classes := []string{"Movies"}

	require.NoError(t, uploader.all(context.Background(), classes, &desc, nil, "", ""))

	askedClasses, askedSince, calls := sourcer.lastOverlapQuery()
	require.Equal(t, 1, calls, "every commit has to consult the overlap check exactly once")
	require.Equal(t, startedAt, askedSince,
		"the check must be asked about the capture start, not the commit instant")
	require.Equal(t, classes, askedClasses,
		"the check must be asked about the classes this backup captured")
}
