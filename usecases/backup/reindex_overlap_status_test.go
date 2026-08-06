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
	setStatus := func(st backup.Status) {
		_, reason := backend.getMetaStatus()
		observed = append(observed, observation{status: st, reason: reason})
	}

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, store, backupID, setStatus, logger)

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

	var observed []backup.Status
	setStatus := func(st backup.Status) { observed = append(observed, st) }

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, store, backupID, setStatus, logger)

	desc := backup.BackupDescriptor{ID: backupID, StartedAt: time.Now().UTC()}
	err := uploader.all(ctx, nil, &desc, nil, "", "")
	require.ErrorIs(t, err, context.Canceled)

	storedStatus, storedReason := backend.getMetaStatus()
	require.Equal(t, backup.Cancelled, storedStatus)
	require.NotContains(t, storedReason, "reindex",
		"an aborted backup must not be blamed on a migration that never ran")

	require.NotContains(t, observed, backup.Failed,
		"the status API must not report FAILED for a backup the operator aborted")
	require.Equal(t, backup.Cancelled, observed[len(observed)-1],
		"the last status an operator can poll has to be the cancellation they caused")
}

// The check is a backstop only because it is asked about the whole capture
// window. Handed the commit instant instead, it would ask "is a reindex running
// right now" — the question the pre-capture gates already answered — and a
// migration that started and finished inside the window would read as clean.
func TestOverlapCheckIsAskedAboutTheCaptureWindowNotTheCommitInstant(t *testing.T) {
	const backupID = "1"
	any := mock.Anything

	backend := newFakeBackend()
	backend.On("PutObject", any, backupID, BackupFile, any).Return(nil)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(fakeBackupDescriptor())

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, store, backupID,
		func(backup.Status) {}, logger)

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
