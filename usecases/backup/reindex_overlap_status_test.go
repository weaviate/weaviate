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
