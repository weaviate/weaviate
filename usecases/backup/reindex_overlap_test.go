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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// recordingSlot records the sequence a node's operation slot was driven
// through, so a test can assert what the slot was never told rather than
// only what it ended on.
type recordingSlot struct {
	mu       sync.Mutex
	statuses []backup.Status
	failures []string
}

func (s *recordingSlot) set(st backup.Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses = append(s.statuses, st)
}

func (s *recordingSlot) setFailed(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses = append(s.statuses, backup.Failed)
	s.failures = append(s.failures, reason)
}

func (s *recordingSlot) saw(st backup.Status) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seen := range s.statuses {
		if seen == st {
			return true
		}
	}
	return false
}

// uploadFixture builds an uploader over a capture that produces one class
// descriptor, carrying descErr when the capture itself failed.
func uploadFixture(t *testing.T, class string, descErr, metaErr error) (*uploader, *fakeSourcer, *recordingSlot, *backup.BackupDescriptor) {
	t.Helper()
	slot := &recordingSlot{}
	u, sourcer, desc := uploadFixtureWithSlot(t, class, descErr, metaErr, slot)
	return u, sourcer, slot, desc
}

// uploadFixtureWithSlot publishes into the caller's slot, which the
// status-API test needs so OnStatus reads what the uploader published.
func uploadFixtureWithSlot(t *testing.T, class string, descErr, metaErr error, slot statusPublisher) (*uploader, *fakeSourcer, *backup.BackupDescriptor) {
	t.Helper()
	const backupID = "1"

	descriptors := make(chan backup.ClassDescriptor, 1)
	descriptors <- backup.ClassDescriptor{Name: class, Error: descErr}
	close(descriptors)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", mock.Anything, backupID, []string{class}, mock.Anything).
		Return((<-chan backup.ClassDescriptor)(descriptors))
	sourcer.On("ReleaseBackup", mock.Anything, backupID, class).Return(nil)

	backend := newFakeBackend()
	backend.On("PutObject", mock.Anything, backupID, BackupFile, mock.Anything).Return(metaErr)
	backend.On("PutObject", mock.Anything, backupID, mock.Anything, mock.Anything).Return(nil).Maybe()
	backend.On("SourceDataPath").Return(t.TempDir()).Maybe()

	logger, _ := test.NewNullLogger()
	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	desc := &backup.BackupDescriptor{ID: backupID, StartedAt: time.Now().UTC().Add(-time.Minute)}

	return newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, slot, logger),
		sourcer, desc
}

// TestCommitTimeOverlapCheckPlacement pins where the check sits: after
// the capture, before anything says the capture is publishable, asked
// about what this node captured and from when it started.
func TestCommitTimeOverlapCheckPlacement(t *testing.T) {
	const class = "Article"

	t.Run("asked once, about the capture start and the captured classes", func(t *testing.T) {
		u, sourcer, slot, desc := uploadFixture(t, class, nil, nil)

		require.NoError(t, u.all(context.Background(), []string{class}, desc, nil, "", ""))

		calls := sourcer.overlapCalls()
		require.Len(t, calls, 1, "one commit, one check")
		assert.Equal(t, []string{class}, calls[0].classes)
		assert.Equal(t, desc.StartedAt, calls[0].since,
			"asked about the capture start, not the commit instant")
		assert.True(t, slot.saw(backup.Success))
	})

	t.Run("a refusal stops the capture from ever reading as transferred", func(t *testing.T) {
		u, sourcer, slot, desc := uploadFixture(t, class, nil, nil)
		refusal := fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
			backup.ErrReindexOverlappedBackup, class)
		sourcer.setOverlapRefusal(refusal)

		err := u.all(context.Background(), []string{class}, desc, nil, "", "")

		require.ErrorIs(t, err, backup.ErrReindexOverlappedBackup)
		assert.False(t, slot.saw(backup.Transferred),
			"a backup a migration overlapped must never be offered for commit")
		assert.False(t, slot.saw(backup.Success))
		assert.Equal(t, backup.Failed, desc.Status)
	})

	t.Run("a capture that already failed is not asked", func(t *testing.T) {
		u, sourcer, _, desc := uploadFixture(t, class, errors.New("no space left on device"), nil)

		require.Error(t, u.all(context.Background(), []string{class}, desc, nil, "", ""))
		assert.Empty(t, sourcer.overlapCalls(),
			"there is nothing to judge when the capture never completed")
	})
}

// TestCommitTimeOverlapRefusalIsNotAnOperatorAbort pins the "Never an
// operator abort" rule in uploader.all: a refusal ends the backup FAILED
// even when its own cause wraps a cancellation.
func TestCommitTimeOverlapRefusalIsNotAnOperatorAbort(t *testing.T) {
	const class = "Article"
	u, sourcer, slot, desc := uploadFixture(t, class, nil, nil)
	sourcer.setOverlapRefusal(fmt.Errorf("%w: the cluster task manager could not be listed: %w",
		backup.ErrReindexOverlapUndetermined, context.Canceled))

	err := u.all(context.Background(), []string{class}, desc, nil, "", "")

	require.ErrorIs(t, err, backup.ErrReindexOverlapUndetermined)
	assert.Equal(t, backup.Failed, desc.Status)
	assert.False(t, slot.saw(backup.Cancelled),
		"a refusal is not an operator abort, whatever its cause wraps")
	require.NotEmpty(t, slot.failures)
}

// TestOperatorCancelDuringTheOverlapCheckIsCancelled is the other half of
// the rule above. The check runs on the operation's own context, so an
// operator cancel arrives as the check's error; that one is an operator
// abort and must not publish FAILED, which would burn the backup id.
func TestOperatorCancelDuringTheOverlapCheckIsCancelled(t *testing.T) {
	const class = "Article"
	u, sourcer, slot, desc := uploadFixture(t, class, nil, nil)
	sourcer.setOverlapRefusal(context.Canceled)

	err := u.all(context.Background(), []string{class}, desc, nil, "", "")

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, backup.Cancelled, desc.Status)
	assert.True(t, slot.saw(backup.Cancelled))
	assert.Empty(t, slot.failures)
}

// TestCommitTimeOverlapRefusalIsServedWhole pins that the refusal reaches a
// status poll intact: its own text first, with a co-occurring metadata write
// fault after it. Wrapping the refusal instead buries the reason behind
// whatever the wrapper of the moment says.
func TestCommitTimeOverlapRefusalIsServedWhole(t *testing.T) {
	const (
		backupID = "1"
		class    = "Article"
	)
	refusal := fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
		backup.ErrReindexOverlappedBackup, class)

	logger, _ := test.NewNullLogger()
	bp := &backupper{logger: logger}
	require.Empty(t, bp.lastOp.renew(backupID, "bucket/backups/1", "", ""))
	u, sourcer, desc := uploadFixtureWithSlot(t, class, nil,
		errors.New("meta write rejected"), &bp.lastOp)
	sourcer.setOverlapRefusal(refusal)

	require.ErrorIs(t, u.all(context.Background(), []string{class}, desc, nil, "", ""),
		backup.ErrReindexOverlappedBackup)

	res := (&Handler{backupper: bp}).OnStatus(context.Background(),
		&StatusRequest{Method: OpCreate, ID: backupID})

	require.Equal(t, backup.Failed, res.Status)
	assert.True(t, strings.HasPrefix(res.Err, refusal.Error()),
		"the reason has to lead; got: %s", res.Err)
	assert.Contains(t, res.Err, "uploading the backup metadata also failed")
	assert.Equal(t, refusal.Error(), desc.Error)
}

// TestGateRefusalIsRedactedOnTheStatusAPI pins that the shard the
// per-shard gate refused never reaches a status poll. The gate's own text
// is served, with a co-occurring metadata fault named beside it rather
// than wrapped around it.
func TestGateRefusalIsRedactedOnTheStatusAPI(t *testing.T) {
	const (
		backupID = "1"
		class    = "Article"
		shard    = "vT4Kq9LmShardId"
	)
	// The shape the storage layer produces: a redacted refusal, wrapped
	// on its way up by layers that do name the shard.
	blocked := backup.ReindexBlockedError{
		Msg: fmt.Sprintf("%s: collection %q has an active runtime-reindex task in DTM",
			backup.ErrBackupBlockedByInFlightReindex, class),
	}
	wrapped := fmt.Errorf("shard %q: %w", shard, blocked)

	cases := []struct {
		name    string
		metaErr error
	}{
		{name: "the descriptor lands"},
		{name: "the descriptor does not land", metaErr: errors.New("meta write rejected")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			bp := &backupper{logger: logger}
			require.Empty(t, bp.lastOp.renew(backupID, "bucket/backups/1", "", ""))
			u, _, desc := uploadFixtureWithSlot(t, class, wrapped, tc.metaErr, &bp.lastOp)

			require.ErrorIs(t, u.all(context.Background(), []string{class}, desc, nil, "", ""),
				backup.ErrBackupBlockedByInFlightReindex)

			res := (&Handler{backupper: bp}).OnStatus(context.Background(),
				&StatusRequest{Method: OpCreate, ID: backupID})

			require.Equal(t, backup.Failed, res.Status)
			for _, served := range []string{res.Err, desc.Error} {
				assert.NotContains(t, served, shard,
					"the shard the wrappers named must not reach a status poll")
				assert.Contains(t, served, class)
				assert.Contains(t, served, backup.ErrBackupBlockedByInFlightReindex.Error())
			}
			if tc.metaErr != nil {
				assert.Contains(t, res.Err, "uploading the backup metadata also failed",
					"the write fault is named beside the refusal, not in place of it")
			}
		})
	}
}
