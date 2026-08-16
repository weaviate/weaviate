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

// recordingSlot keeps every status the slot saw, so a test can assert what it never saw.
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

func observedOverlap(class string) error {
	return fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
		backup.ErrReindexOverlappedBackup, class)
}

// uploadFixture builds an uploader over a one-class capture that failed with descErr.
func uploadFixture(t *testing.T, class string, descErr, metaErr error, slot statusPublisher) (*uploader, *fakeSourcer, *backup.BackupDescriptor) {
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

func TestCommitTimeOverlapCheckPlacement(t *testing.T) {
	const class = "Article"

	t.Run("asked once, about the capture start and the captured classes", func(t *testing.T) {
		slot := &recordingSlot{}
		u, sourcer, desc := uploadFixture(t, class, nil, nil, slot)

		require.NoError(t, u.all(context.Background(), []string{class}, desc, nil, "", ""))

		calls := sourcer.overlapCalls()
		require.Len(t, calls, 1, "one commit, one check")
		assert.Equal(t, []string{class}, calls[0].classes)
		assert.Equal(t, desc.StartedAt, calls[0].since,
			"asked about the capture start, not the commit instant")
		assert.True(t, slot.saw(backup.Success))
	})

	t.Run("a capture that already failed is not asked", func(t *testing.T) {
		u, sourcer, desc := uploadFixture(t, class,
			errors.New("no space left on device"), nil, &recordingSlot{})

		require.Error(t, u.all(context.Background(), []string{class}, desc, nil, "", ""))
		assert.Empty(t, sourcer.overlapCalls(),
			"there is nothing to judge when the capture never completed")
	})
}

// A refusal ends FAILED even when its cause wraps a cancel; an abort ends CANCELLED.
func TestHowACheckErrorIsPublished(t *testing.T) {
	const class = "Article"

	tests := []struct {
		name         string
		refusal      error
		wantStatus   backup.Status
		wantFailures bool
	}{
		{
			name:         "an overlap the check observed",
			refusal:      observedOverlap(class),
			wantStatus:   backup.Failed,
			wantFailures: true,
		},
		{
			// A shape the check does not emit today, pinned for whatever does.
			name: "an unanswerable check whose own cause was cancelled",
			refusal: fmt.Errorf("%w: the cluster task manager could not be listed: %w",
				backup.ErrReindexOverlapUndetermined, context.Canceled),
			wantStatus:   backup.Failed,
			wantFailures: true,
		},
		{
			name:       "an operator cancelling the backup",
			refusal:    context.Canceled,
			wantStatus: backup.Cancelled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot := &recordingSlot{}
			u, sourcer, desc := uploadFixture(t, class, nil, nil, slot)
			sourcer.setOverlapRefusal(tt.refusal)

			require.ErrorIs(t, u.all(context.Background(), []string{class}, desc, nil, "", ""),
				tt.refusal)

			assert.Equal(t, tt.wantStatus, desc.Status)
			assert.Equal(t, tt.wantStatus == backup.Cancelled, slot.saw(backup.Cancelled))
			assert.Equal(t, tt.wantFailures, len(slot.failures) > 0)
			assert.False(t, slot.saw(backup.Transferred),
				"a capture the check would not clear must never be offered for commit")
			assert.False(t, slot.saw(backup.Success))
		})
	}
}

func TestPublishedReasonNeverReadsAsACancel(t *testing.T) {
	const class = "Article"
	cancelledWrite := fmt.Errorf("s3: %w", context.Canceled)

	tests := []struct {
		name            string
		refusal         error
		metaErr         error
		wantStatus      backup.Status
		keepsCancelText bool
	}{
		{
			name:    "an overlap the check observed, and the metadata write was cancelled too",
			refusal: observedOverlap(class), metaErr: cancelledWrite, wantStatus: backup.Failed,
		},
		{
			// Nothing is torn here, so scrubbing would only cost a real CANCELLED.
			name:    "a clean capture keeps its write fault verbatim",
			metaErr: cancelledWrite, wantStatus: backup.Transferred, keepsCancelText: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot := &recordingSlot{}
			u, sourcer, desc := uploadFixture(t, class, nil, tt.metaErr, slot)
			sourcer.setOverlapRefusal(tt.refusal)

			require.Error(t, u.all(context.Background(), []string{class}, desc, nil, "", ""))

			require.Equal(t, tt.wantStatus, desc.Status)
			require.NotEmpty(t, slot.failures)
			assert.Equal(t, tt.keepsCancelText,
				strings.Contains(slot.failures[0], context.Canceled.Error()),
				"scrubbed exactly where this branch splices text into the reason")
		})
	}
}

func TestReindexRefusalOnTheStatusAPI(t *testing.T) {
	const (
		backupID = "1"
		class    = "Article"
		shard    = "vT4Kq9LmShardId"
	)
	// A redacted refusal, wrapped on its way up by layers that do name the shard.
	blocked := fmt.Errorf("shard %q: %w", shard, backup.ReindexBlockedError{
		Msg: fmt.Sprintf("%s: collection %q has an active runtime-reindex task in DTM",
			backup.ErrBackupBlockedByInFlightReindex, class),
	})
	metaErr := errors.New("meta write rejected")

	tests := []struct {
		name         string
		overlapErr   error
		descErr      error
		metaErr      error
		wantSentinel error
	}{
		{
			name:       "the commit-time refusal and the descriptor does not land",
			overlapErr: observedOverlap(class), metaErr: metaErr,
			wantSentinel: backup.ErrReindexOverlappedBackup,
		},
		{
			name:         "the per-shard refusal and the descriptor lands",
			descErr:      blocked,
			wantSentinel: backup.ErrBackupBlockedByInFlightReindex,
		},
		{
			name:    "the per-shard refusal and the descriptor does not land",
			descErr: blocked, metaErr: metaErr,
			wantSentinel: backup.ErrBackupBlockedByInFlightReindex,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			bp := &backupper{logger: logger}
			require.Empty(t, bp.lastOp.renew(backupID, "bucket/backups/1", "", ""))
			u, sourcer, desc := uploadFixture(t, class, tt.descErr, tt.metaErr, &bp.lastOp)
			sourcer.setOverlapRefusal(tt.overlapErr)

			require.ErrorIs(t, u.all(context.Background(), []string{class}, desc, nil, "", ""),
				tt.wantSentinel)

			res := (&Handler{backupper: bp}).OnStatus(context.Background(),
				&StatusRequest{Method: OpCreate, ID: backupID})

			require.Equal(t, backup.Failed, res.Status)
			for _, served := range []string{res.Err, desc.Error} {
				assert.NotContains(t, served, shard,
					"the shard the wrappers named must not reach a status poll")
				assert.Contains(t, served, class)
				assert.Contains(t, served, tt.wantSentinel.Error())
			}
			assert.True(t, strings.HasPrefix(res.Err, desc.Error),
				"the refusal has to lead; got: %s", res.Err)
			const metaFault = "uploading the backup metadata also failed"
			assert.Equal(t, tt.metaErr != nil, strings.Contains(res.Err, metaFault),
				"the write fault is named beside the refusal, and only when there was one")
			assert.NotContains(t, desc.Error, metaFault,
				"the descriptor records the refusal, not a write that failed after it")
		})
	}
}
