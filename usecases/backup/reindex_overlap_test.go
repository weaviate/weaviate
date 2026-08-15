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

// recordingSlot records the whole sequence a node's operation slot was driven
// through, so a test can assert what the slot was never told.
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

// TestCommitTimeOverlapCheckPlacement pins where the check sits: after the
// capture, before anything says the capture is publishable.
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

	t.Run("a capture that already failed is not asked", func(t *testing.T) {
		u, sourcer, _, desc := uploadFixture(t, class, errors.New("no space left on device"), nil)

		require.Error(t, u.all(context.Background(), []string{class}, desc, nil, "", ""))
		assert.Empty(t, sourcer.overlapCalls(),
			"there is nothing to judge when the capture never completed")
	})
}

// TestHowACheckErrorIsPublished pins the line between a refusal and an operator
// abort: a refusal ends FAILED even when its cause wraps a cancellation, and a
// cancel ends CANCELLED.
func TestHowACheckErrorIsPublished(t *testing.T) {
	const class = "Article"

	tests := []struct {
		name         string
		refusal      error
		wantStatus   backup.Status
		wantFailures bool
	}{
		{
			name: "an overlap the check observed",
			refusal: fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
				backup.ErrReindexOverlappedBackup, class),
			wantStatus:   backup.Failed,
			wantFailures: true,
		},
		{
			// A shape the check does not emit: a cancelled context is answered
			// as a cancel before a verdict exists. Pinned for whatever does.
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
			u, sourcer, slot, desc := uploadFixture(t, class, nil, nil)
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

// TestWithMetaFault pins the reason a status poll is served when the metadata
// write fails alongside the capture.
func TestWithMetaFault(t *testing.T) {
	const reason = `backup blocked: collection "Article" was migrated`

	tests := []struct {
		name    string
		metaErr error
		want    string
	}{
		{name: "the write landed", want: reason},
		{
			name:    "the write failed",
			metaErr: errors.New("disk full"),
			want:    reason + "; uploading the backup metadata also failed: disk full",
		},
		{
			name:    "the write failed on a cancelled request",
			metaErr: fmt.Errorf("s3: %w", context.Canceled),
			want:    reason + "; uploading the backup metadata also failed: s3: a canceled context",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := withMetaFault(reason, tt.metaErr)

			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, context.Canceled.Error())
		})
	}
}

// TestOrdinaryCaptureFailureWithAFailedMetaWrite pins the one place the
// migration kill switch is not a byte-identical no-op: an ordinary capture
// failure whose metadata write also failed now leads with the capture's own
// text instead of wrapping it in the write fault.
func TestOrdinaryCaptureFailureWithAFailedMetaWrite(t *testing.T) {
	const class = "Article"
	u, _, slot, desc := uploadFixture(t, class,
		errors.New("no space left on device"), errors.New("meta write rejected"))

	require.Error(t, u.all(context.Background(), []string{class}, desc, nil, "", ""))

	require.Len(t, slot.failures, 1)
	assert.True(t, strings.HasPrefix(slot.failures[0],
		"no space left on device; uploading the backup metadata also failed: "),
		"the capture's reason has to lead; got: %s", slot.failures[0])
	assert.Contains(t, slot.failures[0], "meta write rejected")
}

// TestPublishedReasonNeverReadsAsACancel pins where the cancel phrase is
// scrubbed and where it is not: out of a refusal this branch composes, so a
// metadata write that failed on a cancelled request cannot put it back, and
// left alone on a clean capture, which has no torn state to protect.
func TestPublishedReasonNeverReadsAsACancel(t *testing.T) {
	const class = "Article"
	observed := fmt.Errorf("%w: collection %q was migrated while this backup was being captured",
		backup.ErrReindexOverlappedBackup, class)
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
			refusal: observed, metaErr: cancelledWrite, wantStatus: backup.Failed,
		},
		{
			// The capture passed the check, so there is nothing torn under
			// this id and re-posting it is safe. Scrubbing here would only
			// cost the operator the CANCELLED the write fault earns.
			name:    "a clean capture keeps its write fault verbatim",
			metaErr: cancelledWrite, wantStatus: backup.Transferred, keepsCancelText: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, sourcer, slot, desc := uploadFixture(t, class, nil, tt.metaErr)
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

// TestCommitTimeOverlapRefusalIsServedWhole pins that the refusal reaches a
// status poll intact, with a co-occurring metadata write fault after it.
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

// TestGateRefusalIsRedactedOnTheStatusAPI pins that the shard the per-shard
// gate refused never reaches a status poll.
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

// TestUnanswerableOverlapCheckSurvivesTheRPC pins the RPC round trip: each half
// looked right alone while the pair told the operator to wait for a migration
// that does not exist.
func TestUnanswerableOverlapCheckSurvivesTheRPC(t *testing.T) {
	refusal := backup.ReindexOverlapCheckError{
		Msg: backup.ErrReindexOverlapCheckUnanswerable.Error() +
			": DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS is 0, so a finished runtime-reindex is " +
			"dropped from the cluster task list; raise it above the time a backup takes, or set " +
			"RUNTIME_REINDEX_ENABLED=false",
	}

	kind := classifyCanCommitErr(refusal)
	require.Equal(t, CanCommitErrOverlapCheckUnanswerable, kind)
	require.NotEqual(t, CanCommitErrInFlightReindex, kind,
		"nothing is in flight, and that kind makes the coordinator rebuild the text")

	err := canCommitErrFromResponse(
		&CanCommitResponse{Method: OpCreate, ID: "1", Err: refusal.Error(), ErrKind: kind},
		[]string{"Movies", "Shows"})

	require.ErrorIs(t, err, backup.ErrReindexOverlapCheckUnanswerable)
	require.NotErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)
	assert.Contains(t, err.Error(), "DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS")
	assert.Contains(t, err.Error(), "RUNTIME_REINDEX_ENABLED")
	assert.NotContains(t, err.Error(), "in flight")
	assert.NotContains(t, err.Error(), "retry after the migration finishes")
	for _, class := range []string{"Movies", "Shows"} {
		assert.NotContains(t, err.Error(), class,
			"the cause is this node's configuration, not any collection")
	}
	assert.True(t, isReindexRefusal(err),
		"a configuration refusal is a cluster answer, so it must not be prefixed with a node name")
}
