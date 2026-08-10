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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// hookSnapshotter runs fn on the operation goroutine, from inside the snapshot
// step both the participant backupper and the participant restorer reach while
// they still hold their slot.
type hookSnapshotter struct{ fn func() }

func (h *hookSnapshotter) Snapshot(roles ...string) ([]byte, error) {
	h.fn()
	return []byte(`{"version":1}`), nil
}

func (h *hookSnapshotter) Restore(_ []byte, _ bool) error {
	h.fn()
	return nil
}

// Pins that the participant restorer releases only its own slot, not one a
// newer restore has since claimed.
func TestRestorerRestoreReleasesOnlyItsOwnSlot(t *testing.T) {
	t.Parallel()
	const (
		backupID = "1"
		newID    = "live-restore"
	)

	tests := []struct {
		name       string
		steal      bool
		wantSlotID string
	}{
		{name: "slot still held by this restore", wantSlotID: ""},
		{name: "slot taken over by a newer restore", steal: true, wantSlotID: newID},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := newFakeBackend()
			backend.On("SourceDataPath").Return(t.TempDir())
			backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)

			logger, _ := test.NewNullLogger()
			var r *restorer
			stolen := make(chan struct{})
			rbac := &hookSnapshotter{fn: func() {
				if !tc.steal {
					return
				}
				// assert, not require: Goexit here would kill the restore
				// goroutine mid-flight and surface as a hang.
				takeOverSlot(t, &r.lastOp, backupID, newID)
				close(stolen)
			}}
			r = newRestorer("node1", logger, &fakeSourcer{}, rbac, nil,
				&fakeBackupBackendProvider{backend: backend}, false)

			desc := &backup.BackupDescriptor{
				ID:            backupID,
				ServerVersion: "1.23",
				Version:       "1",
				StartedAt:     time.Now().UTC(),
				RbacBackups:   []byte(`{"version":1}`),
			}
			store := nodeStore{objectStore{backend: backend, backupId: backupID}}
			_, err := r.restore(&Request{Method: OpRestore, ID: backupID, Backend: "s3"}, desc, store)
			require.NoError(t, err)

			if tc.steal {
				// The takeover runs on the restore goroutine, so wait for it
				// rather than racing the assertion against it.
				select {
				case <-stolen:
				case <-time.After(10 * time.Second):
					t.Fatal("the newer restore never got to claim the slot")
				}
				require.Never(t, func() bool {
					return r.lastOp.get().ID != tc.wantSlotID
				}, 200*time.Millisecond, 10*time.Millisecond,
					"the finished restore released a slot a newer restore owns")
				return
			}
			require.Eventually(t, func() bool {
				return r.lastOp.get().ID == tc.wantSlotID
			}, 10*time.Second, 10*time.Millisecond,
				"the finished restore never released its own slot")
		})
	}
}

// Backupper-side mirror of TestRestorerRestoreReleasesOnlyItsOwnSlot; the
// takeover is staged by hand since backup has no production cancel path yet.
func TestBackupperBackupReleasesOnlyItsOwnSlot(t *testing.T) {
	t.Parallel()
	const (
		backupID = "1"
		newID    = "live-backup"
	)

	tests := []struct {
		name       string
		steal      bool
		wantSlotID string
	}{
		{name: "slot still held by this backup", wantSlotID: ""},
		{name: "slot taken over by a newer backup", steal: true, wantSlotID: newID},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			descriptors := make(chan backup.ClassDescriptor)
			close(descriptors)

			sourcer := &fakeSourcer{}
			sourcer.On("BackupDescriptors", mock.Anything, backupID, mock.Anything, mock.Anything).
				Return((<-chan backup.ClassDescriptor)(descriptors))

			backend := newFakeBackend()
			backend.On("SourceDataPath").Return(t.TempDir())
			backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
			backend.On("PutObject", mock.Anything, backupID, BackupFile, mock.Anything).Return(nil)

			logger, _ := test.NewNullLogger()
			var b *backupper
			stolen := make(chan struct{})
			rbac := &hookSnapshotter{fn: func() {
				if !tc.steal {
					return
				}
				takeOverSlot(t, &b.lastOp, backupID, newID)
				close(stolen)
			}}
			b = newBackupper(nodeName, logger, config.Backup{}, sourcer, rbac, nil,
				&fakeBackupBackendProvider{backend: backend})

			store := nodeStore{objectStore{backend: backend, backupId: backupID}}
			_, err := b.backup(store, &Request{Method: OpCreate, ID: backupID, Backend: "s3"})
			require.NoError(t, err)

			if tc.steal {
				select {
				case <-stolen:
				case <-time.After(10 * time.Second):
					t.Fatal("the newer backup never got to claim the slot")
				}
				require.Never(t, func() bool {
					return b.lastOp.get().ID != tc.wantSlotID
				}, 200*time.Millisecond, 10*time.Millisecond,
					"the finished backup released a slot a newer backup owns")
				return
			}
			require.Eventually(t, func() bool {
				return b.lastOp.get().ID == tc.wantSlotID
			}, 10*time.Second, 10*time.Millisecond,
				"the finished backup never released its own slot")
		})
	}
}
