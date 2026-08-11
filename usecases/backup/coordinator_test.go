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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_CoordinatedBackup(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		any         = mock.Anything
		backupID    = "1"
		ctx         = context.Background()
		nodes       = []string{"N1", "N2"}
		classes     = []string{"Class-A", "Class-B"}
		now         = time.Now().UTC()
		creq        = &Request{
			Method:      OpCreate,
			ID:          backupID,
			Backend:     backendName,
			Classes:     classes,
			Duration:    _BookingPeriod,
			Compression: Compression{Level: GzipDefaultCompression, CPUPercentage: DefaultCPUPercentage},
		}
		cresp        = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq         = &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		sresp        = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
		abortReq     = &AbortRequest{OpCreate, backupID, backendName, "", "", ""}
		nodeResolver = newFakeNodeResolver(nodes)
	)

	t.Run("PutMeta", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(ErrAny).Once()

		coordinator := *fc.coordinator()
		req := newReq(classes, backendName, backupID)
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		err := coordinator.Backup(ctx, store, &req)
		assert.NotNil(t, err)
	})

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)
		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		coordinator := *fc.coordinator()
		mockBackendProvider := NewMockBackupBackendProvider(t)
		coordinator.backends = mockBackendProvider
		mockBackendProvider.EXPECT().BackupBackend(backendName, mock.Anything).Return(fc.backend, nil)
		bytes := marshalMeta(backup.BackupDescriptor{Status: backup.Success})
		fc.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bytes, nil).Twice()

		req := newReq(classes, backendName, backupID)
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		err := coordinator.Backup(ctx, store, &req)
		assert.Nil(t, err)
		<-fc.backend.doneChan

		got := fc.backend.glMeta
		assert.GreaterOrEqual(t, got.StartedAt, now)
		assert.Greater(t, got.CompletedAt, got.StartedAt)
		want := backup.DistributedBackupDescriptor{
			StartedAt:     got.StartedAt,
			CompletedAt:   got.CompletedAt,
			ID:            backupID,
			Status:        backup.Success,
			Version:       Version,
			ServerVersion: config.ServerVersion,
			Nodes: map[string]*backup.NodeDescriptor{
				nodes[0]: {
					Classes: classes,
					Status:  backup.Success,
				},
				nodes[1]: {
					Classes: classes,
					Status:  backup.Success,
				},
			},
			Leader:          nodes[0],
			CompressionType: backup.CompressionGZIP,
		}
		assert.Equal(t, want, got)
	})

	t.Run("SuccessOnShardsEmptyPhysical", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return([]string{}, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)
		fc.client.On("Abort", any, any, any).Return(nil)

		oneClassReq := &Request{
			Method:   OpCreate,
			ID:       backupID,
			Backend:  backendName,
			Classes:  []string{classes[1]},
			Duration: _BookingPeriod,
			Compression: Compression{
				Level:         GzipDefaultCompression,
				CPUPercentage: DefaultCPUPercentage,
			},
		}

		twoClassesReqcreq := &Request{
			Method:   OpCreate,
			ID:       backupID,
			Backend:  backendName,
			Classes:  classes[:],
			Duration: _BookingPeriod,
			Compression: Compression{
				Level:         GzipDefaultCompression,
				CPUPercentage: DefaultCPUPercentage,
			},
		}
		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == twoClassesReqcreq.Method && r.ID == twoClassesReqcreq.ID && r.Backend == twoClassesReqcreq.Backend &&
				len(r.Classes) == len(twoClassesReqcreq.Classes) && r.Duration == twoClassesReqcreq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == oneClassReq.Method && r.ID == oneClassReq.ID && r.Backend == oneClassReq.Backend &&
				len(r.Classes) == len(oneClassReq.Classes) && r.Duration == oneClassReq.Duration
		})).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		coordinator := *fc.coordinator()
		mockBackendProvider := NewMockBackupBackendProvider(t)
		coordinator.backends = mockBackendProvider
		mockBackendProvider.EXPECT().BackupBackend(backendName, mock.Anything).Return(fc.backend, nil)
		bytes := marshalMeta(backup.BackupDescriptor{Status: backup.Success})
		fc.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bytes, nil).Twice()

		req := newReq(classes, backendName, backupID)
		store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		err := coordinator.Backup(ctx, store, &req)
		assert.Nil(t, err)
		<-fc.backend.doneChan

		got := fc.backend.glMeta
		assert.GreaterOrEqual(t, got.StartedAt, now)
		assert.Greater(t, got.CompletedAt, got.StartedAt)
		want := backup.DistributedBackupDescriptor{
			StartedAt:     got.StartedAt,
			CompletedAt:   got.CompletedAt,
			ID:            backupID,
			Status:        backup.Success,
			Version:       Version,
			ServerVersion: config.ServerVersion,
			Nodes: map[string]*backup.NodeDescriptor{
				nodes[0]: {
					Classes: twoClassesReqcreq.Classes,
					Status:  backup.Success,
				},
				nodes[1]: {
					Classes: oneClassReq.Classes,
					Status:  backup.Success,
				},
			},
			Leader:          nodes[0],
			CompressionType: backup.CompressionGZIP,
		}
		assert.Equal(t, want, got)
	})

	t.Run("CanCommit", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(&CanCommitResponse{}, nil)
		fc.client.On("Abort", any, nodes[0], abortReq).Return(ErrAny)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)

		coordinator := *fc.coordinator()
		req := newReq(classes, backendName, backupID)
		store := coordStore{objectStore: objectStore{fc.backend, req.ID, "", "", ""}}
		err := coordinator.Backup(ctx, store, &req)
		assert.ErrorIs(t, err, errCannotCommit)
		assert.Contains(t, err.Error(), nodes[1],
			"a generic refusal must say which participant refused")
	})

	t.Run("NodeDown", func(t *testing.T) {
		t.Parallel()
		var (
			fc          = newFakeCoordinator(nodeResolver)
			coordinator = *fc.coordinator()
			req         = newReq(classes, backendName, backupID)
			store       = coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
		)
		coordinator.timeoutNodeDown = 0
		mockBackendProvider := NewMockBackupBackendProvider(t)
		coordinator.backends = mockBackendProvider
		mockBackendProvider.EXPECT().BackupBackend(backendName, mock.Anything).Return(fc.backend, nil)
		fc.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{}).Twice()

		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, ErrAny)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)
		fc.client.On("Abort", any, nodes[1], abortReq).Return(nil)

		err := coordinator.Backup(ctx, store, &req)
		assert.Nil(t, err)
		<-fc.backend.doneChan

		got := fc.backend.glMeta
		assert.GreaterOrEqual(t, got.StartedAt, now)
		assert.Greater(t, got.CompletedAt, got.StartedAt)
		assert.Contains(t, got.Nodes[nodes[1]].Error, ErrAny.Error())
		want := backup.DistributedBackupDescriptor{
			StartedAt:     got.StartedAt,
			CompletedAt:   got.CompletedAt,
			ID:            backupID,
			Status:        backup.Failed,
			Error:         got.Nodes[nodes[1]].Error,
			Version:       Version,
			ServerVersion: config.ServerVersion,
			Nodes: map[string]*backup.NodeDescriptor{
				nodes[0]: {
					Classes: classes,
					Status:  backup.Success,
				},
				nodes[1]: {
					Classes: classes,
					Status:  backup.Failed,
					Error:   got.Nodes[nodes[1]].Error,
				},
			},
			Leader:          nodes[0],
			CompressionType: backup.CompressionGZIP,
		}
		assert.Equal(t, want, got)
	})

	t.Run("NodeDisconnect", func(t *testing.T) {
		t.Parallel()
		var (
			fc          = newFakeCoordinator(nodeResolver)
			coordinator = *fc.coordinator()
		)
		coordinator.timeoutNodeDown = 0
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(ErrAny)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)
		fc.client.On("Abort", any, nodes[1], abortReq).Return(nil)

		req := newReq(classes, backendName, backupID)
		store := coordStore{objectStore: objectStore{fc.backend, req.ID, "", "", ""}}
		err := coordinator.Backup(ctx, store, &req)
		assert.Nil(t, err)
		<-fc.backend.doneChan

		got := fc.backend.glMeta
		assert.GreaterOrEqual(t, got.StartedAt, now)
		assert.Greater(t, got.CompletedAt, got.StartedAt)
		assert.Contains(t, got.Nodes[nodes[0]].Error, ErrAny.Error())
		want := backup.DistributedBackupDescriptor{
			StartedAt:     got.StartedAt,
			CompletedAt:   got.CompletedAt,
			ID:            backupID,
			Status:        backup.Failed,
			Error:         got.Nodes[nodes[0]].Error,
			Version:       Version,
			ServerVersion: config.ServerVersion,
			Nodes: map[string]*backup.NodeDescriptor{
				nodes[1]: {
					Classes: classes,
					Status:  "",
				},
				nodes[0]: {
					Classes: classes,
					Status:  backup.Failed,
					Error:   got.Nodes[nodes[0]].Error,
				},
			},
			Leader:          nodes[0],
			CompressionType: backup.CompressionGZIP,
		}
		assert.Equal(t, want, got)
	})
}

func TestCoordinatedRestore(t *testing.T) {
	t.Parallel()
	var (
		now          = time.Now().UTC()
		backendName  = "s3"
		any          = mock.Anything
		backupID     = "1"
		path         = "backups/1"
		ctx          = context.Background()
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A", "Class-B"}
		nodeResolver = newFakeNodeResolver(nodes)
		genReq       = func() *backup.DistributedBackupDescriptor {
			return &backup.DistributedBackupDescriptor{
				StartedAt:     now,
				CompletedAt:   now.Add(time.Second).UTC(),
				ID:            backupID,
				Status:        backup.Success,
				Version:       Version,
				ServerVersion: config.ServerVersion,
				Nodes: map[string]*backup.NodeDescriptor{
					nodes[0]: {
						Classes: classes,
						Status:  backup.Success,
					},
					nodes[1]: {
						Classes: classes,
						Status:  backup.Success,
					},
				},
			}
		}
		creq = &Request{
			Method:   OpRestore,
			ID:       backupID,
			Backend:  backendName,
			Classes:  classes,
			Duration: _BookingPeriod,
			Compression: Compression{
				Level:         GzipDefaultCompression,
				CPUPercentage: DefaultCPUPercentage,
			},
		}
		cresp    = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq     = &StatusRequest{OpRestore, backupID, backendName, "", "", ""}
		sresp    = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
		abortReq = &AbortRequest{OpRestore, backupID, backendName, "", "", ""}
	)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)

		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		// Mock GetObject for cancellation check (no existing restore in progress)
		fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
		// PutMeta is called 3 times: initial (TRANSFERRING), Finalizing, and final (SUCCESS)
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Times(3)

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}

		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, genReq(), nil)
		assert.Nil(t, err)
	})

	t.Run("CanCommit", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator(nodeResolver)
		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(&CanCommitResponse{}, nil)
		fc.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		// Mock GetObject for cancellation check (no existing restore in progress)
		fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, genReq(), nil)
		assert.ErrorIs(t, err, errCannotCommit)
		assert.Contains(t, err.Error(), nodes[1],
			"a generic refusal must say which participant refused")
	})

	t.Run("PutInitialMeta", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator(nodeResolver)
		fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration
		})).Return(cresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		// Mock GetObject for cancellation check (no existing restore in progress)
		fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(ErrAny).Once()
		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)
		fc.client.On("Abort", any, nodes[1], abortReq).Return(nil)

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, genReq(), nil)
		assert.ErrorIs(t, err, ErrAny)
		assert.Contains(t, err.Error(), "initial")
	})
}

func TestCoordinatedRestoreWithNodeMapping(t *testing.T) {
	t.Parallel()
	var (
		now         = time.Now().UTC()
		backendName = "s3"
		any         = mock.Anything
		backupID    = "1"
		ctx         = context.Background()
		nodes       = []string{"Old-N1", "Old-N2"}
		newNodes    = []string{"New-N1", "New-N2"}
		classes     = []string{"Dedicated-Class-A", "Dedicated-Class-B"}
		nodeMapping = map[string]string{nodes[0]: newNodes[0], nodes[1]: newNodes[1]}
		genReq      = func() *backup.DistributedBackupDescriptor {
			return &backup.DistributedBackupDescriptor{
				StartedAt:     now,
				CompletedAt:   now.Add(time.Second).UTC(),
				ID:            backupID,
				Status:        backup.Success,
				Version:       Version,
				ServerVersion: config.ServerVersion,
				Nodes: map[string]*backup.NodeDescriptor{
					nodes[0]: {
						Classes: classes,
						Status:  backup.Success,
					},
					nodes[1]: {
						Classes: classes,
						Status:  backup.Success,
					},
				},
				NodeMapping:     nodeMapping,
				CompressionType: backup.CompressionGZIP,
			}
		}
		creq = &Request{
			Method:      OpRestore,
			ID:          backupID,
			Backend:     backendName,
			Classes:     classes,
			NodeMapping: nodeMapping,
			Duration:    _BookingPeriod,
			Compression: Compression{
				Level:         GzipDefaultCompression,
				CPUPercentage: DefaultCPUPercentage,
			},
		}
		cresp = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq  = &StatusRequest{OpRestore, backupID, backendName, "", "", ""}
		sresp = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		nodeResolverWithNodeMapping := newFakeNodeResolver(append(nodes, newNodes...))
		fc := newFakeCoordinator(nodeResolverWithNodeMapping)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes)

		fc.client.On("CanCommit", any, newNodes[0], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration &&
				len(r.NodeMapping) == len(creq.NodeMapping)
		})).Return(cresp, nil)
		fc.client.On("CanCommit", any, newNodes[1], mock.MatchedBy(func(r *Request) bool {
			return r.Method == creq.Method && r.ID == creq.ID && r.Backend == creq.Backend &&
				len(r.Classes) == len(creq.Classes) && r.Duration == creq.Duration &&
				len(r.NodeMapping) == len(creq.NodeMapping)
		})).Return(cresp, nil)

		fc.client.On("Commit", any, newNodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, newNodes[1], sReq).Return(nil)
		fc.client.On("Status", any, newNodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, newNodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
		// Mock GetObject for cancellation check (no existing restore in progress)
		fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
		// PutMeta is called 3 times: initial (TRANSFERRING), Finalizing, and final (SUCCESS)
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Times(3)

		coordinator := *fc.coordinator()
		descReq := genReq()
		store := coordStore{objectStore{fc.backend, descReq.ID, "", "", ""}}
		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, descReq, nil)
		assert.Nil(t, err)
	})
}

type fakeSelector struct {
	mock.Mock

	// Plain field so pre-existing restore tests pass without a mock.On call.
	reindexInFlightErr error
	// reindexCollections records what each gate call was scoped to, so an arm
	// that passes the wrong class list cannot pass silently.
	reindexCollections [][]string
	// reindexInFlightFor, when set, answers per scope, which is what tells a
	// cluster-wide question apart from one scoped to the caller's classes.
	reindexInFlightFor func(collections []string) error
}

func (s *fakeSelector) Shards(ctx context.Context, class string) ([]string, error) {
	args := s.Called(ctx, class)
	return args.Get(0).([]string), args.Error(1)
}

func (s *fakeSelector) ListClasses(ctx context.Context) []string {
	args := s.Called(ctx)
	return args.Get(0).([]string)
}

func (s *fakeSelector) Backupable(ctx context.Context, classes []string) error {
	args := s.Called(ctx, classes)
	return args.Error(0)
}

func (s *fakeSelector) RefuseIfAnyReindexInFlight(_ context.Context, collections []string) error {
	s.reindexCollections = append(s.reindexCollections, collections)
	if s.reindexInFlightFor != nil {
		return s.reindexInFlightFor(collections)
	}
	return s.reindexInFlightErr
}

type fakeCoordinator struct {
	selector fakeSelector
	client   fakeClient
	schema   fakeSchemaManger
	backend  *fakeBackend
	log      *logrus.Logger
	// logs lets a test wait for a goroutine whose decision leaves no other trace.
	logs         *test.Hook
	nodeResolver NodeResolver
}

func newFakeCoordinator(resolver NodeResolver) *fakeCoordinator {
	fc := fakeCoordinator{}
	fc.backend = newFakeBackend()
	fc.schema = fakeSchemaManger{}
	logger, hook := test.NewNullLogger()
	fc.log = logger
	fc.logs = hook
	fc.nodeResolver = resolver
	return &fc
}

type fakeNodeResolver struct {
	hosts  map[string]string
	leader string
}

func (r *fakeNodeResolver) AllHostnames() []string {
	hosts := make([]string, len(r.hosts))
	count := 0
	for _, host := range r.hosts {
		hosts[count] = host
		count++
	}
	return hosts[:count]
}

func (r *fakeNodeResolver) NodeHostname(nodeName string) (string, bool) {
	return r.hosts[nodeName], true
}

func (r *fakeNodeResolver) NodeCount() int {
	if r.hosts != nil {
		return len(r.hosts)
	}
	return 1
}

func (r *fakeNodeResolver) LeaderID() string {
	return r.leader
}

func (r *fakeNodeResolver) AllNames() []string {
	xs := make([]string, 0, len(r.hosts))
	for k := range r.hosts {
		xs = append(xs, k)
	}
	return xs
}

func newFakeNodeResolver(nodes []string) *fakeNodeResolver {
	hosts := make(map[string]string)
	for _, node := range nodes {
		hosts[node] = node
	}
	leader := ""
	if len(nodes) > 0 {
		leader = nodes[0]
	}
	return &fakeNodeResolver{hosts: hosts, leader: leader}
}

func (fc *fakeCoordinator) coordinator() *coordinator {
	c := newCoordinator(&fc.selector, &fc.client, &fc.schema, fc.log, fc.nodeResolver, nil)
	c.timeoutNextRound = time.Millisecond * 200
	return c
}

type fakeClient struct {
	mock.Mock
}

func (f *fakeClient) CanCommit(ctx context.Context, node string, req *Request) (*CanCommitResponse, error) {
	args := f.Called(ctx, node, req)
	if args.Get(0) != nil {
		return args.Get(0).(*CanCommitResponse), args.Error(1)
	}
	return nil, args.Error(1)
}

func (f *fakeClient) Commit(ctx context.Context, node string, req *StatusRequest) error {
	args := f.Called(ctx, node, req)
	return args.Error(0)
}

func (f *fakeClient) Status(ctx context.Context, node string, req *StatusRequest) (*StatusResponse, error) {
	args := f.Called(ctx, node, req)
	if args.Get(0) != nil {
		return args.Get(0).(*StatusResponse), args.Error(1)
	}
	return nil, args.Error(1)
}

func (f *fakeClient) Abort(ctx context.Context, node string, req *AbortRequest) error {
	args := f.Called(ctx, node, req)
	return args.Error(0)
}

func newReq(classes []string, backendName, backupID string) Request {
	return Request{
		ID:      backupID,
		Backend: backendName,
		Classes: classes,
		Compression: Compression{
			Level:         GzipDefaultCompression,
			CPUPercentage: DefaultCPUPercentage,
		},
	}
}

// newStagingOperation builds the state commit() picks up: one participant
// per node, already staging.
func newStagingOperation(backupID string, nodes ...string) *operation {
	op := newOperation(&backup.DistributedBackupDescriptor{
		ID:          backupID,
		NodeMapping: map[string]string{},
		Nodes:       map[string]*backup.NodeDescriptor{},
	})
	for i, node := range nodes {
		op.descriptor.Nodes[node] = &backup.NodeDescriptor{Classes: []string{fmt.Sprintf("Class%d", i+1)}}
		op.participants[node] = participantStatus{Status: backup.Transferring, LastTime: time.Now()}
	}
	return op
}

func TestCoordinatorCommitCancellation(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		backupID     = "test-backup"
		ctx          = context.Background()
		nodes        = []string{"N1", "N2"}
		nodeResolver = newFakeNodeResolver(nodes)
		any          = mock.Anything
	)

	t.Run("DetectCancelledStatusInCommit", func(t *testing.T) {
		fc := newFakeCoordinator(nodeResolver)
		coordinator := fc.coordinator()
		op := newStagingOperation(backupID, "N1", "N2")

		// Mock commitAll - Commit calls should succeed (no errors)
		// commitAll doesn't modify Participants on success, so they stay as Transferring
		fc.client.On("Commit", any, "N1", mock.Anything).Return(nil)
		fc.client.On("Commit", any, "N2", mock.Anything).Return(nil)

		// Mock queryAll - return cancelled status for N1, success for N2
		// This will be called in the retry loop and will update Participants
		// The Status response must have Status field set to backup.Cancelled
		cancelledStatusResp := &StatusResponse{
			Status: backup.Cancelled,
			Err:    "restore cancelled",
			ID:     backupID,
			Method: OpRestore,
		}
		successStatusResp := &StatusResponse{
			Status: backup.Success,
			Err:    "",
			ID:     backupID,
			Method: OpRestore,
		}
		fc.client.On("Status", any, "N1", mock.Anything).Return(cancelledStatusResp, nil)
		fc.client.On("Status", any, "N2", mock.Anything).Return(successStatusResp, nil)

		req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}
		node2Addr := map[string]string{"N1": "N1", "N2": "N2"}

		// Set a very short timeout to avoid waiting in the retry loop
		// retryAfter will be timeoutNextRound / 5 = 0.2ms, which is fine for testing
		coordinator.timeoutNextRound = 1 * time.Millisecond

		coordinator.commit(ctx, op, req, node2Addr, true, slotOwner{})

		// After commit, queryAll should have updated Participants with Cancelled status
		// Verify that queryAll was called and updated the status
		assert.Equal(t, backup.Cancelled, op.participants["N1"].Status, "N1 should have Cancelled status after queryAll")
		assert.Equal(t, "restore cancelled", op.participants["N1"].Reason, "N1 should have cancellation reason")
		assert.Equal(t, backup.Success, op.participants["N2"].Status, "N2 should have Success status")

		// The overall descriptor status should be Cancelled because N1 is Cancelled
		assert.Equal(t, backup.Cancelled, op.descriptor.Status, "Overall status should be Cancelled")
		assert.Contains(t, op.descriptor.Error, "restore cancelled", "Error message should contain cancellation reason")
	})

	t.Run("DetectCancelledStatusInQueryAll", func(t *testing.T) {
		fc := newFakeCoordinator(nodeResolver)
		coordinator := fc.coordinator()
		op := newStagingOperation(backupID, "N1")

		// Return cancelled status from node
		cancelledResp := &StatusResponse{
			Status: backup.Cancelled,
			Err:    "restore cancelled",
			ID:     backupID,
			Method: OpRestore,
		}
		fc.client.On("Status", any, "N1", mock.Anything).Return(cancelledResp, nil)

		req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}
		node2Addr := map[string]string{"N1": "N1"}

		nFailures := coordinator.queryAll(ctx, op, req, node2Addr)

		assert.Equal(t, 1, nFailures)
		assert.Equal(t, backup.Cancelled, op.participants["N1"].Status)
		assert.Equal(t, "restore cancelled", op.participants["N1"].Reason)
	})

	t.Run("DetectContextCanceledInCommitAll", func(t *testing.T) {
		fc := newFakeCoordinator(nodeResolver)
		coordinator := fc.coordinator()
		op := newStagingOperation(backupID, "N1")

		// Return context.Canceled error
		fc.client.On("Commit", any, "N1", mock.Anything).Return(context.Canceled)

		req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}
		node2Addr := map[string]string{"N1": "N1"}

		nFailures := coordinator.commitAll(ctx, op, req, node2Addr)

		assert.Equal(t, 1, nFailures)
		assert.Equal(t, backup.Cancelled, op.participants["N1"].Status)
		assert.Contains(t, op.participants["N1"].Reason, context.Canceled.Error())
	})

	t.Run("DetectCancelledStatusInQueryAllTimeout", func(t *testing.T) {
		fc := newFakeCoordinator(nodeResolver)
		coordinator := fc.coordinator()
		op := newStagingOperation(backupID, "N1")
		// An old timestamp is what makes queryAll treat the node as down.
		st := op.participants["N1"]
		st.LastTime = time.Now().Add(-10 * time.Minute)
		op.participants["N1"] = st

		// Return context.Canceled error
		fc.client.On("Status", any, "N1", mock.Anything).Return(nil, context.Canceled)

		req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}
		node2Addr := map[string]string{"N1": "N1"}

		// Set timeoutNodeDown to a small value for testing
		coordinator.timeoutNodeDown = 1 * time.Second

		nFailures := coordinator.queryAll(ctx, op, req, node2Addr)

		assert.Equal(t, 1, nFailures)
		assert.Equal(t, backup.Cancelled, op.participants["N1"].Status)
		assert.Contains(t, op.participants["N1"].Reason, context.Canceled.Error())
	})

	// A cancel landing after the last in-loop check must still reach the
	// stored descriptor, even though every participant reported success.
	{
		t.Run("CancelledOnTheSlotAfterTheLastPoll", func(t *testing.T) {
			fc := newFakeCoordinator(nodeResolver)
			coordinator := fc.coordinator()
			coordinator.timeoutNextRound = time.Millisecond
			op := newStagingOperation(backupID, "N1")

			_, slot := coordinator.lastOp.renew(backupID, "path", "", "")
			fc.client.On("Commit", any, "N1", mock.Anything).Return(nil)
			// The poll that ends staging is the last step before the outcome is
			// computed, so a cancel staged there lands in exactly that gap.
			var once sync.Once
			fc.client.On("Status", any, "N1", mock.Anything).
				Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).
				Run(func(mock.Arguments) {
					once.Do(func() {
						stamped, _ := coordinator.lastOp.claimOf(backupID).stamp(backup.Cancelling)
						assert.True(t, stamped)
					})
				})

			req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}
			coordinator.commit(ctx, op, req, map[string]string{"N1": "N1"}, true, slot)

			assert.Equal(t, backup.Cancelled, op.descriptor.Status,
				"a restore whose participants all succeeded was stored as staged after it was cancelled")
			assert.Equal(t, "restore canceled by user", op.descriptor.Error)
		})
	}
}

// TestCoordinator_TypesErrorFromRemoteErrKind verifies that a refused
// CanCommitResponse with ErrKind == CanCommitErrInFlightReindex is promoted
// to a typed backup.ErrBackupBlockedByInFlightReindex by the coordinator,
// so upstream `errors.Is` checks succeed across the RPC boundary. Older
// nodes that don't populate ErrKind must continue to surface as
// errCannotCommit for backward compatibility.
func TestCoordinator_TypesErrorFromRemoteErrKind(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		any         = mock.Anything
		backupID    = "type-err-test"
		ctx         = context.Background()
		nodes       = []string{"N1", "N2"}
		classes     = []string{"Class-A"}
		// One participant always accepts so we can isolate the refusal path.
		acceptResp = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		// What a current-version participant sends.
		modernRefusal = backup.ErrBackupBlockedByInFlightReindex.Error() +
			`: collection "Class-A" has an active runtime-reindex task in DTM; retry after the migration finishes, ` +
			`or cancel it: GET /v1/schema/Class-A/indexes names the property and index type that are still migrating, ` +
			`and PUT /v1/schema/Class-A/indexes/{that property} with {"{that index type}":{"cancel":true}} cancels the task`
	)

	tests := []struct {
		name string
		// transportErr, when set, is the RPC error returned instead of a response.
		refusalResp     *CanCommitResponse
		transportErr    error
		expectInFlight  bool
		expectCanCommit bool
		expectContain   []string
		// Only the reindex refusals are composed to name no node, so only they
		// are safe to serve to a backup caller unprefixed. Every other refusal
		// keeps the node name: it is an operator-facing failure whose first
		// question is which node produced it.
		expectNodeNamed  bool
		expectLogLevel   logrus.Level
		expectLogContain string
	}{
		{
			name: "ErrKind=in_flight_reindex maps to typed sentinel",
			refusalResp: &CanCommitResponse{
				Method:  OpCreate,
				ID:      backupID,
				Err:     modernRefusal,
				ErrKind: CanCommitErrInFlightReindex,
			},
			expectInFlight: true,
			// Current-version wording is published as-is, node/shard-free.
			expectContain: []string{
				backup.ErrBackupBlockedByInFlightReindex.Error(),
				"active runtime-reindex task in DTM",
				"GET /v1/schema/Class-A/indexes",
				"PUT /v1/schema/Class-A/indexes/{that property}",
			},
			expectLogLevel: logrus.WarnLevel,
		},
		{
			name: "ErrKind=cannot_commit keeps legacy errCannotCommit",
			refusalResp: &CanCommitResponse{
				Method:  OpCreate,
				ID:      backupID,
				Err:     "some other refusal",
				ErrKind: CanCommitErrCannotCommit,
			},
			expectCanCommit: true,
			expectContain:   []string{"some other refusal"},
			expectNodeNamed: true,
			expectLogLevel:  logrus.WarnLevel,
		},
		{
			name: "empty ErrKind (older node) falls back to errCannotCommit",
			refusalResp: &CanCommitResponse{
				Method: OpCreate,
				ID:     backupID,
				// Err empty + ErrKind empty + Timeout == 0 still triggers
				// the refusal path; this models a buggy older participant
				// returning a zero-value response.
			},
			expectCanCommit: true,
			expectNodeNamed: true,
			expectLogLevel:  logrus.WarnLevel,
			// Empty resp.Err falls back to the typed error's text.
			expectLogContain: errCannotCommit.Error(),
		},
		{
			// No response to redact on, and "connection refused" alone is unactionable.
			name:            "a transport error names the node",
			refusalResp:     &CanCommitResponse{},
			transportErr:    errors.New("connection refused"),
			expectContain:   []string{"connection refused"},
			expectNodeNamed: true,
			// Unreachable participant is a cluster fault, so this leg pages.
			expectLogLevel: logrus.ErrorLevel,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodeResolver := newFakeNodeResolver(nodes)
			fc := newFakeCoordinator(nodeResolver)
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			fc.log = logger
			fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)

			// N1 (the leader) accepts; N2 refuses with the response shape under test.
			fc.client.On("CanCommit", any, nodes[0], mock.MatchedBy(func(r *Request) bool {
				return r.Method == OpCreate && r.ID == backupID
			})).Return(acceptResp, nil).Maybe()
			fc.client.On("CanCommit", any, nodes[1], mock.MatchedBy(func(r *Request) bool {
				return r.Method == OpCreate && r.ID == backupID
			})).Return(tc.refusalResp, tc.transportErr)

			// On refusal the coordinator aborts the participant that accepted.
			fc.client.On("Abort", any, nodes[0], mock.Anything).Return(nil).Maybe()
			fc.client.On("Abort", any, nodes[1], mock.Anything).Return(nil).Maybe()
			fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)

			coordinator := *fc.coordinator()
			req := newReq(classes, backendName, backupID)
			store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
			err := coordinator.Backup(ctx, store, &req)
			assert.Error(t, err)

			if tc.expectInFlight {
				assert.True(t, errors.Is(err, backup.ErrBackupBlockedByInFlightReindex),
					"expected errors.Is(err, backup.ErrBackupBlockedByInFlightReindex), got: %v", err)
				// errCannotCommit must NOT be in the chain when we have the
				// typed sentinel — keep the two paths cleanly separable.
				assert.False(t, errors.Is(err, errCannotCommit),
					"in-flight-reindex error must not also match errCannotCommit, got: %v", err)
			}
			if tc.expectCanCommit {
				assert.True(t, errors.Is(err, errCannotCommit),
					"expected errors.Is(err, errCannotCommit), got: %v", err)
				assert.False(t, errors.Is(err, backup.ErrBackupBlockedByInFlightReindex),
					"generic refusal must not match the typed sentinel, got: %v", err)
			}
			for _, want := range tc.expectContain {
				assert.Contains(t, err.Error(), want)
			}
			if tc.expectNodeNamed {
				assert.Contains(t, err.Error(), nodes[1],
					"a refusal that is not node-free by construction must say which node refused")
			} else {
				// The offending node reaches the operator through the log; this
				// error becomes the backup's failure reason, and a backup caller
				// is granted nothing on node names.
				assert.NotContains(t, err.Error(), nodes[1],
					"the refusal leaked the participant's node name")
			}

			var logged *logrus.Entry
			for _, e := range hook.AllEntries() {
				if e.Data["node"] == nodes[1] && strings.HasPrefix(e.Message, "canCommit ") {
					logged = e
				}
			}
			require.NotNil(t, logged, "the operator needs one entry naming the participant")
			assert.Equal(t, tc.expectLogLevel, logged.Level,
				"only a cluster fault pages; a refusal the caller can act on does not")
			if tc.expectLogContain != "" {
				assert.Contains(t, logged.Message, tc.expectLogContain,
					"the log line must carry the refusal text")
			}
		})
	}
}

// TestCoordinator_ARefusalDoesNotBlameHealthySiblings pins that a sibling's
// context cancellation after another node's refusal must not log at Error.
func TestCoordinator_ARefusalDoesNotBlameHealthySiblings(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		any         = mock.Anything
		backupID    = "sibling-blame-test"
		ctx         = context.Background()
		nodes       = []string{"N1", "N2", "N3"}
		classes     = []string{"Class-A"}
	)

	nodeResolver := newFakeNodeResolver(nodes)
	fc := newFakeCoordinator(nodeResolver)
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	fc.log = logger
	fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)

	// N3 stays in flight until context cancellation; sequenced to start
	// before N2's refusal lands.
	n3Started := make(chan struct{})
	fc.client.On("CanCommit", any, nodes[0], any).
		Return(&CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}, nil).Maybe()
	fc.client.On("CanCommit", any, nodes[2], any).Run(func(args mock.Arguments) {
		close(n3Started)
		<-args.Get(0).(context.Context).Done()
	}).Return(nil, context.Canceled).Once()
	fc.client.On("CanCommit", any, nodes[1], any).Run(func(mock.Arguments) {
		<-n3Started
	}).Return(&CanCommitResponse{
		Method:  OpCreate,
		ID:      backupID,
		Err:     backup.ErrBackupBlockedByInFlightReindex.Error() + `: collection "Class-A" has an active runtime-reindex task in DTM`,
		ErrKind: CanCommitErrInFlightReindex,
	}, nil).Once()

	fc.client.On("Abort", any, any, any).Return(nil).Maybe()
	fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)

	coordinator := *fc.coordinator()
	req := newReq(classes, backendName, backupID)
	store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
	require.Error(t, coordinator.Backup(ctx, store, &req))

	var refusals, errored int
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "canCommit refused by participant") {
			refusals++
			assert.Equal(t, logrus.WarnLevel, e.Level,
				"a caller-actionable refusal must not page the on-call")
			assert.Equal(t, nodes[1], e.Data["node"])
		}
		if e.Level <= logrus.ErrorLevel {
			errored++
			t.Logf("unexpected error entry: node=%v msg=%s", e.Data["node"], e.Message)
		}
	}
	assert.Equal(t, 1, refusals, "one refusal is one operator-facing entry")
	assert.Zero(t, errored,
		"the cancellation N3 saw is a consequence of N2's refusal, not a node failure")
}

// TestErrInFlightReindex_IsShared pins that the in-flight-reindex sentinel
// is a single value drawn from entities/backup, not duplicated in either
// the coordinator (usecases/backup) or the storage layer (adapters/repos/db).
//
// Catches the regression where someone re-introduces a parallel
// `var ErrBackupBlockedByInFlightReindex = errors.New(...)` in either
// layer: a parallel declaration would compare equal by string but fail
// pointer-identity, breaking errors.Is across the RPC seam.
//
// We verify identity from this package by:
//  1. Wrapping the shared sentinel through canCommitErrFromResponse — the
//     public coordinator path that consumes a remote CanCommitResponse.
//  2. Asserting errors.Is succeeds against backup.ErrBackupBlockedByInFlightReindex
//     (the entities/backup symbol).
//  3. Asserting the promoter states the condition once, not once per wrap,
//     when the participant's message already opens with the sentinel.
//
// Identity from the adapters/repos/db side is enforced by
// reindex_inflight_test.go, which calls errors.Is against the same shared
// symbol. Both layers therefore depend on the entities/backup value; a
// drift would make one layer's tests red.
func TestErrInFlightReindex_IsShared(t *testing.T) {
	t.Parallel()

	// Shared symbol must be non-nil and carry the expected operator text.
	require.NotNil(t, backup.ErrBackupBlockedByInFlightReindex)
	require.Equal(t,
		"backup blocked: runtime-reindex in flight",
		backup.ErrBackupBlockedByInFlightReindex.Error(),
		"operator-visible sentinel text is part of the contract; do not edit lightly",
	)

	// Round-trip through the coordinator's canCommit error promoter: a
	// CanCommitErrInFlightReindex response must produce an error chain
	// that errors.Is matches against the SHARED sentinel.
	resp := &CanCommitResponse{
		Method:  OpCreate,
		ID:      "shared-sentinel-id",
		Err:     "Node-2/Class-A: shard \"sa\" has 1 active tracker(s)",
		ErrKind: CanCommitErrInFlightReindex,
	}
	err := canCommitErrFromResponse(resp, []string{"Class-A"})
	require.Error(t, err)
	require.True(t, errors.Is(err, backup.ErrBackupBlockedByInFlightReindex),
		"coordinator must wrap the shared sentinel from entities/backup; "+
			"if this fails, a parallel declaration has been re-introduced")

	// A participant message that already opens with the sentinel must not
	// have it stated a second time by the promoter's wrap.
	resp.Err = backup.ErrBackupBlockedByInFlightReindex.Error() + `: collection "Movies" is migrating`
	err = canCommitErrFromResponse(resp, []string{"Class-A"})
	require.ErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)
	require.Equal(t, 1, strings.Count(err.Error(), backup.ErrBackupBlockedByInFlightReindex.Error()),
		"the condition must be stated once, not once per wrap")
}

// The participant composes its refusal to open with the sentinel, so wrapping
// it with %w again would state the condition twice in the operator's face.
func TestCanCommitErrFromResponse_StatesTheConditionOnce(t *testing.T) {
	t.Parallel()

	sentinel := backup.ErrBackupBlockedByInFlightReindex.Error()
	err := canCommitErrFromResponse(&CanCommitResponse{
		Method:  OpCreate,
		ID:      "dedupe-id",
		Err:     sentinel + `: collection "Class-A" has an active runtime-reindex task in DTM`,
		ErrKind: CanCommitErrInFlightReindex,
	}, []string{"Class-A"})
	require.Error(t, err)
	require.ErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)
	assert.Equal(t, 1, strings.Count(err.Error(), sentinel),
		"the participant already opened with the sentinel; wrapping repeats it")
}

// Pins: an older participant's node/shard-bearing wording must not leak into
// the redacted refusal.
func TestCanCommitErrFromResponse_DropsAnOlderParticipantsWording(t *testing.T) {
	t.Parallel()

	sentinel := backup.ErrBackupBlockedByInFlightReindex.Error()
	older := canCommitErrFromResponse(&CanCommitResponse{
		Method:  OpCreate,
		ID:      "mixed-version-id",
		Err:     `Node-1/Class-A: backup blocked: runtime-reindex in flight on this shard: shard "s1" (collection "Class-A")`,
		ErrKind: CanCommitErrInFlightReindex,
	}, []string{"Class-A"})

	require.ErrorIs(t, older, backup.ErrBackupBlockedByInFlightReindex)
	assert.Contains(t, older.Error(), sentinel)
	assert.Contains(t, older.Error(), `"Class-A"`,
		"the collection came from the caller's own request, so it stays")
	assert.NotContains(t, older.Error(), "Node-1",
		"a backup caller is granted nothing on node names")
	assert.NotContains(t, older.Error(), "s1",
		"a backup caller is granted nothing on shard names")
}

// The rebuilt refusal becomes an API error body, so a backup spanning hundreds
// of collections must not turn into a hundreds-of-names sentence.
func TestRedactedReindexRefusal_CapsTheClassList(t *testing.T) {
	t.Parallel()

	classes := make([]string, 0, redactedRefusalClassSample+5)
	for i := 0; i < cap(classes); i++ {
		classes = append(classes, fmt.Sprintf("Class-%02d", i))
	}

	msg := redactedReindexRefusal(classes)

	assert.Equal(t, redactedRefusalClassSample, strings.Count(msg, `"Class-`),
		"the quoted list is a sample, not the whole request")
	assert.Contains(t, msg, "Class-00")
	assert.NotContains(t, msg, "Class-10", "past the cap")
	assert.Contains(t, msg, fmt.Sprintf("and %d more", len(classes)-redactedRefusalClassSample),
		"the caller has to be told the list was cut")
}

// TestCommitAllManyFailures verifies commitAll does not deadlock when the number
// of participants exceeds the connection limit and they all fail. Each failing
// worker sends on errChan, but the consumer only runs after every worker is
// submitted; with an unbuffered channel the first _MaxNumberConns workers block
// on the send, holding all the errgroup slots so the submit loop can never reach
// the consumer.
func TestCommitAllManyFailures(t *testing.T) {
	t.Parallel()

	const numNodes = _MaxNumberConns * 2
	var (
		backendName = "s3"
		backupID    = "test-backup"
		ctx         = context.Background()
		any         = mock.Anything
	)

	nodes := make([]string, numNodes)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("N%d", i)
	}

	fc := newFakeCoordinator(newFakeNodeResolver(nodes))
	coordinator := fc.coordinator()
	op := newOperation(&backup.DistributedBackupDescriptor{ID: backupID})

	node2Addr := make(map[string]string, numNodes)
	for _, n := range nodes {
		op.participants[n] = participantStatus{
			Status:   backup.Transferring,
			LastTime: time.Now(),
		}
		node2Addr[n] = n
	}

	// Every commit fails, so every worker tries to send on errChan.
	fc.client.On("Commit", any, any, any).Return(errors.New("commit failed"))

	req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}

	done := make(chan int, 1)
	enterrors.GoWrapper(func() {
		done <- coordinator.commitAll(ctx, op, req, node2Addr)
	}, coordinator.log)

	select {
	case nFailures := <-done:
		assert.Equal(t, numNodes, nFailures)
	case <-time.After(10 * time.Second):
		t.Fatal("commitAll deadlocked with more failing participants than the connection limit")
	}
}

// Pins that a failed backup releases the slot, which is what future backups
// on this node are refused against.
func TestCoordinatorBackupReleasesSlotOnError(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		any          = mock.Anything
		backupID     = "1"
		ctx          = context.Background()
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A", "Class-B"}
		nodeResolver = newFakeNodeResolver(nodes)
		cresp        = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
	)

	tests := []struct {
		name    string
		level   CompressionLevel
		arrange func(fc *fakeCoordinator)
	}{
		{
			name:    "invalid compression level",
			level:   CompressionLevel(-1),
			arrange: func(*fakeCoordinator) {},
		},
		{
			name:  "participant refused to commit",
			level: GzipDefaultCompression,
			arrange: func(fc *fakeCoordinator) {
				fc.client.On("CanCommit", any, any, any).Return(nil, ErrAny)
				fc.client.On("Abort", any, any, any).Return(nil)
			},
		},
		{
			name:  "initial meta write failed",
			level: GzipDefaultCompression,
			arrange: func(fc *fakeCoordinator) {
				fc.client.On("CanCommit", any, any, any).Return(cresp, nil)
				fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(ErrAny).Once()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(nodeResolver)
			fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
			fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)
			fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
			tc.arrange(fc)

			c := fc.coordinator()
			req := newReq(classes, backendName, backupID)
			req.Level = tc.level
			store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}

			require.Error(t, c.Backup(ctx, store, &req))
			require.Empty(t, c.lastOp.get().ID, "slot still claimed after a failed backup")
		})
	}
}

// Pins that a restore refused for a CANCELLING backup gives back only its own
// slot, not one held by a different restore.
func TestCoordinatorRestoreCancellingReleasesOnlyItsOwnSlot(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		backupID     = "1"
		ctx          = context.Background()
		nodes        = []string{"N1", "N2"}
		nodeResolver = newFakeNodeResolver(nodes)
	)
	cancelling, err := json.Marshal(backup.DistributedBackupDescriptor{ID: backupID, Status: backup.Cancelling})
	require.NoError(t, err)

	tests := []struct {
		name        string
		claimedID   string
		claimStatus backup.Status
		wantSlotID  string
	}{
		{
			name:        "slot holds the cancelled backup",
			claimedID:   backupID,
			claimStatus: backup.Cancelled,
			wantSlotID:  "",
		},
		{
			name:        "slot holds a different, live restore",
			claimedID:   "live-restore",
			claimStatus: backup.Transferring,
			wantSlotID:  "live-restore",
		},
		{
			// The cancel was claimed by another coordinator, so this node's own
			// restore of the same id never saw a cancel and is still writing.
			name:        "slot holds a live restore carrying the same id",
			claimedID:   backupID,
			claimStatus: backup.Transferring,
			wantSlotID:  backupID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(nodeResolver)
			fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(cancelling, nil)

			c := fc.coordinator()
			_, slot := c.lastOp.renew(tc.claimedID, "path", "", "")
			slot.set(tc.claimStatus)

			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			desc := &backup.DistributedBackupDescriptor{ID: backupID, Status: backup.Cancelling}

			err := c.Restore(ctx, store, &req, desc, nil)
			require.ErrorContains(t, err, "cancellation in progress",
				"a refused restore must not be reported to the caller as started")
			require.ErrorContains(t, err, "repeat the cancel",
				"a descriptor stuck on CANCELLING is never cleared by waiting, only by repeating the cancel")
			// The slot is the subsystem's mutual exclusion: clearing one we do not
			// own lets a second restore claim it and run alongside the live one.
			// renew returns the holder it refused, which doubles as the read here.
			prevID, _ := c.lastOp.renew("intruder", "path", "", "")
			require.Equal(t, tc.wantSlotID, prevID,
				"a live restore must still refuse a second claim")
		})
	}
}

// TestCanCommitErrFromResponse pins the promotion of a refused CanCommit
// response back into a typed error, one row per kind. The kind decides the HTTP
// status the caller sees, so a row landing on the wrong arm turns a retryable
// refusal into a 500.
func TestCanCommitErrFromResponse(t *testing.T) {
	t.Parallel()
	const node = "N2"
	classes := []string{"Movies"}

	tests := []struct {
		name string
		resp *CanCommitResponse
		// wantIs are the sentinels the promoted error must carry.
		wantIs []error
		// wantNotIs are the ones it must not, so the arms stay separable.
		wantNotIs   []error
		wantContain string
		// wantNotContain guards the texts served to a backup caller, who is
		// granted neither node nor shard names.
		wantNotContain string
	}{
		{
			name:      "no response at all",
			resp:      nil,
			wantIs:    []error{errCannotCommit},
			wantNotIs: []error{backup.ErrBackupBlockedByInFlightReindex, backup.ErrReindexInFlight},
		},
		{
			name: "a backup refusal keeps the participant's wording",
			resp: &CanCommitResponse{
				ErrKind: CanCommitErrInFlightReindex,
				Err: backup.ErrBackupBlockedByInFlightReindex.Error() +
					`: collection "Movies" has an active runtime-reindex task in DTM`,
			},
			wantIs:      []error{backup.ErrBackupBlockedByInFlightReindex},
			wantNotIs:   []error{errCannotCommit, backup.ErrReindexInFlight},
			wantContain: "has an active runtime-reindex task in DTM",
		},
		{
			name: "a backup refusal an older node worded with its node name is rebuilt",
			resp: &CanCommitResponse{
				ErrKind: CanCommitErrInFlightReindex,
				Err:     "node " + node + ": shard S1 has an active runtime-reindex task",
			},
			wantIs:         []error{backup.ErrBackupBlockedByInFlightReindex},
			wantNotIs:      []error{errCannotCommit},
			wantContain:    `"Movies"`,
			wantNotContain: node,
		},
		{
			// The restore counterpart. It must not land on errCannotCommit:
			// the scheduler answers 422 for this and 500 for that.
			name: "a restore refusal carries the cluster-wide sentinel",
			resp: &CanCommitResponse{
				ErrKind: CanCommitErrRestoreBlockedByReindex,
				Err: "restore blocked: " + backup.ErrReindexInFlight.Error() +
					": retry after the migration finishes",
			},
			wantIs:      []error{backup.ErrReindexInFlight},
			wantNotIs:   []error{errCannotCommit, backup.ErrBackupBlockedByInFlightReindex},
			wantContain: "retry after the migration finishes",
		},
		{
			name:        "any other refusal stays unretryable",
			resp:        &CanCommitResponse{ErrKind: CanCommitErrCannotCommit, Err: "no space left on device"},
			wantIs:      []error{errCannotCommit},
			wantNotIs:   []error{backup.ErrBackupBlockedByInFlightReindex, backup.ErrReindexInFlight},
			wantContain: "no space left on device",
		},
		{
			name:        "an older node sends no kind at all",
			resp:        &CanCommitResponse{Err: "something went wrong"},
			wantIs:      []error{errCannotCommit},
			wantNotIs:   []error{backup.ErrBackupBlockedByInFlightReindex, backup.ErrReindexInFlight},
			wantContain: "something went wrong",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := canCommitErrFromResponse(tc.resp, classes)
			require.Error(t, err)
			for _, sentinel := range tc.wantIs {
				require.ErrorIs(t, err, sentinel)
			}
			for _, sentinel := range tc.wantNotIs {
				require.NotErrorIs(t, err, sentinel)
			}
			if tc.wantContain != "" {
				require.Contains(t, err.Error(), tc.wantContain)
			}
			if tc.wantNotContain != "" {
				require.NotContains(t, err.Error(), tc.wantNotContain)
			}
		})
	}
}
