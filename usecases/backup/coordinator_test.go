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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/entities/backup"
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
		sReq         = &StatusRequest{OpCreate, backupID, backendName, "", ""}
		sresp        = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
		abortReq     = &AbortRequest{OpCreate, backupID, backendName, "", ""}
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
		store := coordStore{objectStore{fc.backend, req.ID, "", ""}}
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
		mockBackendProvider.EXPECT().BackupBackend(backendName).Return(fc.backend, nil)
		bytes := marshalMeta(backup.BackupDescriptor{Status: string(backup.Success)})
		fc.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bytes, nil).Twice()

		req := newReq(classes, backendName, backupID)
		store := coordStore{objectStore{fc.backend, req.ID, "", ""}}
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
		mockBackendProvider.EXPECT().BackupBackend(backendName).Return(fc.backend, nil)
		bytes := marshalMeta(backup.BackupDescriptor{Status: string(backup.Success)})
		fc.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bytes, nil).Twice()

		req := newReq(classes, backendName, backupID)
		store := coordStore{objectStore{fc.backend, req.ID, "", ""}}
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
		store := coordStore{objectStore: objectStore{fc.backend, req.ID, "", ""}}
		err := coordinator.Backup(ctx, store, &req)
		assert.ErrorIs(t, err, errCannotCommit)
		assert.Contains(t, err.Error(), nodes[1])
	})

	t.Run("NodeDown", func(t *testing.T) {
		t.Parallel()
		var (
			fc          = newFakeCoordinator(nodeResolver)
			coordinator = *fc.coordinator()
			req         = newReq(classes, backendName, backupID)
			store       = coordStore{objectStore{fc.backend, req.ID, "", ""}}
		)
		coordinator.timeoutNodeDown = 0
		mockBackendProvider := NewMockBackupBackendProvider(t)
		coordinator.backends = mockBackendProvider
		mockBackendProvider.EXPECT().BackupBackend(backendName).Return(fc.backend, nil)
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
		store := coordStore{objectStore: objectStore{fc.backend, req.ID, "", ""}}
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
		sReq     = &StatusRequest{OpRestore, backupID, backendName, "", ""}
		sresp    = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
		abortReq = &AbortRequest{OpRestore, backupID, backendName, "", ""}
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
		// PutMeta is called 3 times: initial (TRANSFERRING), Finalizing, and final (SUCCESS)
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Times(3)

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", ""}}

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
		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", ""}}
		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, genReq(), nil)
		assert.ErrorIs(t, err, errCannotCommit)
		assert.Contains(t, err.Error(), nodes[1])
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
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(ErrAny).Once()
		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)
		fc.client.On("Abort", any, nodes[1], abortReq).Return(nil)

		coordinator := *fc.coordinator()
		store := coordStore{objectStore{fc.backend, backupID, "", ""}}
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
		sReq  = &StatusRequest{OpRestore, backupID, backendName, "", ""}
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
		// PutMeta is called 3 times: initial (TRANSFERRING), Finalizing, and final (SUCCESS)
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Times(3)

		coordinator := *fc.coordinator()
		descReq := genReq()
		store := coordStore{objectStore{fc.backend, descReq.ID, "", ""}}
		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, descReq, nil)
		assert.Nil(t, err)
	})
}

type fakeSelector struct {
	mock.Mock
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

type fakeCoordinator struct {
	selector     fakeSelector
	client       fakeClient
	schema       fakeSchemaManger
	backend      *fakeBackend
	log          logrus.FieldLogger
	nodeResolver NodeResolver
}

func newFakeCoordinator(resolver NodeResolver) *fakeCoordinator {
	fc := fakeCoordinator{}
	fc.backend = newFakeBackend()
	fc.schema = fakeSchemaManger{}
	logger, _ := test.NewNullLogger()
	fc.log = logger
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
		// Initialize descriptor with nodes that match participant node names
		// The node names in Nodes must match what ToOriginalNodeName will return
		coordinator.descriptor = &backup.DistributedBackupDescriptor{
			ID:          backupID,
			NodeMapping: make(map[string]string), // Empty mapping means ToOriginalNodeName returns node as-is
			Nodes: map[string]*backup.NodeDescriptor{
				"N1": {Classes: []string{"Class1"}},
				"N2": {Classes: []string{"Class2"}},
			},
		}

		// Pre-populate Participants - these must exist before commitAll/queryAll run
		// The node names must match the keys in node2Addr
		coordinator.Participants["N1"] = participantStatus{
			Status:   backup.Transferring,
			LastTime: time.Now(),
		}
		coordinator.Participants["N2"] = participantStatus{
			Status:   backup.Transferring,
			LastTime: time.Now(),
		}

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

		coordinator.commit(ctx, req, node2Addr, true)

		// After commit, queryAll should have updated Participants with Cancelled status
		// Verify that queryAll was called and updated the status
		assert.Equal(t, backup.Cancelled, coordinator.Participants["N1"].Status, "N1 should have Cancelled status after queryAll")
		assert.Equal(t, "restore cancelled", coordinator.Participants["N1"].Reason, "N1 should have cancellation reason")
		assert.Equal(t, backup.Success, coordinator.Participants["N2"].Status, "N2 should have Success status")

		// The overall descriptor status should be Cancelled because N1 is Cancelled
		assert.Equal(t, backup.Cancelled, coordinator.descriptor.Status, "Overall status should be Cancelled")
		assert.Contains(t, coordinator.descriptor.Error, "restore cancelled", "Error message should contain cancellation reason")
	})

	t.Run("DetectCancelledStatusInQueryAll", func(t *testing.T) {
		fc := newFakeCoordinator(nodeResolver)
		coordinator := fc.coordinator()
		coordinator.descriptor = &backup.DistributedBackupDescriptor{
			ID:          backupID,
			NodeMapping: make(map[string]string),
			Nodes: map[string]*backup.NodeDescriptor{
				"N1": {Classes: []string{"Class1"}},
			},
		}

		// Set up participant with initial status
		coordinator.Participants["N1"] = participantStatus{
			Status:   backup.Transferring,
			LastTime: time.Now(),
		}

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

		nFailures := coordinator.queryAll(ctx, req, node2Addr)

		assert.Equal(t, 1, nFailures)
		assert.Equal(t, backup.Cancelled, coordinator.Participants["N1"].Status)
		assert.Equal(t, "restore cancelled", coordinator.Participants["N1"].Reason)
	})

	t.Run("DetectContextCanceledInCommitAll", func(t *testing.T) {
		fc := newFakeCoordinator(nodeResolver)
		coordinator := fc.coordinator()
		coordinator.descriptor = &backup.DistributedBackupDescriptor{
			ID:          backupID,
			NodeMapping: make(map[string]string),
			Nodes: map[string]*backup.NodeDescriptor{
				"N1": {Classes: []string{"Class1"}},
			},
		}

		// Set up participant
		coordinator.Participants["N1"] = participantStatus{
			Status:   backup.Transferring,
			LastTime: time.Now(),
		}

		// Return context.Canceled error
		fc.client.On("Commit", any, "N1", mock.Anything).Return(context.Canceled)

		req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}
		node2Addr := map[string]string{"N1": "N1"}

		nFailures := coordinator.commitAll(ctx, req, node2Addr)

		assert.Equal(t, 1, nFailures)
		assert.Equal(t, backup.Cancelled, coordinator.Participants["N1"].Status)
		assert.Contains(t, coordinator.Participants["N1"].Reason, context.Canceled.Error())
	})

	t.Run("DetectCancelledStatusInQueryAllTimeout", func(t *testing.T) {
		fc := newFakeCoordinator(nodeResolver)
		coordinator := fc.coordinator()
		coordinator.descriptor = &backup.DistributedBackupDescriptor{
			ID:          backupID,
			NodeMapping: make(map[string]string),
			Nodes: map[string]*backup.NodeDescriptor{
				"N1": {Classes: []string{"Class1"}},
			},
		}

		// Set up participant with old timestamp to trigger timeout
		coordinator.Participants["N1"] = participantStatus{
			Status:   backup.Transferring,
			LastTime: time.Now().Add(-10 * time.Minute), // Old timestamp
		}

		// Return context.Canceled error
		fc.client.On("Status", any, "N1", mock.Anything).Return(nil, context.Canceled)

		req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}
		node2Addr := map[string]string{"N1": "N1"}

		// Set timeoutNodeDown to a small value for testing
		coordinator.timeoutNodeDown = 1 * time.Second

		nFailures := coordinator.queryAll(ctx, req, node2Addr)

		assert.Equal(t, 1, nFailures)
		assert.Equal(t, backup.Cancelled, coordinator.Participants["N1"].Status)
		assert.Contains(t, coordinator.Participants["N1"].Reason, context.Canceled.Error())
	})
}
