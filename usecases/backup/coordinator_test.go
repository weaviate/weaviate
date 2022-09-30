//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backup

import (
	"context"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCoordinatedBackup(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		any         = mock.Anything
		backupID    = "1"
		ctx         = context.Background()
		nodes       = []string{"N1", "N2"}
		classes     = []string{"Class-A", "Class-B"}
		req         = Request{
			ID:      backupID,
			Backend: backendName,
			Classes: classes,
		}
		now  = time.Now().UTC()
		creq = &Request{
			Method:   OpCreate,
			ID:       backupID,
			Backend:  backendName,
			Classes:  req.Classes,
			Duration: _BookingPeriod,
		}
		cresp    = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq     = &StatusRequest{OpCreate, backupID, backendName}
		sresp    = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
		abortReq = &AbortRequest{OpCreate, backupID, backendName}
	)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator()
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Once()
		coordinator := *fc.coordinator()
		err := coordinator.Backup(ctx, &req)
		assert.Nil(t, err)
		<-fc.backend.doneChan

		got := fc.backend.glMeta
		assert.GreaterOrEqual(t, got.StartedAt, now)
		assert.Greater(t, got.CompletedAt, got.StartedAt)
		want := backup.DistributedBackupDescriptor{
			StartedAt:     got.StartedAt,
			CompletedAt:   got.CompletedAt,
			ID:            backupID,
			Backend:       backendName,
			Status:        backup.Success,
			Version:       Version,
			ServerVersion: config.ServerVersion,
			Nodes: map[string]backup.NodeDescriptor{
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
		assert.Equal(t, want, got)
	})

	t.Run("Shards", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator()
		fc.selector.On("Shards", ctx, classes[0]).Return([]string{})
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes)
		coordinator := *fc.coordinator()
		err := coordinator.Backup(ctx, &req)
		assert.ErrorIs(t, err, errNoShardFound)
		assert.Contains(t, err.Error(), classes[0])
	})

	t.Run("CanCommit", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator()
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(&CanCommitResponse{}, nil)
		fc.client.On("Abort", any, nodes[0], abortReq).Return(ErrAny)

		coordinator := *fc.coordinator()
		err := coordinator.Backup(ctx, &req)
		assert.ErrorIs(t, err, errCannotCommit)
		assert.Contains(t, err.Error(), nodes[1])
	})

	t.Run("NodeDown", func(t *testing.T) {
		t.Parallel()
		var (
			fc          = newFakeCoordinator()
			coordinator = *fc.coordinator()
		)
		coordinator.timeoutNodeDown = 0
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, ErrAny)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Once()
		err := coordinator.Backup(ctx, &req)
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
			Backend:       backendName,
			Status:        backup.Failed,
			Error:         got.Nodes[nodes[1]].Error,
			Version:       Version,
			ServerVersion: config.ServerVersion,
			Nodes: map[string]backup.NodeDescriptor{
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
		}
		assert.Equal(t, want, got)
	})

	t.Run("NodeDisconnect", func(t *testing.T) {
		t.Parallel()
		var (
			fc          = newFakeCoordinator()
			coordinator = *fc.coordinator()
		)
		coordinator.timeoutNodeDown = 0
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(ErrAny)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Once()
		err := coordinator.Backup(ctx, &req)
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
			Backend:       backendName,
			Status:        backup.Failed,
			Error:         got.Nodes[nodes[0]].Error,
			Version:       Version,
			ServerVersion: config.ServerVersion,
			Nodes: map[string]backup.NodeDescriptor{
				nodes[1]: {
					Classes: classes,
					Status:  backup.Success,
				},
				nodes[0]: {
					Classes: classes,
					Status:  backup.Failed,
					Error:   got.Nodes[nodes[0]].Error,
				},
			},
		}
		assert.Equal(t, want, got)
	})
}

func TestCoordinatedRestore(t *testing.T) {
	t.Parallel()
	var (
		now         = time.Now().UTC()
		backendName = "s3"
		any         = mock.Anything
		backupID    = "1"
		ctx         = context.Background()
		nodes       = []string{"N1", "N2"}
		classes     = []string{"Class-A", "Class-B"}
		genReq      = func() *backup.DistributedBackupDescriptor {
			return &backup.DistributedBackupDescriptor{
				StartedAt:     now,
				CompletedAt:   now.Add(time.Second).UTC(),
				ID:            backupID,
				Backend:       backendName,
				Status:        backup.Success,
				Version:       Version,
				ServerVersion: config.ServerVersion,
				Nodes: map[string]backup.NodeDescriptor{
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
		}
		cresp    = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq     = &StatusRequest{OpRestore, backupID, backendName}
		sresp    = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
		abortReq = &AbortRequest{OpRestore, backupID, backendName}
	)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator()
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		coordinator := *fc.coordinator()
		err := coordinator.Restore(ctx, genReq())
		assert.Nil(t, err)
	})

	t.Run("CanCommit", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator()
		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(&CanCommitResponse{}, nil)
		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)

		coordinator := *fc.coordinator()
		err := coordinator.Restore(ctx, genReq())
		assert.ErrorIs(t, err, errCannotCommit)
		assert.Contains(t, err.Error(), nodes[1])
	})
}

type fakeSelector struct {
	mock.Mock
}

func (s *fakeSelector) Shards(ctx context.Context, class string) []string {
	args := s.Called(ctx, class)
	return args.Get(0).([]string)
}

func (s *fakeSelector) ListClasses(ctx context.Context) []string {
	args := s.Called(ctx)
	return args.Get(0).([]string)
}

type fakeCoordinator struct {
	selector fakeSelector
	client   fakeClient
	backend  *fakeBackend
	log      logrus.FieldLogger
}

func newFakeCoordinator() *fakeCoordinator {
	fc := fakeCoordinator{}
	fc.backend = newFakeBackend()
	logger, _ := test.NewNullLogger()
	fc.log = logger
	return &fc
}

func (fc *fakeCoordinator) coordinator() *coordinator {
	store := objectStore{fc.backend}
	c := NewCoordinator(store, &fc.selector, &fc.client, fc.log)
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
