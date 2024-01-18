//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
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
		now         = time.Now().UTC()
		req         = newReq(classes, backendName, backupID)
		creq        = &Request{
			Method:      OpCreate,
			ID:          backupID,
			Backend:     backendName,
			Classes:     req.Classes,
			Duration:    _BookingPeriod,
			Compression: req.Compression,
		}
		cresp        = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq         = &StatusRequest{OpCreate, backupID, backendName}
		sresp        = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
		abortReq     = &AbortRequest{OpCreate, backupID, backendName}
		nodeResolver = newFakeNodeResolver(nodes)
	)

	t.Run("PutMeta", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(ErrAny).Once()

		coordinator := *fc.coordinator()
		req := newReq(classes, backendName, backupID)
		store := coordStore{objStore{fc.backend, req.ID}}
		err := coordinator.Backup(ctx, store, &req)
		assert.NotNil(t, err)
	})

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		coordinator := *fc.coordinator()
		req := newReq(classes, backendName, backupID)
		store := coordStore{objStore{fc.backend, req.ID}}
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
		}
		assert.Equal(t, want, got)
	})

	t.Run("SuccessOnShardsEmptyPhysical", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return([]string{}, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		creqWithOneClass := &Request{
			Method:   OpCreate,
			ID:       backupID,
			Backend:  backendName,
			Classes:  []string{classes[1]},
			Duration: _BookingPeriod,
			Compression: Compression{
				Level:         DefaultCompression,
				ChunkSize:     DefaultChunkSize,
				CPUPercentage: DefaultCPUPercentage,
			},
		}
		fc.client.On("CanCommit", any, nodes[0], creqWithOneClass).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creqWithOneClass).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		coordinator := *fc.coordinator()
		req := newReq(classes, backendName, backupID)
		store := coordStore{objStore{fc.backend, req.ID}}
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
					Classes: []string{classes[1]},
					Status:  backup.Success,
				},
				nodes[1]: {
					Classes: []string{classes[1]},
					Status:  backup.Success,
				},
			},
		}
		assert.Equal(t, want, got)
	})

	t.Run("FailOnShardWithNoNodes", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return([]string{}, fmt.Errorf("a shard has no nodes"))
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)
		coordinator := *fc.coordinator()
		req := newReq(classes, backendName, backupID)
		store := coordStore{objStore: objStore{fc.backend, req.ID}}
		err := coordinator.Backup(ctx, store, &req)
		assert.ErrorIs(t, err, errNoShardFound)
		assert.Contains(t, err.Error(), classes[0])
	})

	t.Run("CanCommit", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(&CanCommitResponse{}, nil)
		fc.client.On("Abort", any, nodes[0], abortReq).Return(ErrAny)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)

		coordinator := *fc.coordinator()
		req := newReq(classes, backendName, backupID)
		store := coordStore{objStore: objStore{fc.backend, req.ID}}
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
			store       = coordStore{objStore{fc.backend, req.ID}}
		)
		coordinator.timeoutNodeDown = 0
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, ErrAny)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)
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

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.client.On("Commit", any, nodes[0], sReq).Return(ErrAny)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)
		fc.client.On("Abort", any, nodes[1], abortReq).Return(nil)

		req := newReq(classes, backendName, backupID)
		store := coordStore{objStore: objStore{fc.backend, req.ID}}
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
				Level:         DefaultCompression,
				ChunkSize:     DefaultChunkSize,
				CPUPercentage: DefaultCPUPercentage,
			},
		}
		cresp    = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq     = &StatusRequest{OpRestore, backupID, backendName}
		sresp    = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
		abortReq = &AbortRequest{OpRestore, backupID, backendName}
	)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		fc := newFakeCoordinator(nodeResolver)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)

		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)

		fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
		fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Twice()

		coordinator := *fc.coordinator()
		store := coordStore{objStore{fc.backend, backupID}}

		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, genReq())
		assert.Nil(t, err)
	})

	t.Run("CanCommit", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator(nodeResolver)
		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(&CanCommitResponse{}, nil)
		fc.backend.On("HomeDir", mock.Anything).Return(path)
		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)

		coordinator := *fc.coordinator()
		store := coordStore{objStore{fc.backend, backupID}}
		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, genReq())
		assert.ErrorIs(t, err, errCannotCommit)
		assert.Contains(t, err.Error(), nodes[1])
	})

	t.Run("PutInitialMeta", func(t *testing.T) {
		t.Parallel()

		fc := newFakeCoordinator(nodeResolver)
		fc.client.On("CanCommit", any, nodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, nodes[1], creq).Return(cresp, nil)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(ErrAny).Once()
		fc.client.On("Abort", any, nodes[0], abortReq).Return(nil)
		fc.client.On("Abort", any, nodes[1], abortReq).Return(nil)

		coordinator := *fc.coordinator()
		store := coordStore{objStore{fc.backend, backupID}}
		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, genReq())
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
				NodeMapping: nodeMapping,
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
				Level:         DefaultCompression,
				ChunkSize:     DefaultChunkSize,
				CPUPercentage: DefaultCPUPercentage,
			},
		}
		cresp = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq  = &StatusRequest{OpRestore, backupID, backendName}
		sresp = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		nodeResolverWithNodeMapping := newFakeNodeResolver(append(nodes, newNodes...))
		fc := newFakeCoordinator(nodeResolverWithNodeMapping)
		fc.selector.On("Shards", ctx, classes[0]).Return(nodes)
		fc.selector.On("Shards", ctx, classes[1]).Return(nodes)

		fc.client.On("CanCommit", any, newNodes[0], creq).Return(cresp, nil)
		fc.client.On("CanCommit", any, newNodes[1], creq).Return(cresp, nil)

		fc.client.On("Commit", any, newNodes[0], sReq).Return(nil)
		fc.client.On("Commit", any, newNodes[1], sReq).Return(nil)
		fc.client.On("Status", any, newNodes[0], sReq).Return(sresp, nil)
		fc.client.On("Status", any, newNodes[1], sReq).Return(sresp, nil)
		fc.backend.On("HomeDir", backupID).Return("bucket/" + backupID)
		fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Twice()

		coordinator := *fc.coordinator()
		descReq := genReq()
		store := coordStore{objStore{fc.backend, descReq.ID}}
		req := newReq([]string{}, backendName, "")
		err := coordinator.Restore(ctx, store, &req, descReq)
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
	backend      *fakeBackend
	log          logrus.FieldLogger
	nodeResolver nodeResolver
}

func newFakeCoordinator(resolver nodeResolver) *fakeCoordinator {
	fc := fakeCoordinator{}
	fc.backend = newFakeBackend()
	logger, _ := test.NewNullLogger()
	fc.log = logger
	fc.nodeResolver = resolver
	return &fc
}

type fakeNodeResolver struct {
	hosts map[string]string
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

func newFakeNodeResolver(nodes []string) *fakeNodeResolver {
	hosts := make(map[string]string)
	for _, node := range nodes {
		hosts[node] = node
	}
	return &fakeNodeResolver{hosts: hosts}
}

func (fc *fakeCoordinator) coordinator() *coordinator {
	c := newCoordinator(&fc.selector, &fc.client, fc.log, fc.nodeResolver)
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
			Level:         DefaultCompression,
			ChunkSize:     DefaultChunkSize,
			CPUPercentage: DefaultCPUPercentage,
		},
	}
}
