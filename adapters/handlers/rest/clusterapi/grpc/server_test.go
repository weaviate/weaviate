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

package grpc_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	grpcconn "github.com/weaviate/weaviate/grpc/conn"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	"github.com/weaviate/weaviate/usecases/replica/types"
	grpcext "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func Test_ServerReplicationService(t *testing.T) {
	port := 8000
	host := fmt.Sprintf("localhost:%d", port)
	logger, _ := test.NewNullLogger()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := &state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				Cluster: cluster.Config{
					DataBindPort: port,
				},
				GRPC: config.GRPC{
					Port:         port,
					MaxMsgSize:   1024 * 1024,
					MaxOpenConns: 10,
				},
			},
		},
		Logger: logger,
	}

	for _, tt := range []struct {
		name         string
		auth         bool
		requestQueue bool
	}{
		{name: "no auth"},
		{name: "with auth", auth: true},
		{name: "with request queue", requestQueue: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			grpcDialOpts := []grpcext.DialOption{grpcext.WithTransportCredentials(insecure.NewCredentials())}

			if tt.auth {
				pass := "password"
				user := "username"

				state.ServerConfig.Config.Cluster.AuthConfig.BasicAuth.Password = pass
				state.ServerConfig.Config.Cluster.AuthConfig.BasicAuth.Username = user

				authHeader := grpcconn.BasicAuthHeader(user, pass)
				grpcDialOpts = append(grpcDialOpts, grpcext.WithChainUnaryInterceptor(grpcconn.BasicAuthUnaryInterceptor(authHeader)))
			} else {
				state.ServerConfig.Config.Cluster.AuthConfig.BasicAuth.Password = ""
				state.ServerConfig.Config.Cluster.AuthConfig.BasicAuth.Username = ""
			}

			if tt.requestQueue {
				state.ServerConfig.Config.Cluster.RequestQueueConfig.IsEnabled = configRuntime.NewDynamicValue(true)
				state.ServerConfig.Config.Cluster.RequestQueueConfig.NumWorkers = 10
				state.ServerConfig.Config.Cluster.RequestQueueConfig.QueueSize = 100
				state.ServerConfig.Config.Cluster.RequestQueueConfig.QueueFullHttpStatus = 503
				state.ServerConfig.Config.Cluster.RequestQueueConfig.QueueShutdownTimeoutSeconds = 90
			} else {
				state.ServerConfig.Config.Cluster.RequestQueueConfig.IsEnabled = configRuntime.NewDynamicValue(false)
			}

			mockReplicator := types.NewMockReplicator(t)

			s := grpc.NewServer(grpc.Config{
				State:                              state,
				Replicator:                         mockReplicator,
				MaintenanceModeEnabledForLocalhost: func() bool { return false },
				NodeReady:                          func() bool { return true },
			})

			manager, err := grpcconn.NewConnManager(10, time.Second, nil, logger, grpcDialOpts...)
			require.NoError(t, err)
			client := clients.NewGRPCReplicationClient(manager)

			go s.Serve()
			defer s.Close(ctx)

			t.Run("DigestObjects", func(t *testing.T) {
				c := "C"
				s := "S"
				ids := []strfmt.UUID{"id1", "id2"}
				mockReplicator.EXPECT().DigestObjects(mock.Anything, c, s, ids).Return([]routerTypes.RepairResponse{
					{ID: "id1"},
					{ID: "id2"},
				}, nil)
				resp, err := client.DigestObjects(context.Background(), host, c, s, ids, 9)
				require.NoError(t, err)
				require.Len(t, resp, 2)
				require.Equal(t, "id1", resp[0].ID)
				require.Equal(t, "id2", resp[1].ID)
			})

			t.Run("DigestObjectsInRange", func(t *testing.T) {
				c := "C"
				s := "S"
				initialUUID := strfmt.UUID("id1")
				finalUUID := strfmt.UUID("id2")
				limit := 10
				mockReplicator.EXPECT().DigestObjectsInRange(mock.Anything, c, s, initialUUID, finalUUID, limit).Return([]routerTypes.RepairResponse{
					{ID: "id1", Version: 1},
					{ID: "id2", Version: 2},
				}, nil)
				resp, err := client.DigestObjectsInRange(context.Background(), host, c, s, initialUUID, finalUUID, limit)
				require.NoError(t, err)
				require.Len(t, resp, 2)
				require.Equal(t, "id1", resp[0].ID)
				require.Equal(t, int64(1), resp[0].Version)
				require.Equal(t, "id2", resp[1].ID)
				require.Equal(t, int64(2), resp[1].Version)
			})

			t.Run("FindUUIDs", func(t *testing.T) {
				c := "C"
				s := "S"
				limit := 10
				mockReplicator.EXPECT().FindUUIDs(mock.Anything, c, s, (*filters.LocalFilter)(nil), limit).Return([]strfmt.UUID{"uuid1", "uuid2"}, nil)
				resp, err := client.FindUUIDs(context.Background(), host, c, s, nil, limit)
				require.NoError(t, err)
				require.Len(t, resp, 2)
				require.Equal(t, strfmt.UUID("uuid1"), resp[0])
				require.Equal(t, strfmt.UUID("uuid2"), resp[1])
			})

			t.Run("DeleteObject", func(t *testing.T) {
				c := "C"
				s := "S"
				requestID := "req-del"
				uuid := strfmt.UUID("delete-uuid")
				deletionTime := time.UnixMilli(1234567890000)
				var schemaVersion uint64 = 1
				mockReplicator.EXPECT().ReplicateDeletion(mock.Anything, c, s, requestID, uuid, deletionTime, schemaVersion).Return(replica.SimpleResponse{})
				resp, err := client.DeleteObject(context.Background(), host, c, s, requestID, uuid, deletionTime, schemaVersion)
				require.NoError(t, err)
				require.Empty(t, resp.Errors)
			})

			t.Run("DeleteObjects", func(t *testing.T) {
				c := "C"
				s := "S"
				requestID := "req-del-batch"
				uuids := []strfmt.UUID{"del1", "del2"}
				deletionTime := time.UnixMilli(1234567890000)
				dryRun := true
				var schemaVersion uint64 = 1
				mockReplicator.EXPECT().ReplicateDeletions(mock.Anything, c, s, requestID, uuids, deletionTime, dryRun, schemaVersion).Return(replica.SimpleResponse{})
				resp, err := client.DeleteObjects(context.Background(), host, c, s, requestID, uuids, deletionTime, dryRun, schemaVersion)
				require.NoError(t, err)
				require.Empty(t, resp.Errors)
			})

			t.Run("MergeObject", func(t *testing.T) {
				c := "C"
				s := "S"
				requestID := "req-merge"
				var schemaVersion uint64 = 1
				doc := &objects.MergeDocument{
					Class: "TestClass",
					ID:    "merge-id",
				}
				mockReplicator.EXPECT().ReplicateUpdate(mock.Anything, c, s, requestID, mock.Anything, schemaVersion).Return(replica.SimpleResponse{})
				resp, err := client.MergeObject(context.Background(), host, c, s, requestID, doc, schemaVersion)
				require.NoError(t, err)
				require.Empty(t, resp.Errors)
			})

			t.Run("AddReferences", func(t *testing.T) {
				c := "C"
				s := "S"
				requestID := "req-refs"
				var schemaVersion uint64 = 1
				refs := []objects.BatchReference{
					{OriginalIndex: 0, Tenant: "tenant1"},
				}
				mockReplicator.EXPECT().ReplicateReferences(mock.Anything, c, s, requestID, mock.Anything, schemaVersion).Return(replica.SimpleResponse{})
				resp, err := client.AddReferences(context.Background(), host, c, s, requestID, refs, schemaVersion)
				require.NoError(t, err)
				require.Empty(t, resp.Errors)
			})

			t.Run("Commit", func(t *testing.T) {
				c := "C"
				s := "S"
				requestID := "req-commit"
				mockReplicator.EXPECT().CommitReplication(mock.Anything, c, s, requestID).Return(replica.SimpleResponse{})
				var resp replica.SimpleResponse
				err := client.Commit(context.Background(), host, c, s, requestID, &resp)
				require.NoError(t, err)
			})

			t.Run("Abort", func(t *testing.T) {
				c := "C"
				s := "S"
				requestID := "req-abort"
				mockReplicator.EXPECT().AbortReplication(mock.Anything, c, s, requestID).Return(replica.SimpleResponse{})
				resp, err := client.Abort(context.Background(), host, c, s, requestID)
				require.NoError(t, err)
				require.Empty(t, resp.Errors)
			})

			t.Run("PutObject", func(t *testing.T) {
				c := "C"
				s := "S"
				requestID := "req-put"
				var schemaVersion uint64 = 1
				obj := storobj.FromObject(
					&models.Object{
						ID:                 "00000000-0000-0000-0000-000000000001",
						Class:              "TestClass",
						CreationTimeUnix:   1234567890000,
						LastUpdateTimeUnix: 1234567890000,
					},
					[]float32{1.0, 2.0, 3.0},
					nil,
					nil,
				)
				mockReplicator.EXPECT().ReplicateObject(mock.Anything, c, s, requestID, mock.Anything, schemaVersion).Return(replica.SimpleResponse{})
				resp, err := client.PutObject(context.Background(), host, c, s, requestID, obj, schemaVersion)
				require.NoError(t, err)
				require.Empty(t, resp.Errors)
			})

			t.Run("PutObjects", func(t *testing.T) {
				c := "C"
				s := "S"
				requestID := "req-put-batch"
				var schemaVersion uint64 = 1
				objs := []*storobj.Object{
					storobj.FromObject(
						&models.Object{
							ID:                 "00000000-0000-0000-0000-000000000001",
							Class:              "TestClass",
							CreationTimeUnix:   1234567890000,
							LastUpdateTimeUnix: 1234567890000,
						},
						[]float32{1.0, 2.0},
						nil,
						nil,
					),
				}
				mockReplicator.EXPECT().ReplicateObjects(mock.Anything, c, s, requestID, mock.Anything, schemaVersion).Return(replica.SimpleResponse{})
				resp, err := client.PutObjects(context.Background(), host, c, s, requestID, objs, schemaVersion)
				require.NoError(t, err)
				require.Empty(t, resp.Errors)
			})

			t.Run("FetchObject", func(t *testing.T) {
				c := "C"
				s := "S"
				uuid := strfmt.UUID("fetch-id")
				expectedReplica := replica.Replica{
					ID:                      uuid,
					Deleted:                 true,
					LastUpdateTimeUnixMilli: 1234567890000,
				}
				mockReplicator.EXPECT().FetchObject(mock.Anything, c, s, uuid).Return(expectedReplica, nil)
				resp, err := client.FetchObject(context.Background(), host, c, s, uuid, nil, additional.Properties{}, 0)
				require.NoError(t, err)
				require.Equal(t, uuid, resp.ID)
				require.True(t, resp.Deleted)
				require.Equal(t, int64(1234567890000), resp.LastUpdateTimeUnixMilli)
			})

			t.Run("FetchObjects", func(t *testing.T) {
				c := "C"
				s := "S"
				ids := []strfmt.UUID{"fetch-id-1", "fetch-id-2"}
				expectedReplicas := []replica.Replica{
					{ID: "fetch-id-1", Deleted: true, LastUpdateTimeUnixMilli: 100},
					{ID: "fetch-id-2", Deleted: false, LastUpdateTimeUnixMilli: 200},
				}
				mockReplicator.EXPECT().FetchObjects(mock.Anything, c, s, ids).Return(expectedReplicas, nil)
				resp, err := client.FetchObjects(context.Background(), host, c, s, ids)
				require.NoError(t, err)
				require.Len(t, resp, 2)
				require.Equal(t, strfmt.UUID("fetch-id-1"), resp[0].ID)
				require.True(t, resp[0].Deleted)
				require.Equal(t, strfmt.UUID("fetch-id-2"), resp[1].ID)
				require.False(t, resp[1].Deleted)
			})

			t.Run("OverwriteObjects", func(t *testing.T) {
				c := "C"
				s := "S"
				vobjects := []*objects.VObject{
					{ID: "overwrite-id-1", Deleted: true, LastUpdateTimeUnixMilli: 100, Version: 1},
				}
				mockReplicator.EXPECT().OverwriteObjects(mock.Anything, c, s, mock.Anything).Return([]routerTypes.RepairResponse{
					{ID: "overwrite-id-1"},
				}, nil)
				resp, err := client.OverwriteObjects(context.Background(), host, c, s, vobjects)
				require.NoError(t, err)
				require.Len(t, resp, 1)
				require.Equal(t, "overwrite-id-1", resp[0].ID)
			})

			t.Run("HashTreeLevel", func(t *testing.T) {
				c := "C"
				s := "S"
				level := 1
				discriminant := hashtree.NewBitset(8)
				discriminant.Set(0)
				expectedDigests := []hashtree.Digest{
					{1, 2},
					{3, 4},
				}
				mockReplicator.EXPECT().HashTreeLevel(mock.Anything, c, s, level, mock.Anything).Return(expectedDigests, nil)
				resp, err := client.HashTreeLevel(context.Background(), host, c, s, level, discriminant)
				require.NoError(t, err)
				require.Len(t, resp, 2)
				require.Equal(t, hashtree.Digest{1, 2}, resp[0])
				require.Equal(t, hashtree.Digest{3, 4}, resp[1])
			})
		})
	}
}

func Test_ServerNodeNotReady(t *testing.T) {
	port := 8000
	host := fmt.Sprintf("localhost:%d", port)
	logger, _ := test.NewNullLogger()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := &state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				Cluster: cluster.Config{
					DataBindPort: port,
				},
				GRPC: config.GRPC{
					Port:         port,
					MaxMsgSize:   1024 * 1024,
					MaxOpenConns: 10,
				},
			},
		},
		Logger: logger,
	}

	mockReplicator := types.NewMockReplicator(t)

	s := grpc.NewServer(grpc.Config{
		State:                              state,
		Replicator:                         mockReplicator,
		MaintenanceModeEnabledForLocalhost: func() bool { return false },
		NodeReady:                          func() bool { return false },
	})

	manager, err := grpcconn.NewConnManager(10, time.Second, nil, logger, grpcext.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := clients.NewGRPCReplicationClient(manager)

	go s.Serve()
	defer s.Close(ctx)

	t.Run("DigestObjects returns error when node is not ready", func(t *testing.T) {
		c := "C"
		s := "S"
		ids := []strfmt.UUID{"id1", "id2"}
		_, err := client.DigestObjects(context.Background(), host, c, s, ids, 9)
		require.Error(t, err)
		require.Equal(t, codes.Unavailable, status.Code(err))
	})
}

func Test_ServerInMaintenanceMode(t *testing.T) {
	port := 8000
	host := fmt.Sprintf("localhost:%d", port)
	logger, _ := test.NewNullLogger()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := &state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				Cluster: cluster.Config{
					DataBindPort: port,
				},
				GRPC: config.GRPC{
					Port:         port,
					MaxMsgSize:   1024 * 1024,
					MaxOpenConns: 10,
				},
			},
		},
		Logger: logger,
	}

	mockReplicator := types.NewMockReplicator(t)

	s := grpc.NewServer(grpc.Config{
		State:                              state,
		Replicator:                         mockReplicator,
		MaintenanceModeEnabledForLocalhost: func() bool { return true },
		NodeReady:                          func() bool { return true },
	})

	manager, err := grpcconn.NewConnManager(10, time.Second, nil, logger, grpcext.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := clients.NewGRPCReplicationClient(manager)

	go s.Serve()
	defer s.Close(ctx)

	t.Run("DigestObjects returns error when node is in maintenance mode", func(t *testing.T) {
		c := "C"
		s := "S"
		ids := []strfmt.UUID{"id1", "id2"}
		_, err := client.DigestObjects(context.Background(), host, c, s, ids, 9)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}
