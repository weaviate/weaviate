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

package cluster

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// recordingLeader is a leader that accepts every forwarded apply and keeps the last.
type recordingLeader struct {
	client
	last *cmd.ApplyRequest
}

func (r *recordingLeader) Apply(_ context.Context, _ string, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	r.last = req
	return &cmd.ApplyResponse{}, nil
}

func TestUnitPlacementReachesTheLeader(t *testing.T) {
	m := NewMockStore(t, "node-1", 0)
	heartbeatFromLeader(t, m.store)
	leader := &recordingLeader{}
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, leader)
	require.NoError(t, srv.AddDistributedTaskWithGroupsBarrier(context.Background(), "ns", "t", struct{}{},
		[]distributedtask.UnitSpec{{ID: "s1__n1", GroupID: "s1", Shard: "s1", Node: "n1"}}, true))

	var req cmd.AddDistributedTaskRequest
	require.NoError(t, json.Unmarshal(leader.last.SubCommand, &req))
	require.Len(t, req.UnitSpecs, 1)
	require.Equal(t, []string{"s1__n1", "s1", "s1", "n1"},
		[]string{req.UnitSpecs[0].Id, req.UnitSpecs[0].GroupId, req.UnitSpecs[0].Shard, req.UnitSpecs[0].Node})
}

func TestClassWriteFieldsReachTheLeader(t *testing.T) {
	cls := &models.Class{Class: "C"}
	tests := []struct {
		name       string
		write      func(srv *Raft) error
		wantFields []string
	}{
		{
			name: "update sends its fields",
			write: func(srv *Raft) error {
				_, err := srv.UpdateClass(context.Background(), cls, nil,
					cmd.ClassFieldUsingBlockMaxWAND, cmd.ClassFieldVectorConfig("v"))
				return err
			},
			wantFields: []string{cmd.ClassFieldUsingBlockMaxWAND, cmd.ClassFieldVectorConfig("v")},
		},
		{
			name: "replace sends none",
			write: func(srv *Raft) error {
				_, err := srv.ReplaceClass(context.Background(), cls, nil)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMockStore(t, "node-1", 0)
			heartbeatFromLeader(t, m.store)
			leader := &recordingLeader{}
			require.NoError(t, tt.write(NewRaft(mocks.NewMockNodeSelector(), m.store, leader)))

			require.Equal(t, cmd.ApplyRequest_TYPE_UPDATE_CLASS, leader.last.Type)
			var req cmd.UpdateClassRequest
			require.NoError(t, json.Unmarshal(leader.last.SubCommand, &req))
			require.Equal(t, tt.wantFields, req.FieldsToUpdate)
			require.Equal(t, "C", req.Class.Class)
		})
	}
}
