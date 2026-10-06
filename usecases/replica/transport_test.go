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

package replica_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

// TestDeleteBatchResponseFirstError pins what promotes one failed slot to the host's commit error.
// A batch delete seeds every slot, so a slot no delete wrote to has to surface here or the
// replicated leg reports the host as having deleted everything.
func TestDeleteBatchResponseFirstError(t *testing.T) {
	seeded := replicaerrors.Error{
		Code: replicaerrors.StatusConflict,
		Msg:  "no delete reported an outcome for this object",
	}

	tests := []struct {
		name    string
		batch   []replica.UUID2Error
		wantErr bool
	}{
		{name: "a host that answered no slots has no error"},
		{
			name:  "every slot deleted",
			batch: []replica.UUID2Error{{UUID: "a"}, {UUID: "b"}},
		},
		{
			name:    "the last slot carries the seed",
			batch:   []replica.UUID2Error{{UUID: "a"}, {UUID: "b", Error: seeded}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := replica.DeleteBatchResponse{Batch: tt.batch}

			err := resp.FirstError()
			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, "no delete reported an outcome",
				"a slot no delete wrote to must reach the host's commit error")
		})
	}
}

func TestReplicationErrorTimeout(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now())
	defer cancel()
	err := &replicaerrors.Error{Err: ctx.Err()}
	assert.True(t, err.Timeout())
	err = err.Clone()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReplicationErrorMarshal(t *testing.T) {
	rawErr := replicaerrors.Error{Code: replicaerrors.StatusClassNotFound, Msg: "Article", Err: errors.New("error cannot be marshalled")}
	bytes, err := json.Marshal(&rawErr)
	assert.Nil(t, err)
	got := replicaerrors.NewError(0, "")
	assert.Nil(t, json.Unmarshal(bytes, got))
	want := &replicaerrors.Error{Code: replicaerrors.StatusClassNotFound, Msg: "Article"}
	assert.Equal(t, want, got)
}

func TestReplicationErrorStatus(t *testing.T) {
	tests := []struct {
		code replicaerrors.StatusCode
		desc string
	}{
		{-1, ""},
		{replicaerrors.StatusOK, "ok"},
		{replicaerrors.StatusClassNotFound, "class not found"},
		{replicaerrors.StatusShardNotFound, "shard not found"},
		{replicaerrors.StatusNotFound, "not found"},
		{replicaerrors.StatusAlreadyExisted, "already existed"},
		{replicaerrors.StatusConflict, "conflict"},
		{replicaerrors.StatusPreconditionFailed, "precondition failed"},
		{replicaerrors.StatusReadOnly, "read only"},
	}
	for _, test := range tests {
		got := replicaerrors.StatusText(test.code)
		if got != test.desc {
			t.Errorf("StatusText(%d) want %v got %v", test.code, test.desc, got)
		}
	}
}
