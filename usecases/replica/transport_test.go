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
	rplicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

func TestReplicationErrorTimeout(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now())
	defer cancel()
	err := &rplicaerrors.Error{Err: ctx.Err()}
	assert.True(t, err.Timeout())
	err = err.Clone()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReplicationErrorMarshal(t *testing.T) {
	rawErr := rplicaerrors.Error{Code: rplicaerrors.StatusClassNotFound, Msg: "Article", Err: errors.New("error cannot be marshalled")}
	bytes, err := json.Marshal(&rawErr)
	assert.Nil(t, err)
	got := rplicaerrors.NewError(0, "")
	assert.Nil(t, json.Unmarshal(bytes, got))
	want := &rplicaerrors.Error{Code: rplicaerrors.StatusClassNotFound, Msg: "Article"}
	assert.Equal(t, want, got)
}

func TestReplicationErrorStatus(t *testing.T) {
	tests := []struct {
		code rplicaerrors.StatusCode
		desc string
	}{
		{-1, ""},
		{rplicaerrors.StatusOK, "ok"},
		{rplicaerrors.StatusClassNotFound, "class not found"},
		{rplicaerrors.StatusShardNotFound, "shard not found"},
		{rplicaerrors.StatusNotFound, "not found"},
		{rplicaerrors.StatusAlreadyExisted, "already existed"},
		{rplicaerrors.StatusConflict, "conflict"},
		{rplicaerrors.StatusPreconditionFailed, "precondition failed"},
		{rplicaerrors.StatusReadOnly, "read only"},
	}
	for _, test := range tests {
		got := rplicaerrors.StatusText(test.code)
		if got != test.desc {
			t.Errorf("StatusText(%d) want %v got %v", test.code, test.desc, got)
		}
	}
}
