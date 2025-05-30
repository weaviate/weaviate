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

package replica_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/replica"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestReplicationErrorTimeout(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now())
	defer cancel()
	err := &replica.Error{Err: ctx.Err()}
	assert.True(t, err.Timeout())
	err = err.Clone()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReplicationErrorMarshal(t *testing.T) {
	rawErr := replica.Error{Code: replica.StatusClassNotFound, Msg: "Article", Err: errors.New("error cannot be marshalled")}
	bytes, err := json.Marshal(&rawErr)
	assert.Nil(t, err)
	got := replica.NewError(0, "")
	assert.Nil(t, json.Unmarshal(bytes, got))
	want := &replica.Error{Code: replica.StatusClassNotFound, Msg: "Article"}
	assert.Equal(t, want, got)
}

func TestReplicationErrorStatus(t *testing.T) {
	tests := []struct {
		code replica.StatusCode
		desc string
	}{
		{-1, ""},
		{replica.StatusOK, "ok"},
		{replica.StatusClassNotFound, "class not found"},
		{replica.StatusShardNotFound, "shard not found"},
		{replica.StatusNotFound, "not found"},
		{replica.StatusAlreadyExisted, "already existed"},
		{replica.StatusConflict, "conflict"},
		{replica.StatusPreconditionFailed, "precondition failed"},
		{replica.StatusReadOnly, "read only"},
	}
	for _, test := range tests {
		got := replica.StatusText(test.code)
		if got != test.desc {
			t.Errorf("StatusText(%d) want %v got %v", test.code, test.desc, got)
		}
	}
}
