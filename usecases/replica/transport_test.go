//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

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
	rawErr := replicaerrors.NewClassNotFoundError("Article", errors.New("error cannot be marshalled"))

	bytes, err := json.Marshal(rawErr)
	assert.Nil(t, err)

	got := &replicaerrors.Error{}
	assert.Nil(t, json.Unmarshal(bytes, got))

	assert.Equal(t, rawErr.Code, got.Code)
	assert.Equal(t, rawErr.Msg, got.Msg)
}
