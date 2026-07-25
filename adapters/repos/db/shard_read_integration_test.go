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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/multi"
)

// TestShardReadsHonorCtxCancellation: batch read loops must abort on a cancelled ctx.
func TestShardReadsHonorCtxCancellation(t *testing.T) {
	ctx := context.Background()
	_, idx := testShard(t, ctx, "ShardReadCtxCancel")
	s := firstShard(t, idx)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "MultiObjectRawByID",
			call: func(ctx context.Context) error {
				_, err := s.MultiObjectRawByID(ctx, []strfmt.UUID{strfmt.UUID(uuid.NewString())})
				return err
			},
		},
		{
			name: "ObjectDigests",
			call: func(ctx context.Context) error {
				_, err := s.ObjectDigests(ctx, []multi.Identifier{{ID: uuid.NewString()}})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, tc.call(cancelledCtx), context.Canceled)
		})
	}
}
