//go:build !integrationTest

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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHaltForTransferContext(t *testing.T) {
	t.Run("uses configured timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		timeout := 30 * time.Second
		haltCtx, haltCancel := haltForTransferContext(ctx, timeout)
		defer haltCancel()

		deadline, ok := haltCtx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(timeout), deadline, time.Second)
	})
}
