//go:build !integrationTest

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
