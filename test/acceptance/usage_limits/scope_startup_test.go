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

package usage_limits

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/test/docker"
)

// TestUnsupportedScope_StartupFailure verifies the startup gate added in
// usecases/config/config_handler.go: containers booted with
// USAGE_LIMITS_SCOPE=cluster (or =namespace) must REFUSE to come up,
// because the API surface is reserved but the implementation is not yet
// wired. This guards future-Dirk's Namespaces work — operators can't
// silently rely on a contract Weaviate doesn't yet honor.
//
// We expect docker.New().Start(...) to return an error within a small
// timeout window, OR the container to exit quickly. Either signal
// constitutes "fail to boot".
func TestUnsupportedScope_StartupFailure(t *testing.T) {
	for _, scope := range []string{"cluster", "namespace"} {
		t.Run("scope="+scope, func(t *testing.T) {
			// Use a short timeout: a healthy container would come up in
			// well under 30s; a failing one will either exit or be killed
			// by the wait-for-ready logic in the testcontainer harness.
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			compose, err := docker.New().
				WithWeaviate().
				WithWeaviateEnv("USAGE_LIMITS_SCOPE", scope).
				Start(ctx)
			if compose != nil {
				defer compose.Terminate(context.Background())
			}
			// The container must NOT come up successfully. We don't pin the
			// exact failure shape (could be a connection refused, a docker
			// exit error, or a wait-strategy timeout) — what matters is
			// that the operator gets a clear "doesn't start" signal rather
			// than a server claiming to honor an unsupported scope.
			assert.Error(t, err, "USAGE_LIMITS_SCOPE=%s must cause startup to fail", scope)
		})
	}
}
