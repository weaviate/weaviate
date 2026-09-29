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

package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/backup"
)

func TestCanCommitDoesNotEnforceCallerContext(t *testing.T) {
	want := backup.CanCommitResponse{Method: backup.OpCreate, ID: "bak", Timeout: 50 * time.Second}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, pathCanCommit, r.URL.Path)
		require.NoError(t, json.NewEncoder(w).Encode(want))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := NewClusterBackups(server.Client()).CanCommit(ctx, server.Listener.Addr().String(),
		&backup.Request{Method: backup.OpCreate, ID: "bak"})
	require.NoError(t, err)
	require.Equal(t, want, *resp)
}
