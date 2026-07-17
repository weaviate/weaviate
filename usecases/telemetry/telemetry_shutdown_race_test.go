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

package telemetry

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
)

// TestStop_DuringStartupClusterIDWait exercises Stop() while Start() is still
// blocked inside the clusterId wait window. Start() then writes tel.clusterID
// (and, on push failure, tel.failedToStart) while the concurrent Stop() reads
// them via getFailedToStart and push(Terminate) -> buildPayload. Without the
// mutex those accesses are unsynchronized and `go test -race` reports a data
// race on tel.clusterID; with the mutex the accesses are ordered and the test
// passes clean.
//
// The wait window is made deterministic with the entered/release channels: the
// test only proceeds to Stop() once Start() has signaled it is inside the
// waiter, and only releases the waiter once Stop() is running, so the write and
// read genuinely overlap.
func TestStop_DuringStartupClusterIDWait(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sg := &fakeNodesStatusGetter{}
	sg.On("LocalNodeStatus", mock.Anything, "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{Stats: &models.NodeStats{ObjectCount: 1}})
	sm := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()

	entered := make(chan struct{})
	release := make(chan struct{})
	waiter := func(ctx context.Context) (string, error) {
		close(entered)
		select {
		case <-release:
			return "00000000-0000-7000-0000-0000000000ab", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	tel := New(sg, sm, logger, Config{}, waiter)
	tel.consumer = base64.StdEncoding.EncodeToString([]byte(server.URL))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = tel.Start(context.Background())
	}()

	<-entered

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(release)
	}()
	require.NoError(t, tel.Stop(context.Background()))

	wg.Wait()
}
