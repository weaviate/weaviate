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

package clusterapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// deletionIndex hands the test the context of the deletion that reaches it,
// and holds that deletion until the context ends. With release set it then
// holds it until release closes, the way a batch in flight delays a stop.
type deletionIndex struct {
	sharding.RemoteIndexIncomingRepo
	started chan context.Context
	release chan struct{}
}

func (i *deletionIndex) IncomingDeleteObjectsExpired(ctx context.Context, _ *enterrors.ErrorGroupWrapper,
	ec errorcompounder.ErrorCompounder, _ string, _, _ time.Time, _ func(int32), _ uint64,
) {
	i.started <- ctx
	<-ctx.Done()
	if i.release != nil {
		<-i.release
	}
	ec.Add(context.Cause(ctx))
}

type deletionRepo struct{ index *deletionIndex }

func (r deletionRepo) GetIndexForIncomingSharding(schema.ClassName) sharding.RemoteIndexIncomingRepo {
	return r.index
}

// deletionSchema counts the classes a deletion waits on. With block set it
// hands the test the waiting deletion's context and holds the wait until that
// context ends, the way a node lagging behind the class version does. With err
// set the wait fails.
type deletionSchema struct {
	block   bool
	err     error
	started chan context.Context
	calls   *atomic.Int32
}

func (s deletionSchema) ReadOnlyClassWithVersion(ctx context.Context, class string, _ uint64) (*models.Class, error) {
	s.calls.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	if s.block {
		s.started <- ctx
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	return &models.Class{Class: class}, nil
}

// ttlFailures reads weaviate_objects_ttl_deletion_db_failure_count.
func ttlFailures(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "weaviate_objects_ttl_deletion_db_failure_count" {
			return family.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatal("the ttl failure counter is not registered")
	return 0
}

// messagesAt returns the messages hook caught at level.
func messagesAt(hook *test.Hook, level logrus.Level) []string {
	var messages []string
	for _, entry := range hook.AllEntries() {
		if entry.Level == level {
			messages = append(messages, entry.Message)
		}
	}
	return messages
}

// startDeletion posts a deletion of classes to handler and requires it
// accepted.
func startDeletion(t *testing.T, ctx context.Context, handler *clusterapi.ObjectTTL, classes ...string) {
	t.Helper()
	payload := make([]objectttl.ObjectsExpiredPayload, 0, len(classes))
	for _, class := range classes {
		payload = append(payload, objectttl.ObjectsExpiredPayload{Class: class, ClassVersion: 1, Prop: "_creationTimeUnix"})
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(ctx, http.MethodPost,
		"/cluster/object_ttl/delete_expired", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.Expired().ServeHTTP(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)
}

// waitForDeletion returns the context of the deletion index holds.
func waitForDeletion(t *testing.T, started <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-started:
		return ctx
	case <-time.After(5 * time.Second):
		t.Fatal("deletion was never held")
		return nil
	}
}

type deletionHandles struct {
	cancelRequest  context.CancelFunc
	cancelShutdown context.CancelCauseFunc
	status         *objectttl.LocalStatus
}

// A deletion started by POST /cluster/object_ttl/delete_expired outlives its
// request, so the request ending leaves it running and server shutdown stops
// it. An abort stops it too. A stop logs at Warn and is not a failure.
func TestObjectTTLIncomingDeleteContext(t *testing.T) {
	shutdown := errors.New("server shutdown")

	tests := []struct {
		name string
		// blockOnSchema holds the deletion in its schema wait rather than in
		// the index.
		blockOnSchema bool
		// end runs once the deletion is held.
		end func(h deletionHandles)
		// wantCause is what the deletion's context ends with, empty while it
		// keeps running.
		wantCause string
	}{
		{
			name:      "server shutdown stops the deletion",
			end:       func(h deletionHandles) { h.cancelShutdown(shutdown) },
			wantCause: shutdown.Error(),
		},
		{
			name:      "abort stops the deletion",
			end:       func(h deletionHandles) { h.status.ResetRunning("aborted") },
			wantCause: "aborted",
		},
		{
			name:          "server shutdown stops the deletion waiting on the schema",
			blockOnSchema: true,
			end:           func(h deletionHandles) { h.cancelShutdown(shutdown) },
			wantCause:     shutdown.Error(),
		},
		{
			name: "request ending leaves the deletion running",
			end:  func(h deletionHandles) { h.cancelRequest() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			started := make(chan context.Context, 1)
			index := &deletionIndex{started: started}
			schemaWaits := &atomic.Int32{}
			classes := deletionSchema{block: tt.blockOnSchema, started: started, calls: schemaWaits}
			status := objectttl.NewLocalStatus()

			shutdownCtx, cancelShutdown := context.WithCancelCause(context.Background())
			defer cancelShutdown(nil)

			handler := clusterapi.NewObjectTTL(
				sharding.NewRemoteIndexIncoming(deletionRepo{index: index}, classes, nil),
				clusterapi.NewBasicAuthHandler(cluster.AuthConfig{}), logger, config.Config{}, status, shutdownCtx)

			failures := ttlFailures(t)
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			startDeletion(t, requestCtx, handler, "Foo", "Bar")
			deletionCtx := waitForDeletion(t, started)

			tt.end(deletionHandles{cancelRequest: cancelRequest, cancelShutdown: cancelShutdown, status: status})

			if tt.wantCause == "" {
				require.NoError(t, deletionCtx.Err())
				require.True(t, status.IsRunning())
				require.Equal(t, int32(1), schemaWaits.Load())
				status.ResetRunning("aborted")
				return
			}
			require.ErrorContains(t, context.Cause(deletionCtx), tt.wantCause)
			require.Eventually(t, func() bool { return !status.IsRunning() }, 5*time.Second, 10*time.Millisecond,
				"the deletion never finished")
			require.Equal(t, int32(1), schemaWaits.Load(), "a stopped deletion must not move on to the next class")

			// The log line is the deletion's last step before it clears the status,
			// but an abort clears the status first.
			require.Eventually(t, func() bool { return len(messagesAt(hook, logrus.WarnLevel)) > 0 },
				5*time.Second, 10*time.Millisecond, "the stop was never logged")
			warns := messagesAt(hook, logrus.WarnLevel)
			require.Len(t, warns, 1)
			assert.Contains(t, warns[0], "incoming ttl deletion on remote node stopped: ")
			assert.Contains(t, warns[0], tt.wantCause)
			assert.Empty(t, messagesAt(hook, logrus.ErrorLevel))
			assert.Equal(t, failures, ttlFailures(t), "a stop is not a failure")
		})
	}
}

// A deletion that fails on a live context is still a failure: it logs at Error
// and raises the failure count.
func TestObjectTTLIncomingDeleteFailure(t *testing.T) {
	logger, hook := test.NewNullLogger()
	classes := deletionSchema{err: errors.New("class not found"), calls: &atomic.Int32{}}
	handler := clusterapi.NewObjectTTL(
		sharding.NewRemoteIndexIncoming(deletionRepo{index: &deletionIndex{}}, classes, nil),
		clusterapi.NewBasicAuthHandler(cluster.AuthConfig{}), logger, config.Config{},
		objectttl.NewLocalStatus(), context.Background())

	failures := ttlFailures(t)
	startDeletion(t, context.Background(), handler, "Foo")

	require.Eventually(t, func() bool { return len(messagesAt(hook, logrus.ErrorLevel)) > 0 },
		5*time.Second, 10*time.Millisecond, "the failure was never logged")
	errs := messagesAt(hook, logrus.ErrorLevel)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "incoming ttl deletion on remote node failed: ")
	assert.Contains(t, errs[0], "class not found")
	assert.Empty(t, messagesAt(hook, logrus.WarnLevel))
	assert.Equal(t, failures+1, ttlFailures(t))
}

// An abort frees the status at once, so a new deletion can start while the
// aborted one winds down. The aborted one finishing must leave the new one
// running and holding the status.
func TestObjectTTLIncomingDeleteAbortedFinishLeavesNextRunning(t *testing.T) {
	logger, hook := test.NewNullLogger()
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	index := &deletionIndex{started: started, release: release}
	classes := deletionSchema{started: started, calls: &atomic.Int32{}}
	status := objectttl.NewLocalStatus()
	handler := clusterapi.NewObjectTTL(
		sharding.NewRemoteIndexIncoming(deletionRepo{index: index}, classes, nil),
		clusterapi.NewBasicAuthHandler(cluster.AuthConfig{}), logger, config.Config{}, status, context.Background())

	startDeletion(t, context.Background(), handler, "Foo")
	aborted := waitForDeletion(t, started)
	require.True(t, status.ResetRunning("aborted"))
	require.Error(t, aborted.Err())

	// The aborted deletion is held until release closes, so this one starts
	// while it winds down.
	startDeletion(t, context.Background(), handler, "Foo")
	next := waitForDeletion(t, started)

	close(release)
	require.Eventually(t, func() bool { return len(messagesAt(hook, logrus.WarnLevel)) > 0 },
		5*time.Second, 10*time.Millisecond, "the aborted deletion never finished")
	// The aborted deletion clears the status right after it logs.
	require.Never(t, func() bool { return next.Err() != nil }, 200*time.Millisecond, 10*time.Millisecond,
		"the aborted deletion finishing ended the next one")
	assert.True(t, status.IsRunning())

	require.True(t, status.ResetRunning("aborted"))
}
