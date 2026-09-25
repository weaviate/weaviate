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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ttlProbeTimeout bounds every wait in this test, so a window that never opens
// fails the test instead of hanging the package.
const ttlProbeTimeout = 10 * time.Second

// ttlCounterProbe keeps the first collection's delete goroutine calling
// countDeleted across the dispatch loop's next write to the counter map, with
// nothing ordering the two.
type ttlCounterProbe struct {
	sharding.RemoteIndexIncomingRepo
	dispatched atomic.Int32
	reading    chan struct{}
	stop       chan struct{}
	overlapped atomic.Bool
	counted    atomic.Int32
	timedOut   atomic.Bool
}

func newTTLCounterProbe() *ttlCounterProbe {
	return &ttlCounterProbe{reading: make(chan struct{}), stop: make(chan struct{})}
}

func (p *ttlCounterProbe) IncomingDeleteObjectsExpired(_ context.Context,
	eg *enterrors.ErrorGroupWrapper, _ errorcompounder.ErrorCompounder,
	_ string, _, _ time.Time, countDeleted func(int32), _ uint64,
) {
	switch p.dispatched.Add(1) {
	case 1:
		eg.Go(func() error {
			deadline := time.After(ttlProbeTimeout)
			close(p.reading)
			var count int32
			for {
				countDeleted(1)
				count++
				select {
				case <-p.stop:
					p.counted.Store(count)
					return nil
				case <-deadline:
					p.counted.Store(count)
					p.timedOut.Store(true)
					return errors.New("the dispatch loop never reached the next collection")
				default:
				}
			}
		})
		// hold the loop here until the reader is running, so the write that
		// follows lands inside the reader's window
		select {
		case <-p.reading:
			p.overlapped.Store(true)
		case <-time.After(ttlProbeTimeout):
		}
	case 2:
		// the second collection's entry is written, so the overlap under test has happened
		close(p.stop)
	}
}

type ttlProbeRepo struct {
	index sharding.RemoteIndexIncomingRepo
}

func (r ttlProbeRepo) GetIndexForIncomingSharding(schema.ClassName) sharding.RemoteIndexIncomingRepo {
	return r.index
}

type ttlProbeSchema struct{}

func (ttlProbeSchema) ReadOnlyClassWithVersion(_ context.Context, class string, _ uint64) (*models.Class, error) {
	return &models.Class{Class: class}, nil
}

// A delete goroutine must not read the counter map the dispatch loop writes to.
// The access is fatal rather than recoverable, and -race is what reports it.
func TestIncomingDeleteKeepsTheCounterMapOffTheDeleteGoroutines(t *testing.T) {
	const firstClass = "Collection0"

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	probe := newTTLCounterProbe()
	remoteIndex := sharding.NewRemoteIndexIncoming(ttlProbeRepo{index: probe}, ttlProbeSchema{}, nil)

	handler := clusterapi.NewObjectTTL(remoteIndex, clusterapi.NewNoopAuthHandler(), logger,
		config.Config{ObjectsTTLConcurrencyFactor: configRuntime.NewDynamicValue[float64](1)},
		objectttl.NewLocalStatus())

	server := httptest.NewServer(handler.Expired())
	defer server.Close()

	// two collections are enough. The loop writes the second entry while the
	// first collection's deletes are still running
	now := time.Now().UnixMilli()
	payload := []objectttl.ObjectsExpiredPayload{
		{Class: firstClass, Prop: "expiresAt", TtlMilli: now, DelMilli: now},
		{Class: "Collection1", Prop: "expiresAt", TtlMilli: now, DelMilli: now},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	resp, err := http.Post(server.URL+"/cluster/object_ttl/delete_expired",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// the handler returns 202 before the sweep runs, so wait for its log entry
	require.Eventually(t, func() bool {
		return ttlSweepReport(hook) != nil
	}, ttlProbeTimeout, 10*time.Millisecond, "the sweep must finish")

	require.EqualValues(t, len(payload), probe.dispatched.Load(),
		"the loop must reach every collection, or nothing wrote while the reader ran")
	require.True(t, probe.overlapped.Load(),
		"the reader must be running before the loop writes the next entry")
	require.False(t, probe.timedOut.Load(),
		"the reader hit its deadline, so the loop never reached the next collection")

	report := ttlSweepReport(hook)
	assert.Equal(t, probe.counted.Load(), report.Data["c_"+firstClass],
		"the collection is credited with what its own closure counted")
	assert.Equal(t, probe.counted.Load(), report.Data["total_deleted"])
}

func ttlSweepReport(hook *logrustest.Hook) *logrus.Entry {
	for _, entry := range hook.AllEntries() {
		if entry.Message == "incoming ttl deletion on remote node finished" {
			return entry
		}
	}
	return nil
}

// waitingTTLSchema stands in for the versioned schema read, which waits for the
// node to catch up and gives up on its own deadline. waitTimeout stands in for
// that deadline, and a zero one is a node already caught up.
type waitingTTLSchema struct {
	waitTimeout time.Duration
	entered     chan struct{}
	once        sync.Once
}

func (s *waitingTTLSchema) ReadOnlyClassWithVersion(ctx context.Context, class string, _ uint64) (*models.Class, error) {
	s.once.Do(func() { close(s.entered) })

	if s.waitTimeout == 0 && ctx.Err() == nil {
		return &models.Class{Class: class}, nil
	}
	if s.waitTimeout > 0 {
		timer := time.NewTimer(s.waitTimeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	// the real wait reports the version it never reached, carrying neither the
	// context's error nor its cause, whether it gave up or was cancelled
	return nil, fmt.Errorf("class %q: schema version not reached", class)
}

// sweptTTLRepo records the collections the sweep asked it for.
type sweptTTLRepo struct {
	lock  sync.Mutex
	asked []string
}

func (r *sweptTTLRepo) GetIndexForIncomingSharding(class schema.ClassName) sharding.RemoteIndexIncomingRepo {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.asked = append(r.asked, string(class))
	return noopTTLIndex{}
}

func (r *sweptTTLRepo) sweptCollections() []string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return append([]string(nil), r.asked...)
}

// noopTTLIndex embeds the interface so only the one method the sweep calls is
// implemented. It dispatches nothing, so the sweep's own flow is what a test sees.
type noopTTLIndex struct {
	sharding.RemoteIndexIncomingRepo
}

func (noopTTLIndex) IncomingDeleteObjectsExpired(context.Context, *enterrors.ErrorGroupWrapper,
	errorcompounder.ErrorCompounder, string, time.Time, time.Time, func(int32), uint64,
) {
}

// ttlTestServer serves the object_ttl endpoints over ttlSchema. The returned
// outcome reads the handler's log, because the deletion outlives the request.
func ttlTestServer(t *testing.T, ttlSchema sharding.RemoteIncomingSchema) (
	server *httptest.Server, repo *sweptTTLRepo, status *objectttl.LocalStatus,
	sweepOutcome func() (failed error, returned bool),
) {
	t.Helper()

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	repo = &sweptTTLRepo{}
	status = objectttl.NewLocalStatus()
	d := clusterapi.NewObjectTTL(sharding.NewRemoteIndexIncoming(repo, ttlSchema, nil),
		clusterapi.NewNoopAuthHandler(), logger,
		config.Config{ObjectsTTLConcurrencyFactor: configRuntime.NewDynamicValue[float64](1)},
		status)

	mux := http.NewServeMux()
	mux.Handle("/cluster/object_ttl/", d.Expired())
	server = httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return server, repo, status, func() (error, bool) {
		for _, e := range hook.AllEntries() {
			if !strings.HasPrefix(e.Message, "incoming ttl deletion on remote node f") {
				continue
			}
			failed, _ := e.Data[logrus.ErrorKey].(error)
			return failed, true
		}
		return nil, false
	}
}

func postTTLDelete(t *testing.T, server *httptest.Server, collections int) {
	t.Helper()

	payload := make([]objectttl.ObjectsExpiredPayload, collections)
	for i := range payload {
		payload[i] = objectttl.ObjectsExpiredPayload{
			Class:        fmt.Sprintf("Collection%d", i),
			ClassVersion: 1,
			Prop:         "expiresAt",
			TtlMilli:     time.Now().UnixMilli(),
			DelMilli:     time.Now().UnixMilli(),
		}
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	resp, err := http.Post(server.URL+"/cluster/object_ttl/delete_expired",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}

// An abort that cannot reach the schema wait leaves the sweep sitting out one
// schema deadline per collection in the body, holding the slot for all of them.
func TestIncomingDeleteAbortReachesTheSchemaWait(t *testing.T) {
	const (
		collections = 10
		schemaWait  = 500 * time.Millisecond
	)

	ttlSchema := &waitingTTLSchema{waitTimeout: schemaWait, entered: make(chan struct{})}
	server, _, status, sweepOutcome := ttlTestServer(t, ttlSchema)

	sweepReturned := func() bool { _, returned := sweepOutcome(); return returned }

	// the sweep is detached from the request, so let it drain before the test
	// returns even when an assertion below failed. The slot is released last, so
	// the log line this test reads elsewhere would let it through still running.
	t.Cleanup(func() {
		deadline := time.Now().Add(collections*schemaWait + time.Second)
		for status.IsRunning() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	})

	postTTLDelete(t, server, collections)

	select {
	case <-ttlSchema.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweep never reached the schema wait")
	}

	require.True(t, status.Abort(), "the sweep is running, so there is one to abort")

	require.Eventually(t, sweepReturned, 2*time.Second, 10*time.Millisecond,
		"an aborted sweep must return without sitting out a schema deadline per collection")

	failed, _ := sweepOutcome()
	require.ErrorIs(t, failed, objectttl.ErrAborted,
		"and report the abort, not the version the schema wait never reached")
}

// The context the schema wait runs under is the live one a sweep was started
// with, so an unaborted sweep still reaches every collection's index.
func TestIncomingDeleteSweepsEveryCollection(t *testing.T) {
	const collections = 3

	ttlSchema := &waitingTTLSchema{entered: make(chan struct{})}
	server, repo, status, sweepOutcome := ttlTestServer(t, ttlSchema)

	postTTLDelete(t, server, collections)

	// the slot is released by the outermost defer, so waiting on it means every
	// earlier one has run, including the one that logs the outcome read below
	require.Eventually(t, func() bool { return !status.IsRunning() },
		5*time.Second, 10*time.Millisecond,
		"the sweep must run to completion and hand the slot back")
	failed, _ := sweepOutcome()
	require.NoError(t, failed)
	require.Equal(t, []string{"Collection0", "Collection1", "Collection2"}, repo.sweptCollections())
}

// A malformed body has to hand back the slot the handler took to read it, or
// every later sweep on the node is refused.
func TestIncomingDeleteHandsBackTheSlotOnABadBody(t *testing.T) {
	server, _, status, _ := ttlTestServer(t, &waitingTTLSchema{entered: make(chan struct{})})

	resp, err := http.Post(server.URL+"/cluster/object_ttl/delete_expired",
		"application/json", strings.NewReader("not json"))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.False(t, status.IsRunning())

	postTTLDelete(t, server, 1)
}

// The abort endpoint reports the running deletion without releasing its slot,
// which only the deletion itself does once it has drained.
func TestIncomingAbortKeepsTheSlotReserved(t *testing.T) {
	server, _, status, _ := ttlTestServer(t, &waitingTTLSchema{entered: make(chan struct{})})

	postAbort := func() bool {
		t.Helper()
		resp, err := http.Post(server.URL+"/cluster/object_ttl/abort", "application/json", nil)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var body objectttl.ObjectsExpiredAbortResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		return body.Aborted
	}

	require.False(t, postAbort(), "nothing is running, so nothing was cancelled")

	ok, ttlCtx := status.SetRunning()
	require.True(t, ok)

	require.True(t, postAbort(), "the abort cancels the running deletion")
	require.ErrorIs(t, ttlCtx.Err(), context.Canceled)
	require.True(t, status.IsRunning(), "the slot stays reserved while that deletion drains")

	status.Finish()
	require.False(t, postAbort(), "and is free once the deletion returned")
}
