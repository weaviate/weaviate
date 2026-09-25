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
