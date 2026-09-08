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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

// panickingTTLIndex embeds the interface so only the one method under test is
// implemented.
type panickingTTLIndex struct {
	sharding.RemoteIndexIncomingRepo
	shard string
}

func (p *panickingTTLIndex) IncomingDeleteObjectsExpired(_ context.Context,
	eg *enterrors.ErrorGroupWrapper, _ errorcompounder.ErrorCompounder,
	_ string, _, _ time.Time, _ func(int32), _ uint64,
) {
	// a shard delete that panics must be reported as a failed sweep
	eg.Go(func() error { panic("delete panicked") }, ttlTestClass, p.shard)
}

type fakeIncomingRepo struct {
	index sharding.RemoteIndexIncomingRepo
}

func (f *fakeIncomingRepo) GetIndexForIncomingSharding(schema.ClassName) sharding.RemoteIndexIncomingRepo {
	return f.index
}

type fakeIncomingSchema struct{}

func (fakeIncomingSchema) ReadOnlyClassWithVersion(_ context.Context, class string, _ uint64) (*models.Class, error) {
	return &models.Class{Class: class}, nil
}

const ttlTestClass = "TTLPanicClass"

// countingTTLIndex calls the countDeleted it is handed from inside the group,
// so a test can see what a collection's deletions add up to.
type countingTTLIndex struct {
	sharding.RemoteIndexIncomingRepo
}

func (countingTTLIndex) IncomingDeleteObjectsExpired(_ context.Context,
	eg *enterrors.ErrorGroupWrapper, _ errorcompounder.ErrorCompounder,
	_ string, _, _ time.Time, countDeleted func(int32), _ uint64,
) {
	eg.Go(func() error {
		for i := 0; i < 500; i++ {
			countDeleted(1)
		}
		return nil
	})
}

// TestIncomingDeleteReportsAPanickingShard pins that a panicking shard's
// failure is reported. The sweep runs detached from the request, so it is
// observable only in the log.
func TestIncomingDeleteReportsAPanickingShard(t *testing.T) {
	// the sweep reports the panic only where the group recovers it
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	remoteIndex := sharding.NewRemoteIndexIncoming(
		&fakeIncomingRepo{index: &panickingTTLIndex{shard: "shard-1"}},
		fakeIncomingSchema{}, nil)

	cfg := config.Config{
		ObjectsTTLConcurrencyFactor: configRuntime.NewDynamicValue[float64](1),
	}
	d := clusterapi.NewObjectTTL(remoteIndex, clusterapi.NewNoopAuthHandler(), logger,
		cfg, objectttl.NewLocalStatus())

	mux := http.NewServeMux()
	mux.Handle("/cluster/object_ttl/", d.Expired())
	server := httptest.NewServer(mux)
	defer server.Close()

	body, err := json.Marshal([]objectttl.ObjectsExpiredPayload{{
		Class:    ttlTestClass,
		Prop:     "expiresAt",
		TtlMilli: time.Now().UnixMilli(),
		DelMilli: time.Now().UnixMilli(),
	}})
	require.NoError(t, err)

	resp, err := http.Post(server.URL+"/cluster/object_ttl/delete_expired",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		var failure string
		for _, e := range hook.AllEntries() {
			if strings.HasPrefix(e.Message, "incoming ttl deletion on remote node failed") {
				failure = e.Message
			}
		}
		require.NotEmpty(c, failure, "a panicking shard delete must be reported as a failed sweep")
		require.Contains(c, failure, ttlTestClass,
			"the reported failure names the collection the panic came from")
		require.Contains(c, failure, "shard-1",
			"and the shard, which only the goroutine's localVars carry")
	}, 5*time.Second, 10*time.Millisecond)
}

// TestIncomingDeleteCountsWithoutRacingTheCounterMap guards against a delete
// goroutine reading the counter map while the dispatch loop still writes to it:
// a concurrent map access is fatal, so the group's recover can't catch it.
func TestIncomingDeleteCountsWithoutRacingTheCounterMap(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	remoteIndex := sharding.NewRemoteIndexIncoming(
		&fakeIncomingRepo{index: countingTTLIndex{}}, fakeIncomingSchema{}, nil)

	cfg := config.Config{
		ObjectsTTLConcurrencyFactor: configRuntime.NewDynamicValue[float64](1),
	}
	d := clusterapi.NewObjectTTL(remoteIndex, clusterapi.NewNoopAuthHandler(), logger,
		cfg, objectttl.NewLocalStatus())

	mux := http.NewServeMux()
	mux.Handle("/cluster/object_ttl/", d.Expired())
	server := httptest.NewServer(mux)
	defer server.Close()

	// one map write per collection, so an earlier collection's deletes are
	// running while the loop adds the next
	payload := make([]objectttl.ObjectsExpiredPayload, 0, 8)
	for i := 0; i < 8; i++ {
		payload = append(payload, objectttl.ObjectsExpiredPayload{
			Class:    fmt.Sprintf("Collection%d", i),
			Prop:     "expiresAt",
			TtlMilli: time.Now().UnixMilli(),
			DelMilli: time.Now().UnixMilli(),
		})
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	resp, err := http.Post(server.URL+"/cluster/object_ttl/delete_expired",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	// the sweep is detached from the request, so wait for it to finish before
	// the test returns, or it races nothing at all
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		var finished bool
		for _, e := range hook.AllEntries() {
			if e.Message == "incoming ttl deletion on remote node finished" {
				finished = true
			}
		}
		require.True(c, finished, "the sweep must run to completion")
	}, 10*time.Second, 10*time.Millisecond)
}

// ttlTestServer starts the delete_expired handler over an index whose shard
// delete counts 500 deletions and returns, and hands back the status it guards
// so a test can drive the slot from both ends.
func ttlTestServer(t *testing.T) (*httptest.Server, *objectttl.LocalStatus) {
	t.Helper()

	logger, _ := logrustest.NewNullLogger()
	remoteIndex := sharding.NewRemoteIndexIncoming(
		&fakeIncomingRepo{index: countingTTLIndex{}}, fakeIncomingSchema{}, nil)
	cfg := config.Config{
		ObjectsTTLConcurrencyFactor: configRuntime.NewDynamicValue[float64](1),
	}
	status := objectttl.NewLocalStatus()
	d := clusterapi.NewObjectTTL(remoteIndex, clusterapi.NewNoopAuthHandler(), logger,
		cfg, status)

	mux := http.NewServeMux()
	mux.Handle("/cluster/object_ttl/", d.Expired())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, status
}

// TestIncomingDeleteRefusesAMalformedBody guards each validation check
// individually — the message is asserted so one check cannot pass for another.
func TestIncomingDeleteRefusesAMalformedBody(t *testing.T) {
	complete := func(class string) objectttl.ObjectsExpiredPayload {
		return objectttl.ObjectsExpiredPayload{
			Class:    class,
			Prop:     "expiresAt",
			TtlMilli: time.Now().UnixMilli(),
			DelMilli: time.Now().UnixMilli(),
		}
	}
	without := func(class string, f func(*objectttl.ObjectsExpiredPayload)) objectttl.ObjectsExpiredPayload {
		p := complete(class)
		f(&p)
		return p
	}

	cases := []struct {
		name    string
		payload []objectttl.ObjectsExpiredPayload
		wantMsg string
	}{
		{
			name:    "one collection named twice",
			payload: []objectttl.ObjectsExpiredPayload{complete("Books"), complete("Books")},
			wantMsg: "named more than once",
		},
		{
			name:    "two spellings of one collection",
			payload: []objectttl.ObjectsExpiredPayload{complete("Books"), complete("BOOKS")},
			wantMsg: "Books and BOOKS",
		},
		{
			name:    "no property to expire on",
			payload: []objectttl.ObjectsExpiredPayload{without("Books", func(p *objectttl.ObjectsExpiredPayload) { p.Prop = "" })},
			wantMsg: "required",
		},
		{
			name:    "no ttl threshold",
			payload: []objectttl.ObjectsExpiredPayload{without("Books", func(p *objectttl.ObjectsExpiredPayload) { p.TtlMilli = 0 })},
			wantMsg: "ttlMilli are required",
		},
		{
			// a zero reaches the shard as 1970, which tombstones older than the object
			name:    "no deletion time",
			payload: []objectttl.ObjectsExpiredPayload{without("Books", func(p *objectttl.ObjectsExpiredPayload) { p.DelMilli = 0 })},
			wantMsg: "delMilli must be a deletion time",
		},
		{
			name:    "a deletion time before the epoch",
			payload: []objectttl.ObjectsExpiredPayload{without("Books", func(p *objectttl.ObjectsExpiredPayload) { p.DelMilli = -1 })},
			wantMsg: "delMilli must be a deletion time",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := ttlTestServer(t)

			body, err := json.Marshal(tt.payload)
			require.NoError(t, err)

			resp, err := http.Post(server.URL+"/cluster/object_ttl/delete_expired",
				"application/json", bytes.NewReader(body))
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			msg, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(msg), tt.wantMsg)
		})
	}
}

// TestIncomingDeleteAcceptsAnOldThreshold guards that a very old threshold is
// still dispatched: ttlMilli is an absolute instant, so a long ttl computed from
// now can be very negative, and refusing on that would refuse a valid sweep.
func TestIncomingDeleteAcceptsAnOldThreshold(t *testing.T) {
	server, _ := ttlTestServer(t)

	body, err := json.Marshal([]objectttl.ObjectsExpiredPayload{{
		Class:    ttlTestClass,
		Prop:     "expiresAt",
		TtlMilli: -2_000_000_000_000,
		DelMilli: time.Now().UnixMilli(),
	}})
	require.NoError(t, err)

	resp, err := http.Post(server.URL+"/cluster/object_ttl/delete_expired",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestIncomingAbortReportsTheRunningDeletion(t *testing.T) {
	server, status := ttlTestServer(t)

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

	ok, ctx := status.SetRunning()
	require.True(t, ok)

	require.True(t, postAbort(), "the first abort cancels the running deletion")
	require.Error(t, ctx.Err(), "the deletion's context is cancelled")

	require.True(t, postAbort(), "a repeat while it drains still reports the deletion")
	require.True(t, status.IsRunning(), "the slot stays reserved until the deletion finishes")

	status.Finished()
	require.False(t, postAbort(), "and false once the deletion returned")
}

func TestIncomingDeleteTakesTheSlotBeforeReadingTheBody(t *testing.T) {
	server, status := ttlTestServer(t)

	post := func(payload string) int {
		t.Helper()
		resp, err := http.Post(server.URL+"/cluster/object_ttl/delete_expired",
			"application/json", strings.NewReader(payload))
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp.StatusCode
	}

	valid := fmt.Sprintf(`[{"class":%q,"prop":"expiresAt","ttlMilli":1,"delMilli":2}]`, ttlTestClass)

	held, _ := status.SetRunning()
	require.True(t, held)
	require.Equal(t, http.StatusTooManyRequests, post(valid),
		"a second caller is refused while a deletion holds the slot")
	require.Equal(t, http.StatusTooManyRequests, post("not json"),
		"and is refused without the body being read, or this would be a 400")

	status.Finished()

	require.Equal(t, http.StatusBadRequest, post("not json"),
		"a malformed body is refused")
	require.False(t, status.IsRunning(),
		"and the slot it took to read that body is handed back")

	require.Equal(t, http.StatusAccepted, post(valid),
		"so the next sweep is accepted")
}
