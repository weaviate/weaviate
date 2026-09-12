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

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/cluster"
	ucfg "github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestDebugDumpConfig_RuntimeDynamicValues(t *testing.T) {
	// Test that runtime DynamicValue updates are reflected in the marshaled output
	cfg := ucfg.Config{
		// Set default value for a DynamicValue field
		QuerySlowLogEnabled: configRuntime.NewDynamicValue(false), // default: false
		Persistence:         ucfg.Persistence{DataPath: "/test/data"},
	}

	jsonBytesBefore, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)
	jsonStrBefore := string(jsonBytesBefore)

	assert.Contains(t, jsonStrBefore, `"query_slow_log_enabled": false`, "should show default value false before runtime update")

	cfg.QuerySlowLogEnabled.SetValue(true) // runtime: true

	// Second call: Marshal after runtime update - should show updated runtime value
	jsonBytesAfter, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)
	jsonStrAfter := string(jsonBytesAfter)

	assert.Contains(t, jsonStrAfter, `"query_slow_log_enabled": true`, "should reflect runtime value true after update")
	assert.NotContains(t, jsonStrAfter, `"query_slow_log_enabled": false`, "should not contain default value false after update")
}

func TestRedactDebugConfigSecrets(t *testing.T) {
	t.Run("redacts api keys, cluster password, and sentry dsn", func(t *testing.T) {
		m := map[string]any{
			"authentication": map[string]any{
				"APIKey": map[string]any{
					"enabled":      true,
					"users":        []any{"root-user", "regular-user"},
					"allowed_keys": []any{"root-secret", "user-secret"},
				},
			},
			"cluster": map[string]any{
				"auth": map[string]any{
					"basic": map[string]any{
						"username": "cluster-user",
						"password": "cluster-secret",
					},
				},
			},
			"sentry": map[string]any{
				"enabled":       true,
				"dsn":           "https://abc123@sentry.io/12345",
				"cluster_id":    "node-0",
				"cluster_owner": "team-storage",
			},
		}

		redactDebugConfigSecrets(m)

		// API keys redacted (each entry replaced; count preserved)
		apiKey := m["authentication"].(map[string]any)["APIKey"].(map[string]any)
		assert.Equal(t, []any{"<redacted>", "<redacted>"}, apiKey["allowed_keys"])
		// Sibling fields untouched
		assert.Equal(t, true, apiKey["enabled"])
		assert.Equal(t, []any{"root-user", "regular-user"}, apiKey["users"])

		// Cluster password redacted, username untouched
		basic := m["cluster"].(map[string]any)["auth"].(map[string]any)["basic"].(map[string]any)
		assert.Equal(t, "<redacted>", basic["password"])
		assert.Equal(t, "cluster-user", basic["username"])

		// Sentry DSN redacted, sibling identifiers untouched
		sentry := m["sentry"].(map[string]any)
		assert.Equal(t, "<redacted>", sentry["dsn"])
		assert.Equal(t, "node-0", sentry["cluster_id"])
		assert.Equal(t, "team-storage", sentry["cluster_owner"])
	})

	t.Run("no-op when sensitive fields are absent", func(t *testing.T) {
		m := map[string]any{
			"authentication": map[string]any{
				"APIKey": map[string]any{"enabled": false},
			},
		}
		redactDebugConfigSecrets(m)
		// Did not introduce a key that wasn't there
		_, ok := m["authentication"].(map[string]any)["APIKey"].(map[string]any)["allowed_keys"]
		assert.False(t, ok)
	})
}

// Runs a populated config.Config through the exact /debug/config pipeline. The
// hand-built-map subtests above can't catch a json tag drifting away from a
// redactPath path (e.g. APIKey gaining a json tag) — this fails when that
// happens, instead of silently re-exposing the secret in production.
func TestRedactDebugConfigSecrets_RealConfigPipeline(t *testing.T) {
	const (
		apiKey1     = "secret-api-key-1"
		apiKey2     = "secret-api-key-2"
		clusterPass = "secret-cluster-password"
		sentryDSN   = "https://publickey@o0.ingest.sentry.io/12345"
	)

	cfg := ucfg.Config{
		Authentication: ucfg.Authentication{
			APIKey: ucfg.StaticAPIKey{
				Enabled:     true,
				Users:       []string{"root-user"},
				AllowedKeys: []string{apiKey1, apiKey2},
			},
		},
		Cluster: cluster.Config{
			AuthConfig: cluster.AuthConfig{
				BasicAuth: cluster.BasicAuth{
					Username: "cluster-user",
					Password: clusterPass,
				},
			},
		},
		Sentry: &entsentry.ConfigOpts{Enabled: true, DSN: sentryDSN},
	}

	// Mirror the /debug/config handler exactly.
	jsonBytes, err := json.Marshal(cfg)
	require.NoError(t, err)
	var configMap map[string]any
	require.NoError(t, json.Unmarshal(jsonBytes, &configMap))
	cleaned := cleanEmptyValues(configMap)
	redactDebugConfigSecrets(cleaned)

	// No secret value may survive to the wire. (MarshalIndent HTML-escapes, but
	// none of these secrets contain <, > or &, so a leak would appear verbatim.)
	out, err := json.MarshalIndent(cleaned, "", "  ")
	require.NoError(t, err)
	outStr := string(out)
	for _, secret := range []string{apiKey1, apiKey2, clusterPass, sentryDSN} {
		assert.NotContainsf(t, outStr, secret,
			"secret leaked into /debug/config — a json-tag drift likely broke a redactPath path")
	}

	// Each secret is gone because it was redacted, not because the section was
	// dropped: assert redaction fired AND the non-secret siblings still survive.
	authn, _ := cleaned["authentication"].(map[string]any)
	apiKey, _ := authn["APIKey"].(map[string]any)
	assert.Equal(t, []any{"<redacted>", "<redacted>"}, apiKey["allowed_keys"])
	assert.Equal(t, []any{"root-user"}, apiKey["users"])

	clusterM, _ := cleaned["cluster"].(map[string]any)
	authM, _ := clusterM["auth"].(map[string]any)
	basicM, _ := authM["basic"].(map[string]any)
	assert.Equal(t, "<redacted>", basicM["password"])
	assert.Equal(t, "cluster-user", basicM["username"])

	sentryM, _ := cleaned["sentry"].(map[string]any)
	assert.Equal(t, "<redacted>", sentryM["dsn"])
}

type debugReassignTestIndex struct {
	names       []string
	shards      map[string]*debugReassignTestShard
	lookupError string
	walkDone    chan struct{}
}

func (i *debugReassignTestIndex) GetShard(_ context.Context, name string) (db.ShardLike, func(), error) {
	if name == i.lookupError {
		return nil, func() {}, errors.New("shard unavailable")
	}
	shard := i.shards[name]
	if shard == nil {
		return nil, func() {}, nil
	}
	shard.released.Store(false)
	return shard, func() { shard.released.Store(true) }, nil
}

func (i *debugReassignTestIndex) ForEachShard(fn func(string, db.ShardLike) error) error {
	defer close(i.walkDone)
	for _, name := range i.names {
		if err := fn(name, i.shards[name]); err != nil {
			return err
		}
	}
	return nil
}

type debugReassignTestShard struct {
	db.ShardLike
	index    db.VectorIndex
	target   string
	released atomic.Bool
}

func (s *debugReassignTestShard) GetVectorIndex(target string) (db.VectorIndex, bool) {
	s.target = target
	return s.index, s.index != nil
}

type debugReassignTestVector struct {
	db.VectorIndex
	run func() (hfresh.ReassignAllStats, error)
}

func (v *debugReassignTestVector) EnqueueReassignAll(context.Context) (hfresh.ReassignAllStats, error) {
	return v.run()
}

func TestHFreshReassignHandler(t *testing.T) {
	for _, tc := range []struct {
		name, method, query string
		missingCollection   bool
		status              int
		want                []string
		all                 bool
	}{
		{name: "single shard", method: http.MethodPost, query: "collection=Movies&shard=a&vector=description", status: 202, want: []string{"a"}},
		{name: "all shards", method: http.MethodPost, query: "collection=Movies&vector=description", status: 202, want: []string{"a", "b"}, all: true},
		{name: "explicit empty shard", method: http.MethodPost, query: "collection=Movies&shard=&vector=description", status: 202, want: []string{"a", "b"}, all: true},
		{name: "missing collection argument", method: http.MethodPost, query: "shard=a", status: 400},
		{name: "unknown collection", method: http.MethodPost, query: "collection=Missing", missingCollection: true, status: 404},
		{name: "shard lookup failure", method: http.MethodPost, query: "collection=Movies&shard=unavailable", status: 404},
		{name: "unknown shard", method: http.MethodPost, query: "collection=Movies&shard=missing", status: 404},
		{name: "missing vector", method: http.MethodPost, query: "collection=Movies&shard=no-vector", status: 404},
		{name: "non hfresh", method: http.MethodPost, query: "collection=Movies&shard=non-hfresh", status: 400},
		{name: "get cannot enqueue", method: http.MethodGet, query: "collection=Movies", status: 405},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			calls := make(chan string, 4)
			idx := &debugReassignTestIndex{names: []string{"a", "missing", "unavailable", "no-vector", "non-hfresh", "b"}, lookupError: "unavailable", shards: map[string]*debugReassignTestShard{}, walkDone: make(chan struct{})}
			for _, name := range []string{"a", "b"} {
				shard := &debugReassignTestShard{}
				shard.index = &debugReassignTestVector{run: func() (hfresh.ReassignAllStats, error) {
					assert.True(t, shard.released.Load(), "release shard reference before the scan")
					assert.Equal(t, "description", shard.target)
					calls <- name
					if name == "a" {
						return hfresh.ReassignAllStats{}, errors.New("scan failed")
					}
					return hfresh.ReassignAllStats{}, nil
				}}
				idx.shards[name] = shard
			}
			idx.shards["no-vector"] = &debugReassignTestShard{}
			idx.shards["non-hfresh"] = &debugReassignTestShard{index: &dbVectorIndexWithoutReassign{}}
			handler := newHFreshReassignHandler(logger, func(name schema.ClassName) hfreshReassignIndex {
				if tc.missingCollection {
					return nil
				}
				assert.Equal(t, schema.ClassName("Movies"), name)
				return idx
			})
			rec := httptest.NewRecorder()
			handler(rec, httptest.NewRequest(tc.method, "/debug/index/reassign/vector?"+tc.query, nil))
			require.Equal(t, tc.status, rec.Code)
			var got []string
			for range tc.want {
				select {
				case name := <-calls:
					got = append(got, name)
				case <-time.After(5 * time.Second):
					t.Fatal("reassignment did not start")
				}
			}
			if tc.all {
				select {
				case <-idx.walkDone:
				case <-time.After(5 * time.Second):
					t.Fatal("shard walk did not finish")
				}
			}
			require.ElementsMatch(t, tc.want, got)
			require.Empty(t, calls)
		})
	}
}

type dbVectorIndexWithoutReassign struct{ db.VectorIndex }

func TestHFreshReassignAllShardsRunsInBackground(t *testing.T) {
	logger, _ := test.NewNullLogger()
	unblock := make(chan struct{})
	defer close(unblock)
	started := make(chan string, 2)
	idx := &debugReassignTestIndex{names: []string{"a", "b"}, shards: map[string]*debugReassignTestShard{}, walkDone: make(chan struct{})}
	for _, name := range idx.names {
		idx.shards[name] = &debugReassignTestShard{index: &debugReassignTestVector{run: func() (hfresh.ReassignAllStats, error) {
			started <- name
			if name == "a" {
				<-unblock
			}
			return hfresh.ReassignAllStats{}, nil
		}}}
	}
	handler := newHFreshReassignHandler(logger, func(schema.ClassName) hfreshReassignIndex { return idx })
	rec := httptest.NewRecorder()
	returned := make(chan struct{})
	enterrors.GoWrapper(func() {
		handler(rec, httptest.NewRequest(http.MethodPost, "/debug/index/reassign/vector?collection=Movies", nil))
		close(returned)
	}, logger)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP response waited for shard scan to complete")
	}
	require.Equal(t, http.StatusAccepted, rec.Code)
	select {
	case name := <-started:
		require.Equal(t, "a", name)
	case <-time.After(5 * time.Second):
		t.Fatal("first shard did not start")
	}
	require.Empty(t, started, "second shard must wait until the first scan finishes")
	// Unblock the first scan, then wait for the worker to exit before the test ends.
	unblock <- struct{}{}
	select {
	case <-idx.walkDone:
	case <-time.After(5 * time.Second):
		t.Fatal("all-shard scan did not finish")
	}
	require.Equal(t, "b", <-started)
}
