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

package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// AsyncCheckpointStatusEntry mirrors the cluster API's wire shape; kept
// wire-coupled (not imported from prod code) so tests pin the protocol.
type AsyncCheckpointStatusEntry struct {
	Root        []byte `json:"root"`
	CutoffMs    int64  `json:"cutoff_ms"`
	CreatedAtMs int64  `json:"created_at_ms"`
}

// CreateAsyncCheckpoint takes caller-supplied createdAtMs so the same value
// can be pinned across nodes (replicas reject one another via the
// strict-greater-than guard otherwise).
func CreateAsyncCheckpoint(t *testing.T, clusterURI, className string, shards []string, cutoffMs, createdAtMs int64) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"shards":        shards,
		"cutoff_ms":     cutoffMs,
		"created_at_ms": createdAtMs,
	})
	require.NoError(t, err)
	resp, err := http.Post(asyncCheckpointURL(clusterURI, className), "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("create checkpoint returned %d: %s", resp.StatusCode, respBody)
	}
}

func DeleteAsyncCheckpoint(t *testing.T, clusterURI, className string, shards []string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"shards": shards})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodDelete, asyncCheckpointURL(clusterURI, className), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("delete checkpoint returned %d: %s", resp.StatusCode, respBody)
	}
}

// AsyncCheckpointStatus returns an empty map when the node hosts none of the requested shards.
func AsyncCheckpointStatus(t *testing.T, clusterURI, className string, shards []string) map[string]AsyncCheckpointStatusEntry {
	t.Helper()
	u, err := url.Parse(asyncCheckpointURL(clusterURI, className))
	require.NoError(t, err)
	if len(shards) > 0 {
		q := u.Query()
		for _, s := range shards {
			q.Add("shards", s)
		}
		u.RawQuery = q.Encode()
	}
	resp, err := http.Get(u.String())
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status returned %d: %s", resp.StatusCode, body)
	}
	var out map[string]AsyncCheckpointStatusEntry
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func asyncCheckpointURL(clusterURI, className string) string {
	// docker's clusterURI is host:port (no scheme); the cluster API speaks plain HTTP.
	uri := clusterURI
	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		uri = "http://" + uri
	}
	return fmt.Sprintf("%s/replicas/indices/%s/async-checkpoint", uri, className)
}

// DiscoverShards uses the public REST API: the cluster API has no shard-enumeration endpoint.
func DiscoverShards(t *testing.T, restURI, className string) []string {
	t.Helper()
	uri := restURI
	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		uri = "http://" + uri
	}
	resp, err := http.Get(fmt.Sprintf("%s/v1/schema/%s/shards", uri, className))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var shards []struct {
		Name string `json:"name"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&shards))
	out := make([]string, len(shards))
	for i, s := range shards {
		out[i] = s.Name
	}
	return out
}
