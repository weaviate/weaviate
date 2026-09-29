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
	require.NoError(t, TryDeleteAsyncCheckpoint(clusterURI, className, shards))
}

// TryDeleteAsyncCheckpoint is the error-returning variant, safe inside EventuallyWithT closures.
func TryDeleteAsyncCheckpoint(clusterURI, className string, shards []string) error {
	body, err := json.Marshal(map[string]any{"shards": shards})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodDelete, asyncCheckpointURL(clusterURI, className), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete checkpoint on %s returned %d: %s", clusterURI, resp.StatusCode, respBody)
	}
	return nil
}

// AsyncCheckpointStatus returns an empty map when the node hosts none of the requested shards.
func AsyncCheckpointStatus(t *testing.T, clusterURI, className string, shards []string) map[string]AsyncCheckpointStatusEntry {
	t.Helper()
	out, err := TryAsyncCheckpointStatus(clusterURI, className, shards)
	require.NoError(t, err)
	return out
}

// TryAsyncCheckpointStatus is the error-returning variant, safe inside EventuallyWithT closures.
func TryAsyncCheckpointStatus(clusterURI, className string, shards []string) (map[string]AsyncCheckpointStatusEntry, error) {
	u, err := url.Parse(asyncCheckpointURL(clusterURI, className))
	if err != nil {
		return nil, err
	}
	if len(shards) > 0 {
		q := u.Query()
		for _, s := range shards {
			q.Add("shards", s)
		}
		u.RawQuery = q.Encode()
	}
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status on %s returned %d: %s", clusterURI, resp.StatusCode, body)
	}
	var out map[string]AsyncCheckpointStatusEntry
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
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
