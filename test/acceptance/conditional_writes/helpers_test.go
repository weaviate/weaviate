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

// Package conditional_writes contains acceptance tests for the conditional-write
// wire contract. This file holds shared HTTP helpers and schema utilities used
// by both the HA regression suite and the production-readiness suite.
package conditional_writes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// insertObjectHTTP posts an object to POST /v1/objects on hostURI.
//
// When conditional is true the request includes ?condition=insert_if_not_exists.
// consistencyLevel may be "" (server default) or an explicit value like "QUORUM"
// or "ALL". The function returns the HTTP status code without asserting it;
// callers decide what constitutes success or failure.
func insertObjectHTTP(
	t *testing.T,
	hostURI string,
	className string,
	id string,
	consistencyLevel string,
	conditional bool,
) int {
	t.Helper()

	type body struct {
		Class      string                 `json:"class"`
		ID         string                 `json:"id"`
		Properties map[string]interface{} `json:"properties"`
	}
	payload := body{
		Class: className,
		ID:    id,
		Properties: map[string]interface{}{
			"testfield": fmt.Sprintf("value-for-%s", id),
		},
	}
	jsonData, err := json.Marshal(payload)
	require.NoError(t, err, "marshal insert body")

	var url string
	if conditional {
		url = fmt.Sprintf("http://%s/v1/objects?condition=insert_if_not_exists", hostURI)
		if consistencyLevel != "" {
			url = fmt.Sprintf("%s&consistency_level=%s", url, consistencyLevel)
		}
	} else {
		url = fmt.Sprintf("http://%s/v1/objects", hostURI)
		if consistencyLevel != "" {
			url = fmt.Sprintf("%s?consistency_level=%s", url, consistencyLevel)
		}
	}

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, url,
		bytes.NewBuffer(jsonData),
	)
	require.NoError(t, err, "build HTTP request")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Logf("insertObjectHTTP: network error (host=%s id=%s conditional=%v): %v",
			hostURI, id, conditional, err)
		return 0
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode
}

// condInsertHTTP is a convenience wrapper around insertObjectHTTP for
// conditional (insert_if_not_exists) inserts.
func condInsertHTTP(t *testing.T, hostURI, className, id, consistencyLevel string) int {
	t.Helper()
	return insertObjectHTTP(t, hostURI, className, id, consistencyLevel, true)
}

// uncondInsertHTTP is a convenience wrapper around insertObjectHTTP for
// unconditional inserts (plain POST /v1/objects).
func uncondInsertHTTP(t *testing.T, hostURI, className, id, consistencyLevel string) int {
	t.Helper()
	return insertObjectHTTP(t, hostURI, className, id, consistencyLevel, false)
}

// setupRF3Class creates a collection with RF=3, the given number of shards,
// and no vectorizer. When numShards <= 0, the default sharding config is used.
func setupRF3Class(t *testing.T, className string, numShards int) {
	t.Helper()
	var shardingConfig interface{}
	if numShards > 0 {
		shardingConfig = map[string]interface{}{"desiredCount": numShards}
	}
	cls := &models.Class{
		Class:      className,
		Vectorizer: "none",
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 3,
		},
		ShardingConfig: shardingConfig,
		Properties: []*models.Property{
			{
				Name:     "testfield",
				DataType: []string{"text"},
			},
		},
	}
	helper.CreateClass(t, cls)
}

// waitForSchemaOnAllNodes polls GET /v1/schema/{className} on every node in
// the cluster until the class (including its testfield property) is visible on
// all of them, or until timeout elapses. This guards against schema propagation
// lag under RAFT: a write routed to a node that has not yet received the schema
// update returns 422, not 201/200.
func waitForSchemaOnAllNodes(t *testing.T, compose *docker.DockerCompose, className string, numNodes int) {
	t.Helper()
	const (
		timeout  = 60 * time.Second
		interval = 500 * time.Millisecond
	)
	deadline := time.Now().Add(timeout)
	for nodeIdx := 0; nodeIdx < numNodes; nodeIdx++ {
		nodeURI := compose.ContainerURI(nodeIdx)
		var ready bool
		for time.Now().Before(deadline) {
			helper.SetupClient(nodeURI)
			cls, err := helper.GetClassWithoutAssert(t, className, "")
			if err == nil && cls != nil {
				hasField := false
				for _, p := range cls.Properties {
					if p.Name == "testfield" {
						hasField = true
						break
					}
				}
				if hasField {
					ready = true
					break
				}
			}
			time.Sleep(interval)
		}
		require.True(t, ready,
			"schema for class %q (with testfield property) did not propagate to node %d within %s",
			className, nodeIdx, timeout)
	}
}
