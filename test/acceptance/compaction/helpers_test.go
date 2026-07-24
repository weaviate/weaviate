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

package compaction_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// holdView sends POST to the debug endpoint to hold a consistent view on a bucket.
func holdView(t *testing.T, debugHost, col, shard, bucket string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/debug/consistent-view/%s/shards/%s/buckets/%s",
		debugHost, col, shard, bucket)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"holdView failed: status=%d body=%s", resp.StatusCode, body)
}
