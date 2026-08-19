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

package helper

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
)

// DebugUsageReport reads a node's usage report from its debug port. host is the
// node's debug address, e.g. "localhost:6060". shardConcurrency, when given,
// caps how many shards the node reads in parallel to build the report.
func DebugUsageReport(host string, shardConcurrency ...int) (*usagetypes.Report, error) {
	url := fmt.Sprintf("http://%s/debug/usage?exactObjectCount=true", host)
	if len(shardConcurrency) > 0 {
		url += fmt.Sprintf("&shardConcurrency=%d", shardConcurrency[0])
	}
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var report usagetypes.Report
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, fmt.Errorf("parse usage report: %w", err)
	}
	return &report, nil
}

// DebugUsageForCollection returns one collection out of the node's usage report.
func DebugUsageForCollection(host, collection string) (usagetypes.CollectionUsage, error) {
	report, err := DebugUsageReport(host)
	if err != nil {
		return usagetypes.CollectionUsage{}, err
	}
	for _, col := range report.Collections {
		if col != nil && col.Name == collection {
			return *col, nil
		}
	}
	return usagetypes.CollectionUsage{}, fmt.Errorf("collection %s not found in debug usage report", collection)
}
