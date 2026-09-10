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

package errors

import (
	"net/url"
	"sync/atomic"
)

const (
	// DocsLinkField holds a docs link in a log entry's structured data instead
	// of its message: operators fingerprint and alert on message text, so a URL
	// appended there breaks their rules.
	DocsLinkField = "docs_url"

	// DocsBaseURL is where every docs link points. The docs site keeps its
	// paths stable, so the host never needs to change.
	DocsBaseURL = "https://docs.weaviate.io"
)

// clusterIDSource reports the cluster's id, or "" before raft has committed
// one. It is only installed when telemetry is enabled, so links from a node
// with telemetry disabled never carry an id.
var clusterIDSource atomic.Pointer[func() string]

// SetClusterIDSource makes every docs link carry the cluster id as
// ?clusterid=<id>, so the docs page can tell which cluster the reader came
// from. nil removes it.
func SetClusterIDSource(fn func() string) {
	if fn == nil {
		clusterIDSource.Store(nil)
		return
	}
	clusterIDSource.Store(&fn)
}

// WithClusterID appends ?clusterid=<id> to u when the cluster has an id.
func WithClusterID(u string) string {
	p := clusterIDSource.Load()
	if p == nil {
		return u
	}
	id := (*p)()
	if id == "" {
		return u
	}
	return u + "?clusterid=" + url.QueryEscape(id)
}
