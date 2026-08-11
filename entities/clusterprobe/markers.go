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

// Package clusterprobe holds the wire markers of the read-only
// cluster-internal probes. It is a leaf so that the handler serving a probe
// and the client reading it can agree on one value without importing each
// other.
package clusterprobe

// Each route puts its marker in the "probe" field; its client refuses a 200
// carrying anything else, so a proxy or misrouted ingress can't be misread
// as a node's permissive "nothing running here".
const (
	BackupNodeActivityMarker = "weaviate/backup-node-activity"
	ReindexCleanupMarker     = "weaviate/reindex-cleanup-activity"
)

// ProbeNotWiredMarker is the whole body of the 503 a node sends when it serves
// a probe route but the subsystem behind it doesn't exist on this build,
// telling the client to stop retrying. Only routes where that's safe to treat
// as terminal send it; others stay a plain, retryable 503.
const ProbeNotWiredMarker = "weaviate/probe-not-wired"

// The route each probe is served on, defined once for both the mux and the
// client. A path mismatch would 404, which clients read as "older build" and
// let through — a typo would silently disable the gate rather than break it.
const (
	BackupNodeActivityPath     = "/backups/node-activity"
	ReindexCleanupActivityPath = "/reindex/cleanup-activity"
)
