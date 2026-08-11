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

// Each probe's "nothing running here" answer is what lets the reindex gate
// admit a migration, so only an answer proven to come from a node may say
// it: without a marker, a proxy or misrouted ingress answering in a node's
// stead would decode to the permissive value and clear the whole cluster.
// Each route puts its marker in the "probe" field; its client refuses a 200
// carrying anything else.
const (
	BackupNodeActivityMarker = "weaviate/backup-node-activity"
	ReindexCleanupMarker     = "weaviate/reindex-cleanup-activity"
)

// ProbeNotWiredMarker is the whole body of the 503 a node sends when it serves
// a probe route but the subsystem behind it does not exist on this build. No
// amount of retrying turns that into an answer, so a client that sees it stops
// asking instead of spending its budget on a node that can never reply.
//
// Only a route whose "cannot answer" is safe to treat as terminal sends it. A
// route where stopping would make the caller proceed on an assumption sends a
// plain 503, which stays a retryable error.
const ProbeNotWiredMarker = "weaviate/probe-not-wired"

// The route each probe is served on, defined once for both the mux and the
// client. A path mismatch would 404, which clients read as "older build" and
// let through — a typo would silently disable the gate rather than break it.
const (
	BackupNodeActivityPath     = "/backups/node-activity"
	ReindexCleanupActivityPath = "/reindex/cleanup-activity"
)
