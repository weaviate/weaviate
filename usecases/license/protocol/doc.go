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

// Package protocol implements the client side of the Weaviate license
// protocol: the customer key format (wv8.<license_id>.<seed>), canonical JSON
// signing of verify requests, server-signed verify responses, an HTTP client,
// and the Checker runtime (periodic checks, signed on-disk cache, backoff,
// grace period, degraded state).
//
// This is a copy of pkg/license from github.com/weaviate/wcs-license-server
// (commit bf7ddb8), kept identical apart from the package name so it can be
// replaced by an import of the published license-go module later. Keep
// changes there first. Standard library only.
package protocol
