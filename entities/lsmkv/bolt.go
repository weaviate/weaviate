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

package lsmkv

import "time"

// BoltFlockTimeout bounds how long a bolt open waits for the file lock. A
// leaked handle (e.g. from a shard teardown that failed mid-way) holds the
// flock; without a timeout the open retries forever and wedges the loading
// goroutine. Shared by every bolt sidecar on the shard-load path.
const BoltFlockTimeout = 5 * time.Second
