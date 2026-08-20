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

package hnsw

import (
	"os"
	"testing"
)

// TestMain clears the scan-prefill switches this package reads from the environment.
// Both are process-global and the worker budget is resolved once, so an operator
// running the suite with HNSW_PREFILL_SCAN_WORKERS=0 — the documented revert path —
// would otherwise route every scan test to the serial prefiller and fail them for the
// setting rather than for the code. Tests that care about a value set it themselves,
// through withPrefillWorkers or t.Setenv.
func TestMain(m *testing.M) {
	os.Unsetenv(prefillScanWorkersEnv)
	os.Unsetenv(prefillTargetedReadsEnv)
	os.Exit(m.Run())
}
