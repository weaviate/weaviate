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

package replica_test

import (
	"os"
	"testing"

	"github.com/weaviate/weaviate/entities/storobj"
)

// TestMain opens the write-gate to v2 for all tests in the replica_test package.
// The existing conditional-write tests (TestConditionalWriteVersionCAS*,
// TestConditionalWriteVersionCLInvariant, etc.) all exercise version-CAS, which
// requires the gate to be open. Tests that specifically test gate-CLOSED behaviour
// (TestB1_*) save and restore the gate themselves.
func TestMain(m *testing.M) {
	storobj.SetWriteMarshallerVersion(2)
	os.Exit(m.Run())
}
