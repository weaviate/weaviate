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

package validation

import (
	"testing"
)

func TestParseDate_StrictRFC3339(t *testing.T) {
	// The Bug: +14:60 should be rejected by our dateVal function
	badDate := "2025-01-01T00:00:00+14:60"

	// Call the wrapper function we just patched
	_, err := dateVal(badDate)

	t.Logf("Parsing '%s' returned error: %v", badDate, err)

	if err == nil {
		t.Error("FAIL: Invalid date was accepted! The fix is not working.")
	} else {
		t.Log("SUCCESS: Invalid date was correctly rejected.")
	}
}
