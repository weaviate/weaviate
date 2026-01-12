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
