package build

import "testing"

func TestVersionOrDev(t *testing.T) {
	previousVersion := Version
	t.Cleanup(func() {
		Version = previousVersion
	})

	Version = "1.2.3"
	if got := VersionOrDev(); got != "1.2.3" {
		t.Fatalf("expected build version, got %q", got)
	}

	Version = ""
	if got := VersionOrDev(); got != "dev" {
		t.Fatalf("expected dev fallback, got %q", got)
	}
}
