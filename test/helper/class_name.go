package helper

import (
	"strings"
	"testing"
)

func ClassNameFromTest(t *testing.T) string {
	return strings.ReplaceAll(t.Name(), "/", "")
}
