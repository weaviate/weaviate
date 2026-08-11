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

package config

import (
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

// clearEnv empties the process environment for the duration of the test and
// puts it back afterwards.
//
// os.Clearenv on its own removes TMP/TEMP too, and os.TempDir falls back to
// C:\Windows on Windows and /tmp elsewhere -- so every later t.TempDir in the
// package either fails outright or silently writes somewhere unintended. The
// fallback being writable on Linux is why this only shows up on Windows and
// under -count>1, where a test that clears the environment runs before one
// that needs a temporary directory.
func clearEnv(t *testing.T) {
	t.Helper()

	saved := os.Environ()
	t.Cleanup(func() {
		os.Clearenv()
		for _, entry := range saved {
			if key, value, ok := strings.Cut(entry, "="); ok {
				os.Setenv(key, value)
			}
		}
	})

	os.Clearenv()
}

type fakeModuleProvider struct {
	valid []string
}

func (f *fakeModuleProvider) ValidateVectorizer(moduleName string) error {
	for _, valid := range f.valid {
		if moduleName == valid {
			return nil
		}
	}

	return errors.Errorf("invalid vectorizer %q", moduleName)
}
