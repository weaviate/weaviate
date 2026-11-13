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

package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTreeFileSourceRefusesPathOutsideRoot(t *testing.T) {
	src := NewTreeFileSource(t.TempDir())
	for _, relPath := range []string{"../escape.db", "/etc/passwd"} {
		t.Run(relPath, func(t *testing.T) {
			_, err := src.Stat(relPath)
			require.ErrorContains(t, err, "sanitize file path")

			_, err = src.Open(relPath)
			require.ErrorContains(t, err, "sanitize file path")
		})
	}
}
