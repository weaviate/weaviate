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

//go:build linux || darwin

package file

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNameMax(t *testing.T) {
	tmp := t.TempDir()
	tests := []struct {
		name    string
		dir     string
		wantErr bool
	}{
		{name: "temp dir", dir: tmp},
		{name: "missing path", dir: filepath.Join(tmp, "missing"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NameMax(tt.dir)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Positive(t, got)
			// The limit varies by filesystem, so check that a name of exactly
			// the reported length can be created.
			require.NoError(t, os.WriteFile(filepath.Join(tt.dir, strings.Repeat("a", got)), nil, 0o600))
		})
	}
}
