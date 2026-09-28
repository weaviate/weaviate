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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOverrideHandleShutdown(t *testing.T) {
	const before = "func (s *Server) handleShutdown() {\n"
	const after = "}\n"

	tests := []struct {
		name    string
		src     string
		want    string
		wantErr bool
	}{
		{
			name: "generated gate is replaced, the rest of the file kept",
			src:  before + generatedShutdownGate + after,
			want: before + shutdownAfterDrain + after,
		},
		{name: "gate missing, as after a template change", src: before + after, wantErr: true},
		{name: "already patched", src: before + shutdownAfterDrain + after, wantErr: true},
		{name: "gate twice", src: generatedShutdownGate + generatedShutdownGate, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "server.go")
			require.NoError(t, os.WriteFile(name, []byte(tt.src), 0o600))

			err := overrideHandleShutdown(name)

			got, readErr := os.ReadFile(name)
			require.NoError(t, readErr)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.src, string(got))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}

// server.go is committed after generation, so it must hold the patched
// handleShutdown, or the next regeneration changes it.
func TestServerGoCarriesHandleShutdownPatch(t *testing.T) {
	src, err := os.ReadFile("../../adapters/handlers/rest/server.go")
	require.NoError(t, err)
	require.Contains(t, string(src), shutdownAfterDrain)
	require.NotContains(t, string(src), generatedShutdownGate)
}
