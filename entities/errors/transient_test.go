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

package errors

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "not enough memory",
			err:  NewNotEnoughMemory("oom"),
			want: true,
		},
		{
			name: "not enough mappings",
			err:  ErrNotEnoughMappings,
			want: true,
		},
		{
			name: "raw ENOSPC",
			err:  syscall.ENOSPC,
			want: true,
		},
		{
			name: "ENOSPC wrapped in os.PathError (as returned by os.Write)",
			err:  &os.PathError{Op: "write", Path: "/var/lib/weaviate/foo", Err: syscall.ENOSPC},
			want: true,
		},
		{
			name: "ENOSPC wrapped via fmt.Errorf %w",
			err:  fmt.Errorf("add to hnsw: %w", &os.PathError{Op: "write", Path: "/var/lib/weaviate/foo", Err: syscall.ENOSPC}),
			want: true,
		},
		{
			name: "ENOSPC wrapped via pkg/errors.Wrapf (as used in hfresh)",
			err:  pkgerrors.Wrapf(&os.PathError{Op: "write", Path: "/var/lib/weaviate/foo", Err: syscall.ENOSPC}, "failed to upsert new centroid %d after split operation", 8178),
			want: true,
		},
		{
			name: "unrelated permanent error",
			err:  fmt.Errorf("some permanent error"),
			want: false,
		},
		{
			name: "other syscall errno (EACCES) is not transient",
			err:  &os.PathError{Op: "write", Path: "/var/lib/weaviate/foo", Err: syscall.EACCES},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsTransient(tt.err))
		})
	}
}
