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
	"syscall"
	"testing"

	pkgerrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/storagestate"
)

func TestDocsLinkFields(t *testing.T) {
	// Spelled out rather than composed from the constants: the id and the host
	// are the contract the docs repo redirects from.
	mappingsFields := logrus.Fields{"docs_url": "https://docs.weaviate.io/e/core-mem001"}

	tests := []struct {
		name string
		err  error
		want logrus.Fields
	}{
		{
			name: "nil error",
			err:  nil,
			want: nil,
		},
		{
			name: "not enough mappings",
			err:  ErrNotEnoughMappings,
			want: mappingsFields,
		},
		{
			name: "not enough mappings wrapped as the shard init path wraps it",
			err:  pkgerrors.Wrap(ErrNotEnoughMappings, "memory pressure: cannot init shard"),
			want: mappingsFields,
		},
		{
			name: "not enough mappings wrapped via fmt.Errorf %w",
			err:  fmt.Errorf("add missing tenant shard t1 during update index: %w", ErrNotEnoughMappings),
			want: mappingsFields,
		},
		{
			name: "not enough memory is a different condition",
			err:  NewNotEnoughMemory("oom"),
			want: nil,
		},
		{
			name: "read-only shard",
			err:  storagestate.ErrStatusReadOnly,
			want: nil,
		},
		{
			name: "disk full",
			err:  syscall.ENOSPC,
			want: nil,
		},
		{
			name: "unrelated error",
			err:  fmt.Errorf("some other error"),
			want: nil,
		},
		{
			name: "message text alone does not qualify",
			err:  fmt.Errorf("not enough memory mappings"),
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, DocsLinkFields(tt.err))
		})
	}
}
