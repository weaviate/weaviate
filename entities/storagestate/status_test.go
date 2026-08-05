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

package storagestate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestErrStatusReadOnlyWithReason pins the sentinel contract: the with-reason
// constructor must %w-wrap ErrStatusReadOnly — errors.Is classification (the
// transient-error classifier, the raft apply path's park decision) depends on
// it — while keeping the exact message text emitted today, because operators
// and log-based tooling match on it.
func TestErrStatusReadOnlyWithReason(t *testing.T) {
	err := ErrStatusReadOnlyWithReason("resource pressure")
	require.Equal(t, "store is read-only due to: resource pressure", err.Error(),
		"message text is part of the operator-facing contract and must not change")
	require.ErrorIs(t, err, ErrStatusReadOnly,
		"ErrStatusReadOnlyWithReason must wrap ErrStatusReadOnly so errors.Is classification works")
}

func TestStatusValidation(t *testing.T) {
	t.Run("with invalid status", func(t *testing.T) {
		tests := []string{
			"READ_ONLY",
			"read only",
			"ok",
			"WRITEONLY",
			"INDESKING",
			"",
		}

		for _, test := range tests {
			_, err := ValidateStatus(test)
			require.EqualError(t, ErrInvalidStatus, err.Error())
		}
	})

	t.Run("with valid status", func(t *testing.T) {
		tests := []struct {
			in       string
			expected Status
		}{
			{"READONLY", StatusReadOnly},
			{"READY", StatusReady},
			{"INDEXING", StatusIndexing},
		}

		for _, test := range tests {
			status, err := ValidateStatus(test.in)
			require.Nil(t, err)
			require.Equal(t, test.expected, status)
		}
	})
}
