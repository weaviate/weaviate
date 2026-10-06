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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A cause is matched with errors.Is from both ends, so both arms of the unwrap
// have to be reachable.
func TestNewCanceledCause(t *testing.T) {
	reason := errors.New("aborted")

	withReason := NewCanceledCause(reason)
	require.ErrorIs(t, withReason, context.Canceled)
	require.ErrorIs(t, withReason, reason)
	require.Equal(t, "context canceled: aborted", withReason.Error())

	require.Equal(t, context.Canceled, NewCanceledCause(nil),
		"no reason is plain context.Canceled, not a cause that prints as one")
}
