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

package objects

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/usagelimits"
)

func TestUnanimousLimitExceeded(t *testing.T) {
	limit10 := &usagelimits.LimitExceededError{Limit: usagelimits.LimitObjects, Value: 10, RenderedMessage: "objects limit 10"}
	limit10Different := &usagelimits.LimitExceededError{Limit: usagelimits.LimitObjects, Value: 10, RenderedMessage: "different message"}
	limit20 := &usagelimits.LimitExceededError{Limit: usagelimits.LimitObjects, Value: 20, RenderedMessage: "objects limit 20"}
	otherErr := errors.New("not a limit error")

	tests := []struct {
		name  string
		input BatchObjects
		want  *usagelimits.LimitExceededError
	}{
		{name: "empty", input: BatchObjects{}, want: nil},
		{name: "single unanimous", input: BatchObjects{{Err: limit10}}, want: limit10},
		{name: "all same limit + value", input: BatchObjects{{Err: limit10}, {Err: limit10Different}, {Err: limit10}}, want: limit10},
		{name: "first is not limit-exceeded", input: BatchObjects{{Err: otherErr}, {Err: limit10}}, want: nil},
		{name: "one is not limit-exceeded", input: BatchObjects{{Err: limit10}, {Err: otherErr}, {Err: limit10}}, want: nil},
		{name: "mismatched Value", input: BatchObjects{{Err: limit10}, {Err: limit20}}, want: nil},
		{name: "nil err anywhere disqualifies", input: BatchObjects{{Err: limit10}, {Err: nil}}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unanimousLimitExceeded(tt.input)
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tt.want.Limit, got.Limit)
			assert.Equal(t, tt.want.Value, got.Value)
		})
	}
}
