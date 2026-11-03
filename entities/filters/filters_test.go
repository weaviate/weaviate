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

package filters

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/inverted"
)

func TestOperators(t *testing.T) {
	type test struct {
		op              Operator
		expectedName    string
		expectedOnValue bool
	}

	tests := []test{
		{op: OperatorEqual, expectedName: "Equal", expectedOnValue: true},
		{op: OperatorNotEqual, expectedName: "NotEqual", expectedOnValue: true},
		{op: OperatorGreaterThan, expectedName: "GreaterThan", expectedOnValue: true},
		{op: OperatorGreaterThanEqual, expectedName: "GreaterThanEqual", expectedOnValue: true},
		{op: OperatorLessThanEqual, expectedName: "LessThanEqual", expectedOnValue: true},
		{op: OperatorLessThan, expectedName: "LessThan", expectedOnValue: true},
		{op: OperatorWithinGeoRange, expectedName: "WithinGeoRange", expectedOnValue: true},
		{op: OperatorLike, expectedName: "Like", expectedOnValue: true},
		{op: OperatorAnd, expectedName: "And", expectedOnValue: false},
		{op: OperatorOr, expectedName: "Or", expectedOnValue: false},
		{op: ContainsAny, expectedName: "ContainsAny", expectedOnValue: true},
		{op: ContainsAll, expectedName: "ContainsAll", expectedOnValue: true},
		{op: ContainsNone, expectedName: "ContainsNone", expectedOnValue: true},
		{op: OperatorNot, expectedName: "Not", expectedOnValue: false},
		{op: Operator(999), expectedName: "Unknown(999)", expectedOnValue: false},
	}

	for _, test := range tests {
		t.Run(test.expectedName, func(t *testing.T) {
			assert.Equal(t, test.expectedName, test.op.Name(), "name must match")
			assert.Equal(t, test.expectedOnValue, test.op.OnValue(), "onValue must match")
		})
	}
}

func TestValueContainsKeys(t *testing.T) {
	t.Run("concurrent callers share one build", func(t *testing.T) {
		v := &Value{}
		var calls atomic.Int32
		build := func() (inverted.SortedKeys, error) {
			calls.Add(1)
			kb := inverted.NewFixedKeyBuilder(1, 1)
			kb.AppendBuf()[0] = 'x'
			return kb.Build()
		}
		const callers = 16
		got := make([]inverted.SortedKeys, callers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				keys, err := v.ContainsKeys(build)
				assert.NoError(t, err)
				got[i] = keys
			}()
		}
		close(start)
		wg.Wait()

		require.Equal(t, int32(1), calls.Load())
		for i := range got {
			require.Same(t, &got[0].At(0)[0], &got[i].At(0)[0], "caller %d must reuse the shared slab", i)
		}
	})

	t.Run("an error is cached and returned to every caller", func(t *testing.T) {
		v := &Value{}
		wantErr := errors.New("malformed")
		var calls atomic.Int32
		build := func() (inverted.SortedKeys, error) {
			calls.Add(1)
			return inverted.SortedKeys{}, wantErr
		}
		for range 3 {
			_, err := v.ContainsKeys(build)
			require.ErrorIs(t, err, wantErr)
		}
		require.Equal(t, int32(1), calls.Load())
	})
}
