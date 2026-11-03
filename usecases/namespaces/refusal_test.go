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

package namespaces

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func TestRefusalText(t *testing.T) {
	t.Run("the guard's unknown-state error gets the sentinel's text appended", func(t *testing.T) {
		err := fmt.Errorf("%w: shard %q", fmt.Errorf("%w %q: %w",
			errors.New("unknown namespace state"), "x", ErrInvalidState), "s")
		text, ok := RefusalText(err)
		assert.True(t, ok)
		assert.Equal(t, `unknown namespace state "x": namespace is in an invalid state for this operation: `+
			`shard "s": namespace is in an invalid state for this operation`, text)
	})

	t.Run("every error RequireShardLoadable returns is a refusal", func(t *testing.T) {
		states := []cmd.NamespaceState{"not-a-state"}
		for state := range stateTransitions {
			states = append(states, state)
		}
		for _, state := range states {
			err := RequireShardLoadable(state)
			if err == nil {
				continue
			}
			_, ok := RefusalText(err)
			assert.True(t, ok, "state %q: %v", state, err)
		}
	})
}

func TestRefusalFromText(t *testing.T) {
	type testCase struct {
		name string
		msg  string
		want error
	}
	tests := []testCase{
		{
			name: "a sentinel's text followed by more text is no refusal",
			msg:  ErrNamespaceSuspended.Error() + `: shard "s"` + "\n",
		},
	}
	for _, sentinel := range refusalSentinels {
		tests = append(tests, testCase{
			name: sentinel.Error() + " ending the text before its newline",
			msg:  `get shard "s": ` + sentinel.Error() + "\n",
			want: sentinel,
		})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, RefusalFromText(tc.msg))
		})
	}
}
