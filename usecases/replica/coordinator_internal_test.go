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

package replica

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsReplicaShardNotFoundErr(t *testing.T) {
	t.Run("matches StatusShardNotFound wrapped errors", func(t *testing.T) {
		err := fmt.Errorf("outer: %w", &Error{Code: StatusShardNotFound, Msg: "S1", Err: errors.New("inner")})
		assert.True(t, isReplicaShardNotFoundErr(err))
	})

	t.Run("does not match generic errors", func(t *testing.T) {
		assert.False(t, isReplicaShardNotFoundErr(errors.New("shard not found locally")))
	})
}
