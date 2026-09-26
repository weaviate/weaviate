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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStartedWithoutRaftStateTag(t *testing.T) {
	ctx := context.Background()
	require.False(t, IsStartedWithoutRaftState(ctx))
	require.False(t, IsStartedWithoutRaftState(WithStartupDBLoad(ctx)))

	tagged := WithStartupDBLoad(WithStartedWithoutRaftState(ctx))
	require.True(t, IsStartedWithoutRaftState(tagged))
	require.True(t, IsStartupDBLoad(tagged))

	tagged = WithStartedWithoutRaftState(WithStartupDBLoad(ctx))
	require.True(t, IsStartedWithoutRaftState(tagged))
	require.True(t, IsStartupDBLoad(tagged))
}
