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

package backgroundaccess

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarkCrossesTheWire(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		header string
		marked bool
	}{
		{name: "unmarked", ctx: context.Background()},
		{name: "marked", ctx: With(context.Background()), header: "true", marked: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.marked, Is(tt.ctx))
			req, err := http.NewRequestWithContext(tt.ctx, http.MethodPost, "http://host/path", nil)
			require.NoError(t, err)
			SetHeader(req)
			require.Equal(t, tt.header, req.Header.Get(Header))
			require.Equal(t, tt.marked, Is(FromHeader(context.Background(), req.Header)))
			require.Equal(t, tt.header, Value(tt.ctx))
			require.Equal(t, tt.marked, Is(FromValue(context.Background(), Value(tt.ctx))))
		})
	}
	require.False(t, Is(FromValue(context.Background(), "yes")))
}
