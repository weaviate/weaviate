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

package acceptance_with_go_v6_client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"

	"acceptance_tests_with_v6_client/internal/wvhost"
)

// NewClient connects to the Weaviate instance under test and closes the
// connection when the test ends.
func NewClient(t *testing.T, ctx context.Context, options ...client.Option) *client.Client {
	t.Helper()

	c, err := client.NewClient(ctx, append([]client.Option{
		client.WithScheme("http"),
		client.WithHost(wvhost.Host()),
		client.WithHTTPPort(wvhost.RESTPort()),
		client.WithGRPCPort(wvhost.GRPCPort()),
	}, options...)...)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// contextionary is the text2vec-contextionary vectorizer. The v6 client ships
// definitions only for a handful of modules; any other one is just its name.
type contextionary struct{}

func (contextionary) Name() string { return "text2vec-contextionary" }

// Insert adds objects to a collection and fails the test if the server rejected
// any of them. Inserts are batched, so rejections are reported per object
// instead of as a request error.
func Insert(t *testing.T, ctx context.Context, h *collections.Handle, objects ...*data.Object) {
	t.Helper()

	result, err := h.Data.Insert(ctx, objects...)
	require.NoError(t, err)
	for id, msg := range result.Errors {
		t.Fatalf("insert %s: %s", id, msg)
	}
}
