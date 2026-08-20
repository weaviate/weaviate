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
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate/test/docker"

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

// NewClientForContainer connects to a Weaviate started by the test/docker
// helpers. The container must publish its gRPC port -- the v6 client sends
// every query and every write over gRPC, so docker.Compose.WithWeaviate() is
// not enough, WithWeaviateWithGRPC() is required.
//
// Container endpoints change whenever the container is restarted, so call this
// again after a restart instead of reusing the client.
func NewClientForContainer(t *testing.T, ctx context.Context, container *docker.DockerContainer, options ...client.Option) *client.Client {
	t.Helper()

	restHost, restPort := splitHostPort(t, container.URI())
	grpcHost, grpcPort := splitHostPort(t, container.GrpcURI())

	c, err := client.NewClient(ctx, append([]client.Option{
		client.WithScheme("http"),
		client.WithHTTPHost(restHost),
		client.WithHTTPPort(restPort),
		client.WithGRPCHost(grpcHost),
		client.WithGRPCPort(grpcPort),
	}, options...)...)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func splitHostPort(t *testing.T, uri string) (string, int) {
	t.Helper()

	host, port, err := net.SplitHostPort(uri)
	require.NoErrorf(t, err, "unexpected endpoint %q", uri)

	p, err := strconv.Atoi(port)
	require.NoErrorf(t, err, "unexpected port in endpoint %q", uri)

	return host, p
}
