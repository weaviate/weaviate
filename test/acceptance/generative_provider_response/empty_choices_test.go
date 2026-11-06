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

package generative_provider_response

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	grpchelper "github.com/weaviate/weaviate/test/helper/grpc"
)

// emptyChoicesProvider is an OpenAI-compatible server that answers every
// request with HTTP 200 and an empty choices array.
type emptyChoicesProvider struct {
	port     int
	requests atomic.Int64
}

func newEmptyChoicesProvider(t *testing.T) *emptyChoicesProvider {
	t.Helper()
	p := &emptyChoicesProvider{}

	// Bind all interfaces (not httptest's default loopback) so the container
	// can reach the server through the host-gateway IP.
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	p.port = ln.Addr().(*net.TCPAddr).Port

	server := &httptest.Server{
		Listener: ln,
		Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p.requests.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"choices":[]}`))
		})},
	}
	server.Start()
	t.Cleanup(server.Close)
	return p
}

func (p *emptyChoicesProvider) baseURL() string {
	return fmt.Sprintf("http://host.docker.internal:%d", p.port)
}

// TestGenerativeEmptyChoicesReturnsError runs single-prompt and grouped-task
// generative searches over gRPC against a provider that answers with no
// choices. With DISABLE_RECOVERY_ON_PANIC=true a panic anywhere in the
// request kills the server, so a running container proves none happened.
func TestGenerativeEmptyChoicesReturnsError(t *testing.T) {
	ctx := context.Background()
	const className = "EmptyChoices"

	searches := []struct {
		name       string
		generative *pb.GenerativeSearch
	}{
		{
			name: "single prompt",
			generative: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{Prompt: "Describe {name}"},
			},
		},
		{
			name: "grouped task",
			generative: &pb.GenerativeSearch{
				Grouped: &pb.GenerativeSearch_Grouped{
					Task:       "Summarize these objects",
					Properties: &pb.TextArray{Values: []string{"name"}},
				},
			},
		},
		{
			name: "single prompt and grouped task",
			generative: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{Prompt: "Describe {name}"},
				Grouped: &pb.GenerativeSearch_Grouped{
					Task:       "Summarize these objects",
					Properties: &pb.TextArray{Values: []string{"name"}},
				},
			},
		},
	}

	for _, disableRecovery := range []bool{false, true} {
		t.Run(fmt.Sprintf("DISABLE_RECOVERY_ON_PANIC=%t", disableRecovery), func(t *testing.T) {
			provider := newEmptyChoicesProvider(t)

			compose, err := docker.New().
				WithWeaviateWithGRPC().
				WithGenerativeOpenAI("sk-test", "", "").
				WithWeaviateHostGateway().
				WithWeaviateEnv("DISABLE_RECOVERY_ON_PANIC", strconv.FormatBool(disableRecovery)).
				Start(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

			weaviate := compose.GetWeaviate()
			helper.SetupClient(weaviate.URI())
			helper.SetupGRPCClient(t, weaviate.GrpcURI())

			helper.CreateClass(t, &models.Class{
				Class:      className,
				Vectorizer: "none",
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
				},
				ModuleConfig: map[string]any{
					"generative-openai": map[string]any{
						"baseURL": provider.baseURL(),
						"model":   "gpt-4o-mini",
					},
				},
			})
			helper.CreateObjectsBatch(t, []*models.Object{
				{Class: className, Properties: map[string]any{"name": "Mercury"}},
				{Class: className, Properties: map[string]any{"name": "Venus"}},
			})

			for _, tt := range searches {
				t.Run(tt.name, func(t *testing.T) {
					before := provider.requests.Load()

					_, err := grpchelper.SearchWithTimeout(t, &pb.SearchRequest{
						Collection:  className,
						Limit:       2,
						Generative:  tt.generative,
						Uses_127Api: true,
					}, time.Minute)
					require.Error(t, err)
					require.ErrorContains(t, err, "empty choices in response")
					require.NotContains(t, err.Error(), "panic")
					require.Greater(t, provider.requests.Load(), before, "the provider was never called")

					state, err := weaviate.Container().State(ctx)
					require.NoError(t, err)
					require.True(t, state.Running, "weaviate exited with code %d", state.ExitCode)

					reply := grpchelper.AssertSearch(t, &pb.SearchRequest{
						Collection:  className,
						Limit:       2,
						Uses_127Api: true,
					})
					require.Len(t, reply.Results, 2)
				})
			}
		})
	}
}
