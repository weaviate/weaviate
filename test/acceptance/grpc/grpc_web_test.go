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

package test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestGRPCWebSearchOverRESTPort exercises the grpc-web feature end to end: a
// browser-shaped Connect-protocol JSON POST to /v1/grpc-web/weaviate.v1.Weaviate/Search
// on the REST port must be transcoded to the in-process gRPC Search and return the
// objects that were inserted over REST. It is deliberately non-vacuous: a REST 404
// fallthrough (grpc-web unmounted/misrouted), an empty result set, or an undecodable
// body each fail loudly, so it guards the wiring the unit-level grpcweb tests cannot
// reach (the real configure_api composition serving on the live REST listener).
func TestGRPCWebSearchOverRESTPort(t *testing.T) {
	ctx := context.Background()

	// REST-port-only node: the transcoder wraps the in-process gRPC server object,
	// so grpc-web is served on the REST listener with no gRPC port exposed.
	compose, err := docker.New().
		WithWeaviate().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	class := &models.Class{
		Class:      "GRPCWebSearch",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "wordCount", DataType: []string{"int"}},
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, class.Class)

	books := []struct {
		title     string
		wordCount int64
	}{
		{"Dune", 412},
		{"Foundation", 255},
		{"Neuromancer", 271},
	}
	// Order-insensitive expectation keyed by title: a plain Search returns objects in
	// no guaranteed order.
	want := make(map[string]int64, len(books))
	for _, b := range books {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.NewString()),
			Properties: map[string]interface{}{
				"title":     b.title,
				"wordCount": b.wordCount,
			},
		}))
		want[b.title] = b.wordCount
	}

	endpoint := fmt.Sprintf("http://%s/v1/grpc-web/weaviate.v1.Weaviate/Search", compose.GetWeaviate().URI())
	// Connect-protocol unary JSON body (protojson lowerCamelCase field names). uses127Api
	// is the modern, non-deprecated variant of the uses_12X_api flags used by the sibling
	// gRPC search tests; returnAllNonrefProperties makes the values come back under
	// nonRefProps.fields, and metadata.uuid proves the id round-trips too.
	reqBody := fmt.Sprintf(
		`{"collection":%q,"limit":100,"uses127Api":true,"metadata":{"uuid":true},"properties":{"returnAllNonrefProperties":true}}`,
		class.Class,
	)

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		status, contentType, body, err := grpcWebSearch(ctx, endpoint, reqBody)
		require.NoError(ct, err)
		require.Equalf(ct, http.StatusOK, status,
			"grpc-web Search must return 200 (a REST 404 fallthrough means the mount is not serving); body=%s", body)
		require.Truef(ct, strings.HasPrefix(contentType, "application/json"),
			"grpc-web Search must reply as Connect JSON; got content-type %q body=%s", contentType, body)

		// Strict decode: protojson rejects unknown fields by default, so a Connect error
		// envelope or an HTML/JSON error page fails here rather than passing vacuously.
		var reply pb.SearchReply
		require.NoErrorf(ct, protojson.Unmarshal(body, &reply), "strict-decode SearchReply; body=%s", body)
		require.Lenf(ct, reply.GetResults(), len(books),
			"returned result count must equal inserted count; body=%s", body)

		got := make(map[string]int64, len(books))
		for _, r := range reply.GetResults() {
			require.NotEmptyf(ct, r.GetMetadata().GetId(), "each result must carry its uuid; body=%s", body)
			fields := r.GetProperties().GetNonRefProps().GetFields()
			got[fields["title"].GetTextValue()] = fields["wordCount"].GetIntValue()
		}
		require.Equal(ct, want, got, "returned property values must equal the inserted values")
	}, 30*time.Second, 2*time.Second, "grpc-web Search did not return the inserted objects in time")
}

// grpcWebSearch POSTs a Connect-protocol JSON Search request to the grpc-web mount on
// the REST port and returns the status, content type and raw body for strict validation.
func grpcWebSearch(ctx context.Context, endpoint, reqJSON string) (int, string, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(reqJSON))
	if err != nil {
		return 0, "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Vanguard classifies a bare application/json POST as REST (transcoder.go
	// classifyRequest); the Connect-Protocol-Version header is what selects the
	// Connect unary protocol so the body is decoded as the request message.
	req.Header.Set("Connect-Protocol-Version", "1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, resp.Header.Get("Content-Type"), nil, err
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), body, nil
}
