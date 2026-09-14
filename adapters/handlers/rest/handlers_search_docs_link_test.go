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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	restsearch "github.com/weaviate/weaviate/adapters/handlers/rest/search"
	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestSearchErrPayloadDocsLink(t *testing.T) {
	tests := []struct {
		name      string
		principal *models.Principal
		err       error
		want      string
	}{
		{
			name: "undocumented error is passed through",
			err:  fmt.Errorf("explorer: get class: something else"),
			want: "explorer: get class: something else",
		},
		{
			name: "documented error gets the page appended",
			err:  fmt.Errorf("explorer: get class: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want: "explorer: get class: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
		{
			name:      "namespace named after the link's scheme leaves the link intact",
			principal: &models.Principal{Username: "u", Namespace: "https"},
			err:       fmt.Errorf("explorer: get class: https:Articles: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want:      "explorer: get class: Articles: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// the search package strips the namespace before handing the error over
			apiErr := &restsearch.APIError{Status: http.StatusInternalServerError, Err: namespacing.StripErrForPrincipal(tt.principal, tt.err)}
			payload := searchErrPayload(apiErr)
			require.Len(t, payload.Error, 1)
			assert.Equal(t, tt.want, payload.Error[0].Message)
		})
	}
}

// failingSearch answers every search with err, the way the traverser reports a
// failure from the engine below it.
type failingSearch struct{ err error }

func (f failingSearch) GetClass(context.Context, *models.Principal, dto.GetParams) ([]any, error) {
	return nil, f.err
}

func (f failingSearch) Aggregate(context.Context, *models.Principal, *aggregation.Params) (any, error) {
	return nil, f.err
}

// searchableSchema is a real schema reader over one collection a keyword
// search can run against.
func searchableSchema(t *testing.T, name string) clusterSchema.SchemaReader {
	t.Helper()
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	sm := clusterSchema.NewSchemaManager("node1", nil, parser, prometheus.NewPedanticRegistry(), logger)

	class := &models.Class{
		Class:      name,
		Properties: []*models.Property{{Name: "title", DataType: schema.DataTypeText.PropString()}},
	}
	sub, err := json.Marshal(api.AddClassRequest{
		Class: class,
		State: &sharding.State{PartitioningEnabled: true},
	})
	require.NoError(t, err)
	require.NoError(t, sm.AddClass(&api.ApplyRequest{
		Type: api.ApplyRequest_TYPE_ADD_CLASS, Class: class.Class, SubCommand: sub,
	}, "node1", true, false))

	return sm.NewSchemaReader()
}

// TestSearchErrPayloadDocsLinkThroughHandler: the handler shortens an
// internal failure's message, so the reply body's link has to come off the
// cause it keeps. A documented failure keeps its own text and its page; an
// undocumented one gets the generic message and no page.
func TestSearchErrPayloadDocsLinkThroughHandler(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "documented failure keeps its message and page",
			err: fmt.Errorf("explorer: get class: local shard object search movie_abc: cannot init shard: %w",
				enterrors.ErrNotEnoughMappings),
			want: "cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
		{
			name: "undocumented failure is generic",
			err:  fmt.Errorf("explorer: get class: local shard object search movie_abc: cannot parse stored value"),
			want: "internal server error; details are in the server log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := restsearch.NewHandler(restsearch.HandlerConfig{
				Traverser:    failingSearch{err: tt.err},
				SchemaReader: searchableSchema(t, "Movie"),
				Authorizer:   mocks.NewMockAuthorizer(),
				DefaultLimit: 10,
			})

			assertBody := func(apiErr *restsearch.APIError) {
				require.NotNil(t, apiErr)
				require.Equal(t, http.StatusInternalServerError, apiErr.Status)
				payload := searchErrPayload(apiErr)
				require.Len(t, payload.Error, 1)
				assert.Equal(t, tt.want, payload.Error[0].Message)
			}

			query := "space"
			_, searchErr := h.Bm25(context.Background(), nil, "Movie", &models.SearchBm25Request{Query: &query})
			assertBody(searchErr)

			// aggregate strips its errors on its own path
			_, aggregateErr := h.Aggregate(context.Background(), nil, "Movie", &models.AggregateRequest{})
			assertBody(aggregateErr)
		})
	}
}
