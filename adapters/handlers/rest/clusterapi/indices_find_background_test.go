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

package clusterapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/backgroundaccess"
	"github.com/weaviate/weaviate/entities/filters"
)

type findUUIDsShards struct {
	shards
	background []bool
}

func (f *findUUIDsShards) FindUUIDs(ctx context.Context, _, _ string, _ *filters.LocalFilter, _ int) ([]strfmt.UUID, error) {
	f.background = append(f.background, backgroundaccess.Is(ctx))
	return []strfmt.UUID{"a"}, nil
}

func TestFindUUIDsHandlerHonoursTheBackgroundMark(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   bool
	}{
		{name: "no header", want: false},
		{name: "background header", header: "true", want: true},
		{name: "unknown value", header: "yes", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			fake := &findUUIDsShards{}
			h := NewIndices(fake, nil, NewNoopAuthHandler(), func() bool { return false }, logger).Indices()

			body, err := shared.IndicesPayloads.FindUUIDsParams.Marshal(nil, 10)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/indices/C1/shards/S1/objects/_find", bytes.NewReader(body))
			shared.IndicesPayloads.FindUUIDsParams.SetContentTypeHeaderReq(req)
			if tt.header != "" {
				req.Header.Set(backgroundaccess.Header, tt.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, []bool{tt.want}, fake.background)
		})
	}
}
