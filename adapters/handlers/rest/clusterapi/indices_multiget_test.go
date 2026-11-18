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
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storobj"
)

// multiGetShards embeds shards so that only MultiGetObjects needs a body, and
// records what the handler passes on.
type multiGetShards struct {
	shards
	ids   []strfmt.UUID
	props []string
	addl  additional.Properties
}

func (m *multiGetShards) MultiGetObjects(_ context.Context, _, _ string,
	ids []strfmt.UUID, props []string, addl additional.Properties,
) ([]*storobj.Object, error) {
	m.ids, m.props, m.addl = ids, props, addl
	return nil, nil
}

// TestMultiGetObjectsPassesSelectionToShards sends a multi-get through the REST
// client and handler. A request without props must reach the shards as nil
// props, which reads whole objects.
func TestMultiGetObjectsPassesSelectionToShards(t *testing.T) {
	ids := []strfmt.UUID{"73f2eb5f-5abf-447a-81ca-74b1dd168247"}

	cases := []struct {
		name  string
		props []string
		addl  additional.Properties
		// idsOnly sends what a node that never sends props sends: only ids
		idsOnly bool
	}{
		{name: "selected props and vectors", props: []string{"name"}, addl: additional.Properties{Vector: true, Vectors: []string{"title"}}},
		{name: "no props selected", props: []string{}},
		{name: "request with only ids", idsOnly: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			shards := &multiGetShards{}
			server := httptest.NewServer(NewIndices(shards, startedDB{},
				NewNoopAuthHandler(), func() bool { return false }, logger).Indices())
			defer server.Close()
			host := strings.TrimPrefix(server.URL, "http://")

			if tc.idsOnly {
				encoded, err := json.Marshal(ids)
				require.NoError(t, err)
				query := url.Values{"ids": {base64.StdEncoding.EncodeToString(encoded)}}
				res, err := http.Get("http://" + host + "/indices/MyClass/shards/myshard/objects?" + query.Encode())
				require.NoError(t, err)
				defer res.Body.Close()
				require.Equal(t, http.StatusOK, res.StatusCode)
			} else {
				_, err := clients.NewRemoteIndex(&http.Client{}).MultiGetObjects(context.Background(),
					host, "MyClass", "myshard", ids, tc.props, tc.addl)
				require.NoError(t, err)
			}

			assert.Equal(t, ids, shards.ids)
			assert.Equal(t, tc.props, shards.props)
			assert.Equal(t, tc.addl, shards.addl)
		})
	}
}
