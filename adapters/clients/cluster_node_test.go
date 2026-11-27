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

package clients

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/replica"
)

// TestRemoteNodeStatusCarriesTheSchemaVersion checks a node status request
// puts the asking node's class version on the wire.
func TestRemoteNodeStatusCarriesTheSchemaVersion(t *testing.T) {
	var got string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query().Get(replica.SchemaVersionKey)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	_, err := NewRemoteNode(ts.Client()).GetNodeStatus(t.Context(), ts.URL[7:], "C1", "", verbosity.OutputVerbose, 7)

	require.NoError(t, err)
	require.Equal(t, "7", got)
}
