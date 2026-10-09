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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// uncalledShards fails the test when HashTreeLevel reaches it. Any other
// method nil-panics through the embedded shards.
type uncalledShards struct {
	shards
	t *testing.T
}

func (s uncalledShards) HashTreeLevel(context.Context, string, string, int,
	*hashtree.Bitset,
) ([]hashtree.Digest, error) {
	s.t.Error("HashTreeLevel called after the 405")
	return nil, nil
}

func TestIndicesPostOnlyRoutesRejectOtherMethods(t *testing.T) {
	logger, _ := test.NewNullLogger()
	idx := NewIndices(uncalledShards{t: t}, startedDB{},
		NewNoopAuthHandler(), func() bool { return false }, logger)

	body, err := hashtree.NewBitset(8).Marshal()
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet,
		"/indices/MyClass/shards/myshard/objects/hashtree/0", bytes.NewReader(body))

	rec := httptest.NewRecorder()
	idx.Indices().ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code, "body: %s", rec.Body.String())
}
