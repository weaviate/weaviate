//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db_users

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
)

type FakeNodeResolver struct {
	path string
}

func (f FakeNodeResolver) NodeHostname(nodeName string) (string, bool) {
	return strings.TrimPrefix(f.path, "http://"), true
}

type fakeHandler struct {
	t             *testing.T
	nodeResponses []map[string]time.Time
	counter       atomic.Int32
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	require.Equal(f.t, http.MethodPost, r.Method)

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var body apikey.UserStatusRequest
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		http.Error(w, "Error parsing JSON body", http.StatusBadRequest)
		return
	}
	if !body.ReturnStatus {
		return
	}
	counter := f.counter.Add(1) - 1

	ret := apikey.UserStatusResponse{Users: f.nodeResponses[counter]}
	outBytes, err := json.Marshal(ret)
	require.Nil(f.t, err)

	w.Write(outBytes)
}
