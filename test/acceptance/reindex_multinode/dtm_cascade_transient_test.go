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

package reindex_multinode

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Regression guard for the post-rolling-restart flake in
// TestMultiNode_DeleteRecreateCleansReindexTasks: right after a node restarts,
// its internal gRPC connection for ListDistributedTasks is briefly
// reconnecting, so GET /v1/tasks transiently returns a non-200 ("the client
// connection is closing"). assertTaskGone's Eventually is meant to ride that
// out, but previously its inner read hard-asserted status==200 and so
// FailNow'd from the Eventually condition goroutine on the first transient.
// This drives assertTaskGone against a server that returns the transient for
// the first few polls and then a clean empty task list; it must pass.
func TestAssertTaskGone_ToleratesTransientReadError(t *testing.T) {
	var polls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&polls, 1) <= 3 {
			// The post-restart reconnect window.
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"error":[{"message":"list distributed tasks: failed to execute query: rpc error: code = Canceled desc = grpc: the client connection is closing"}]}`)
			return
		}
		// gRPC connection re-established: the cascade-deleted task is absent.
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"reindex":[]}`)
	}))
	defer srv.Close()

	restURI := strings.TrimPrefix(srv.URL, "http://")
	assertTaskGone(t, restURI, "DTMCascadeDelete:change-tokenization:text:dead", "transient-tolerance proof")
}
