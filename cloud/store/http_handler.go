//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"net/http"
	"strings"
)

type handler struct {
	c Cluster
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/raft/join") {
		w.WriteHeader(http.StatusNotFound)
	}
	// h.join(w, r)
}

func NewHandler(c Cluster) http.Handler {
	return &handler{c}
}

// func (h *handler) join(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != "POST" {
// 		w.WriteHeader(http.StatusMethodNotAllowed)
// 		return
// 	}

// 	b, err := io.ReadAll(r.Body)
// 	if err != nil {
// 		w.WriteHeader(http.StatusBadRequest)
// 		return
// 	}
// 	joiner := Joiner{}
// 	if err := json.Unmarshal(b, &joiner); err != nil {
// 		w.WriteHeader(http.StatusBadRequest)
// 		return
// 	}

// 	if joiner.Addr == "" || joiner.ID == "" {
// 		w.WriteHeader(http.StatusBadRequest)
// 		return
// 	}

// 	log.Printf("join request %+v request", joiner)

// 	if err := h.c.Join(joiner.ID, joiner.Addr, joiner.Voter); err != nil {
// 		if err == ErrNotLeader {
// 			leaderAddr := h.c.Leader()
// 			if leaderAddr == "" {
// 				http.Error(w, errLeaderNotFound.Error(), http.StatusServiceUnavailable)
// 				return
// 			}

// 			http.Redirect(w, r, string(leaderAddr), http.StatusMovedPermanently)
// 			return
// 		}

// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// }
