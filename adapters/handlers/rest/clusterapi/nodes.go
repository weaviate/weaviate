//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clusterapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
)

type nodesManager interface {
	GetNodeStatus(ctx context.Context) (*models.NodeStatus, error)
}

type nodes struct {
	nodesManager nodesManager
}

func NewNodes(manager nodesManager) *nodes {
	return &nodes{nodesManager: manager}
}

func (s *nodes) Nodes() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/status"):
			if r.Method != http.MethodGet {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			s.incomingNodeStatus().ServeHTTP(w, r)
			return
		default:
			http.Error(w, "415 Unsupported Media Type", http.StatusUnsupportedMediaType)
			return
		}
	})
}

func (s *nodes) incomingNodeStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		nodeStatus, err := s.nodesManager.GetNodeStatus(r.Context())
		if err != nil {
			http.Error(w, "error getting node status: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		if nodeStatus == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		nodeStatusBytes, err := json.Marshal(nodeStatus)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Write(nodeStatusBytes)
	})
}
