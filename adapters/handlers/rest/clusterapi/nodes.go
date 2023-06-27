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

package clusterapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"

	"github.com/weaviate/weaviate/entities/models"
)

type nodesManager interface {
	GetNodeStatus(ctx context.Context, className string) (*models.NodeStatus, error)
}

type nodes struct {
	nodesManager nodesManager
}

func NewNodes(manager nodesManager) *nodes {
	return &nodes{nodesManager: manager}
}

var (
	regxNodes      = regexp.MustCompile(`/status`)
	regxNodesClass = regexp.MustCompile(`/status/([A-Z][_0-9A-Za-z]*)`)
)

func (s *nodes) Nodes() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case regxNodes.MatchString(path) || regxNodesClass.MatchString(path):
			if r.Method != http.MethodGet {
				msg := fmt.Sprintf("/nodes api path %q not found", path)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			s.incomingNodeStatus().ServeHTTP(w, r)
			return
		default:
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
	})
}

func (s *nodes) incomingNodeStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var className string

		args := regxNodesClass.FindStringSubmatch(r.URL.Path)
		if len(args) == 3 {
			className = args[2]
		}

		nodeStatus, err := s.nodesManager.GetNodeStatus(r.Context(), className)
		if err != nil {
			http.Error(w, "/nodes fulfill request: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		if nodeStatus == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		nodeStatusBytes, err := json.Marshal(nodeStatus)
		if err != nil {
			http.Error(w, "/nodes marshal response: "+err.Error(),
				http.StatusInternalServerError)
		}

		w.Write(nodeStatusBytes)
	})
}
