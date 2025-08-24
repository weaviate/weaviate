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

package clusterapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
)

type nodesManager interface {
	GetNodeStatus(ctx context.Context, className, shardName, output string) (*models.NodeStatus, error)
	GetStatistics(ctx context.Context) (*models.Statistics, error)
}

type nodes struct {
	nodesManager nodesManager
	auth         auth
}

func NewNodes(manager nodesManager, auth auth) *nodes {
	return &nodes{nodesManager: manager, auth: auth}
}

var (
	regxNodes      = regexp.MustCompile(`/status`)
	regxNodesClass = regexp.MustCompile(`/status/(` + entschema.ClassNameRegexCore + `)`)
	regxStatistics = regexp.MustCompile(`/statistics`)
)

func (s *nodes) Nodes() http.Handler {
	return s.auth.handleFunc(s.nodesHandler())
}

func (s *nodes) nodesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		case regxStatistics.MatchString(path):
			if r.Method != http.MethodGet {
				msg := fmt.Sprintf("/nodes api path %q not found", path)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			s.incomingStatistics().ServeHTTP(w, r)
			return
		default:
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
	}
}

func (s *nodes) incomingNodeStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var className string

		// url is /nodes/status[/className], where /className is optional
		args := strings.Split(r.URL.Path, "/")
		if len(args) == 4 {
			className = args[3]
		}

		// shard is an optional query parameter
		shardName := r.URL.Query().Get("shard")

		output := verbosity.OutputMinimal
		out, found := r.URL.Query()["output"]
		if found && len(out) > 0 {
			output = out[0]
		}
		nodeStatus, err := s.nodesManager.GetNodeStatus(r.Context(), className, shardName, output)
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

func (s *nodes) incomingStatistics() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		statistics, err := s.nodesManager.GetStatistics(r.Context())
		if err != nil {
			http.Error(w, "/nodes fulfill request: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		if statistics == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		statisticsBytes, err := json.Marshal(statistics)
		if err != nil {
			http.Error(w, "/nodes marshal response: "+err.Error(),
				http.StatusInternalServerError)
		}

		w.Write(statisticsBytes)
	})
}
