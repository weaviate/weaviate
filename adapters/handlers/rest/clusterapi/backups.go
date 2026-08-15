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
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/clusterprobe"
	"github.com/weaviate/weaviate/usecases/backup"
)

type backupManager interface {
	OnCanCommit(ctx context.Context, req *backup.Request) *backup.CanCommitResponse
	OnCommit(ctx context.Context, req *backup.StatusRequest) error
	OnAbort(ctx context.Context, req *backup.AbortRequest) error
	OnStatus(ctx context.Context, req *backup.StatusRequest) *backup.StatusResponse
}

type nodeActivityProber interface {
	Node() string
	Activity() backup.NodeActivity
}

type backups struct {
	manager backupManager
	// Never nil in production: requireNodeActivityProbe stops the node at mux
	// build, so only a direct caller can pass nil, and only this route panics on it.
	activity nodeActivityProber
	auth     auth
	logger   logrus.FieldLogger
}

// NewBackups refuses a nil logger. The node-activity route logs on every answer,
// so a nil one turns a peer's question into a panic on a node that served every
// other backup route fine before that route existed.
func NewBackups(manager backupManager, activity nodeActivityProber, auth auth,
	logger logrus.FieldLogger,
) *backups {
	if logger == nil {
		panic("clusterapi: NewBackups needs a logger")
	}
	return &backups{manager: manager, activity: activity, auth: auth, logger: logger}
}

func (b *backups) CanCommit() http.Handler {
	return b.auth.handleFunc(b.canCommitHandler())
}

func (b *backups) NodeActivity() http.Handler {
	return b.auth.handleFunc(b.nodeActivityHandler())
}

func (b *backups) nodeActivityHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.Method != http.MethodGet {
			http.Error(w, clusterprobe.BackupNodeActivityPath+" only serves GET", http.StatusMethodNotAllowed)
			return
		}

		log := b.logger.WithField("action", "backup_node_activity_probe")

		activity := b.activity.Activity()
		// 503, not 404: a 404 reads as "too old to ask" and lets this node pass. And
		// not 200: serialized, an undecided answer is busy with no kind, which the
		// shipped client refuses as "cannot tell", so one rendering instead of three.
		if !activity.Answered {
			http.Error(w, "this node could not decide whether a backup is running",
				http.StatusServiceUnavailable)
			return
		}

		res := backup.NewNodeActivityResponse(b.activity.Node(), activity)
		log.WithField("busy", *res.Busy).
			WithField("kind", res.Kind).
			WithField("id", clusterprobe.Loggable(res.ID)).
			Debug("backup node activity probe answered")

		data, err := json.Marshal(res)
		if err != nil {
			http.Error(w, fmt.Errorf("marshal response: %w", err).Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}

func (b *backups) canCommitHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("read request body: %w", err).Error(), status)
			return
		}
		defer r.Body.Close()

		var req backup.Request
		if err := json.Unmarshal(body, &req); err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("unmarshal request: %w", err).Error(), status)
			return
		}

		resp := b.manager.OnCanCommit(r.Context(), &req)
		b, err := json.Marshal(&resp)
		if err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("marshal response: %w", err).Error(), status)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(b)
	}
}

func (b *backups) Commit() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("read request body: %w", err).Error(), status)
			return
		}
		defer r.Body.Close()

		var req backup.StatusRequest
		if err := json.Unmarshal(body, &req); err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("unmarshal request: %w", err).Error(), status)
			return
		}

		if err := b.manager.OnCommit(r.Context(), &req); err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("commit: %w", err).Error(), status)
			return
		}

		w.WriteHeader(http.StatusCreated)
	})
}

func (b *backups) Abort() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("read request body: %w", err).Error(), status)
			return
		}
		defer r.Body.Close()

		var req backup.AbortRequest
		if err := json.Unmarshal(body, &req); err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("unmarshal request: %w", err).Error(), status)
			return
		}

		params := r.URL.Query()
		req.Bucket = params.Get("bucket")
		req.Path = params.Get("path")

		if err := b.manager.OnAbort(r.Context(), &req); err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("abort: %w", err).Error(), status)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (b *backups) Status() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("read request body: %w", err).Error(), status)
			return
		}
		defer r.Body.Close()

		var req backup.StatusRequest
		if err := json.Unmarshal(body, &req); err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("unmarshal request: %w", err).Error(), status)
			return
		}

		params := r.URL.Query()
		if params.Get("bucket") != "" {
			req.Bucket = params.Get("bucket")
		}
		if params.Get("path") != "" {
			req.Path = params.Get("path")
		}

		resp := b.manager.OnStatus(r.Context(), &req)
		b, err := json.Marshal(&resp)
		if err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("marshal response: %w", err).Error(), status)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(b)
	})
}
