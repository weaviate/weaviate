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
	"sync"

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
	manager  backupManager
	activity nodeActivityProber
	auth     auth
	logger   logrus.FieldLogger

	// Whether a probe is wired cannot change while the process runs, so one
	// warning says everything a repeat would. The route answers once per peer
	// per gate evaluation, so a warning per call would scale with cluster size
	// times submission rate.
	warnUnwired sync.Once
}

// NewBackups requires a logger. The node-activity route logs on every answer,
// so a nil one turns a peer's question into a panic on a node that served every
// other backup route fine before that route existed.
func NewBackups(manager backupManager, activity nodeActivityProber, auth auth,
	logger logrus.FieldLogger,
) *backups {
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

		// 503, not 404: 404 would tell the caller to give up and let this node pass.
		if b.activity == nil {
			b.warnUnwired.Do(func() {
				log.Warn("backup node activity probe is not wired on this node, so every peer " +
					"asking whether a backup is running is answered 503")
			})
			http.Error(w, "backup activity probe not wired on this node, so it cannot say "+
				"whether a backup is running", http.StatusServiceUnavailable)
			return
		}

		activity := b.activity.Activity()
		log.WithField("busy", activity.Busy).
			WithField("kind", activity.Kind).
			WithField("id", clusterprobe.Loggable(activity.ID)).
			Debug("backup node activity probe answered")

		data, err := json.Marshal(backup.NewNodeActivityResponse(b.activity.Node(), activity))
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
