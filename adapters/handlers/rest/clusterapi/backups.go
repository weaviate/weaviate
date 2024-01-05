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
	"io"
	"net/http"

	"github.com/weaviate/weaviate/usecases/backup"
)

type backupManager interface {
	OnCanCommit(ctx context.Context, req *backup.Request) *backup.CanCommitResponse
	OnCommit(ctx context.Context, req *backup.StatusRequest) error
	OnAbort(ctx context.Context, req *backup.AbortRequest) error
	OnStatus(ctx context.Context, req *backup.StatusRequest) *backup.StatusResponse
}

type backups struct {
	manager backupManager
	auth    auth
}

func NewBackups(manager backupManager, auth auth) *backups {
	return &backups{manager: manager, auth: auth}
}

func (b *backups) CanCommit() http.Handler {
	return b.auth.handleFunc(b.canCommitHandler())
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
