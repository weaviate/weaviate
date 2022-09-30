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
	"fmt"
	"net/http"

	"github.com/semi-technologies/weaviate/entities/models"
	ubak "github.com/semi-technologies/weaviate/usecases/backup"
)

type backupManager interface {
	OnCanCommit(ctx context.Context, pr *models.Principal, req *ubak.Request) *ubak.CanCommitResponse
	OnCommit(ctx context.Context, req *ubak.StatusRequest) error
	OnAbort(ctx context.Context, req *ubak.AbortRequest) error
	OnStatus(ctx context.Context, req *ubak.StatusRequest) (*ubak.StatusResponse, error)
}

type backups struct {
	manager backupManager
}

func NewBackups(manager backupManager) *backups {
	return &backups{manager: manager}
}

func (b *backups) CanCommit() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := &ubak.Request{Method: "", ID: ""}

		// TODO: figure out what to do with principal here (nil)
		resp := b.manager.OnCanCommit(r.Context(), nil, req)
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

func (b *backups) Commit() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := &ubak.StatusRequest{Method: "", ID: ""}

		if err := b.manager.OnCommit(r.Context(), req); err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("commit: %w", err).Error(), status)
			return
		}

		w.WriteHeader(http.StatusCreated)
	})
}

func (b *backups) Abort() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := &ubak.AbortRequest{Method: "", ID: ""}

		if err := b.manager.OnAbort(r.Context(), req); err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("abort: %w", err).Error(), status)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (b *backups) Status() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := &ubak.StatusRequest{Method: "", ID: ""}

		resp, err := b.manager.OnStatus(r.Context(), req)
		if err != nil {
			status := http.StatusInternalServerError
			http.Error(w, fmt.Errorf("abort: %w", err).Error(), status)
			return
		}

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
