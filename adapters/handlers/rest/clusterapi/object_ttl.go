//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clusterapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type ObjectTTL struct {
	remoteIndex    *sharding.RemoteIndexIncoming
	auth           auth
	requestRunning atomic.Bool
	logger         logrus.FieldLogger
	config         config.Config
}

func NewObjectTTL(remoteIndex *sharding.RemoteIndexIncoming, auth auth, logger logrus.FieldLogger, config config.Config) *ObjectTTL {
	return &ObjectTTL{
		remoteIndex:    remoteIndex,
		auth:           auth,
		requestRunning: atomic.Bool{},
		logger:         logger,
		config:         config,
	}
}

func (d *ObjectTTL) Expired() http.Handler {
	return d.auth.handleFunc(d.deleteExpiredHandler())
}

func (d *ObjectTTL) deleteExpiredHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch path {
		case "/cluster/object_ttl/delete_expired":
			if r.Method != http.MethodPost {
				msg := fmt.Sprintf("/objectTTL api path %q with method %v not found", path, r.Method)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			d.incomingDelete().ServeHTTP(w, r)
			return
		case "/cluster/object_ttl/status":
			if r.Method != http.MethodGet {
				msg := fmt.Sprintf("/objectTTL api path %q with method %v not found", path, r.Method)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			d.incomingStatus().ServeHTTP(w, r)
			return

		default:
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
	}
}

func (d *ObjectTTL) incomingStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		running := d.requestRunning.Load()
		status := objectttl.ObjectsExpiredStatusResponse{
			DeletionOngoing: running,
		}
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(status); err != nil {
			http.Error(w, "/object ttl marshal response: "+err.Error(),
				http.StatusInternalServerError)
		}
	})
}

func (d *ObjectTTL) incomingDelete() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if !d.requestRunning.CompareAndSwap(false, true) {
			http.Error(w, "another request is still being processed", http.StatusTooManyRequests)
			return
		}

		var body []objectttl.ObjectsExpiredPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			d.requestRunning.Store(false)
			http.Error(w, "Error parsing JSON body", http.StatusBadRequest)
			return
		}

		d.logger.WithFields(logrus.Fields{"action": "incoming_delete_expired_objects", "classes": len(body)}).Info("received request to delete expired objects")

		// run the deletion in a separate goroutine to free up the HTTP handler immediately
		enterrors.GoWrapper(func() {
			// make sure to unlock the requestRunning flag when all deletions are done
			defer d.requestRunning.Store(false)
			eg := enterrors.NewErrorGroupWrapper(d.logger)
			eg.SetLimit(concurrency.TimesFloatGOMAXPROCS(d.config.ObjectsTTLConcurrencyFactor))

			ec := errorcompounder.NewSafe()
			for _, classPayload := range body {
				className := classPayload.Class

				// TODO aliszka:ttl handle graceful index close / drop
				idx, err := d.remoteIndex.IndexForIncomingWrite(context.Background(), className, classPayload.ClassVersion)
				if err != nil {
					ec.Add(fmt.Errorf("get index for class %q: %w", className, err))
					continue
				}

				idx.IncomingDeleteObjectsExpired(eg, ec, classPayload.Prop, time.UnixMilli(classPayload.TtlMilli), time.UnixMilli(classPayload.DelMilli), classPayload.ClassVersion)
			}

			eg.Wait() // ignore errors from goroutines, they are collected in ec
			if err := ec.ToError(); err != nil {
				d.logger.WithError(err).Error("incoming delete expired objects failed")
			}
		}, d.logger)

		w.WriteHeader(http.StatusAccepted)
	})
}
