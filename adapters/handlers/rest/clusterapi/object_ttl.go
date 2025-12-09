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
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type ObjectTTL struct {
	remoteIndex    *sharding.RemoteIndexIncoming
	auth           auth
	requestRunning atomic.Bool
	logger         logrus.FieldLogger
}

func NewObjectTTL(remoteIndex *sharding.RemoteIndexIncoming, auth auth, logger logrus.FieldLogger) *ObjectTTL {
	return &ObjectTTL{remoteIndex: remoteIndex, auth: auth, requestRunning: atomic.Bool{}, logger: logger}
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

		d.logger.WithFields(logrus.Fields{
			"action":  "object_ttl_deletions",
			"classes": len(body),
		}).Info("received request to delete expired objects")

		// run the deletion in a separate goroutine to free up the HTTP handler immediately
		enterrors.GoWrapper(func() {
			// make sure to unlock the requestRunning flag when all deletions are done
			defer d.requestRunning.Store(false)

			start := time.Now()

			ec := errorcompounder.NewSafe()
			eg := enterrors.NewErrorGroupWrapper(d.logger)
			eg.SetLimit(concurrency.GOMAXPROCS) // TODO aliszka:ttl move to config

			objectsDeleted := atomic.Int32{}
			onObjectsDeleted := func(count int32) { objectsDeleted.Add(count) }

			for _, classPayload := range body {
				collection := classPayload.Class
				onCollectionError := func(err error) {
					if err != nil {
						ec.Add(fmt.Errorf("collection %q [%w]", collection, err))
					}
				}
				// TODO aliszka:ttl add server shutdown context check?

				// TODO aliszka:ttl index protected from closing/deletion?
				// TODO aliszka:ttl schema version?
				idx, err := d.remoteIndex.IndexForIncomingWrite(context.Background(), collection, 0)
				if err != nil {
					onCollectionError(fmt.Errorf("get index: %w", err))
					continue
				}
				idx.IncomingDeleteObjectsExpired(classPayload.Prop, time.UnixMilli(classPayload.TtlMilli), time.UnixMilli(classPayload.DelMilli),
					eg, onCollectionError, onObjectsDeleted, 0)
			}

			eg.Wait() // ignore errors from goroutines, they are collected in ec

			l := d.logger.WithFields(logrus.Fields{
				"action":        "object_ttl_deletions",
				"total_deleted": objectsDeleted.Load(),
				"took":          time.Since(start).String(),
			})

			if err := ec.ToError(); err != nil {
				l.WithError(err).Errorf("incoming ttl deletions finished with errors")
				return
			}
			l.Info("incoming ttl deletions successfully finished")

		}, d.logger)

		w.WriteHeader(http.StatusAccepted)
	})
}
