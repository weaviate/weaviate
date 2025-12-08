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
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/objectTTL"
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
		case "/cluster/objectTTL/deleteExpired":
			if r.Method != http.MethodPost {
				msg := fmt.Sprintf("/objectTTL api path %q with method %v not found", path, r.Method)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			d.incomingDelete().ServeHTTP(w, r)
			return
		case "/cluster/objectTTL/status":
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
	})
}

func (d *ObjectTTL) incomingDelete() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if !d.requestRunning.CompareAndSwap(false, true) {
			http.Error(w, "another request is still being processed", http.StatusTooManyRequests)
			return
		}

		var body []objectTTL.ObjectsExpiredPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Error parsing JSON body", http.StatusBadRequest)
			return
		}

		eg := enterrors.NewErrorGroupWrapper(d.logger)

		// make sure to unlock the requestRunning flag when all deletions are dine
		enterrors.GoWrapper(func() {
			defer d.requestRunning.Store(false)
			err := eg.Wait()
			if err != nil {
				d.logger.WithError(err).Error("incoming delete expired objects failed")
			}
		}, d.logger)

		ec := errorcompounder.New()
		for _, classPayload := range body {
			className := classPayload.Class

			idx, err := d.remoteIndex.IndexForIncomingWrite(r.Context(), className, 0)
			if err != nil {
				ec.Add(fmt.Errorf("get index for class %q: %w", className, err))
				continue
			}

			err = idx.IncomingDeleteObjectsExpired(r.Context(), eg, classPayload.Prop, time.UnixMilli(classPayload.TtlMilli), time.UnixMilli(classPayload.DelMilli), 0)
			if err != nil {
				ec.Add(fmt.Errorf("delete expired for class %q: %w", className, err))
				continue
			}

		}

		if err := ec.ToError(); err != nil {
			http.Error(w, "/objectTTL response: "+err.Error(), http.StatusInternalServerError)
		}
		w.WriteHeader(http.StatusOK)
	})
}
