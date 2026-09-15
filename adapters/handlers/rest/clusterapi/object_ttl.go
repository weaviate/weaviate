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
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type ObjectTTL struct {
	remoteIndex *sharding.RemoteIndexIncoming
	auth        auth
	logger      logrus.FieldLogger
	config      config.Config
	localStatus *objectttl.LocalStatus
}

func NewObjectTTL(remoteIndex *sharding.RemoteIndexIncoming, auth auth, logger logrus.FieldLogger,
	config config.Config, localStatus *objectttl.LocalStatus,
) *ObjectTTL {
	return &ObjectTTL{
		remoteIndex: remoteIndex,
		auth:        auth,
		logger:      logger,
		config:      config,
		localStatus: localStatus,
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
				msg := fmt.Sprintf("/object_ttl api path %q with method %v not found", path, r.Method)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			d.incomingDelete().ServeHTTP(w, r)
			return
		case "/cluster/object_ttl/status":
			if r.Method != http.MethodGet {
				msg := fmt.Sprintf("/object_ttl api path %q with method %v not found", path, r.Method)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			d.incomingStatus().ServeHTTP(w, r)
			return
		case "/cluster/object_ttl/abort":
			if r.Method != http.MethodPost {
				msg := fmt.Sprintf("/object_ttl api path %q with method %v not found", path, r.Method)
				http.Error(w, msg, http.StatusMethodNotAllowed)
				return
			}

			d.incomingAbort().ServeHTTP(w, r)
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

		status := objectttl.ObjectsExpiredStatusResponse{
			DeletionOngoing: d.localStatus.IsRunning(),
		}

		status.SetContentTypeHeader(w)
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

		// the slot is taken before the body is read, so a second caller is refused
		// without decoding one. An unbounded read is the cluster server's to bound;
		// what this handler owes is not to multiply it.
		ok, ttlCtx := d.localStatus.SetRunning()
		if !ok {
			http.Error(w, "another request is still being processed", http.StatusTooManyRequests)
			return
		}
		refuse := func(msg string) {
			d.localStatus.Finished()
			http.Error(w, msg, http.StatusBadRequest)
		}

		var body []objectttl.ObjectsExpiredPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			refuse("Error parsing JSON body")
			return
		}
		// the coordinator names each collection once, so a repeat asks for two
		// different deletions of one collection and says nothing about which wins
		named := make(map[string]string, len(body))
		for _, classPayload := range body {
			// db.indices is keyed by the lowercased class name, so two spellings
			// reach one index, and admitting both sweeps it twice at once
			key := strings.ToLower(classPayload.Class)
			if first, repeated := named[key]; repeated {
				refuse(fmt.Sprintf("collection named more than once: %s and %s",
					first, classPayload.Class))
				return
			}
			named[key] = classPayload.Class

			// the coordinator fills all three
			if classPayload.Prop == "" || classPayload.TtlMilli == 0 {
				refuse(fmt.Sprintf("collection %s: prop and ttlMilli are required",
					classPayload.Class))
				return
			}
			// the coordinator sets delMilli to the instant it swept; an omitted
			// field decodes to zero, which is a timestamp rather than an absence,
			// so nothing downstream can tell it apart from a real deletion time
			if classPayload.DelMilli <= 0 {
				refuse(fmt.Sprintf("collection %s: delMilli must be a deletion time",
					classPayload.Class))
				return
			}
		}

		// run the deletion in a separate goroutine to free up the HTTP handler immediately
		enterrors.GoWrapper(func() {
			// make sure to unlock the requestRunning flag when all deletions are done
			defer d.localStatus.Finished()

			started := time.Now()

			metrics := monitoring.GetMetrics()
			metrics.IncObjectsTtlCount()
			metrics.IncObjectsTtlRunning()

			var err error
			// count objects deleted per collection
			objsDeletedCounters := make(objectttl.DeletedCounters, len(body))
			colNames := make([]string, len(body))
			for i := range body {
				colNames[i] = body[i].Class
			}

			ec := errorcompounder.NewSafe()
			swept := len(body)

			logger := d.logger.WithField("action", "objects_ttl_deletion")
			logger.WithFields(logrus.Fields{
				"collections":       colNames,
				"collections_count": len(colNames),
			}).Debug("incoming ttl deletion on remote node started")
			defer func() {
				took := time.Since(started)

				// add fields c_{collection_name}=>{count_deleted} and total_deleted=>{total_deleted}
				fields, total := objsDeletedCounters.ToLogFields(16)
				fields["took"] = took.String()
				logger = logger.WithFields(fields)

				metrics.DecObjectsTtlRunning()
				metrics.ObserveObjectsTtlDuration(took)
				metrics.AddObjectsTtlObjectsDeleted(float64(total))

				// ec holds what a callee returned; the context says whether an
				// abort stopped it.
				cause := context.Cause(ttlCtx)
				if cause != nil {
					logger = logger.WithFields(logrus.Fields{
						"stopped_early":     true,
						"collections_swept": swept,
					})
				}

				if !ec.Empty() {
					metrics.IncObjectsTtlFailureCount()
					logger.Errorf("incoming ttl deletion on remote node failed: %v", err)
					return
				}
				if cause != nil {
					logger.Warnf("incoming ttl deletion on remote node stopped early: %v", cause)
					return
				}
				logger.Debug("incoming ttl deletion on remote node finished")
			}()

			eg := enterrors.NewErrorGroupWrapper(d.logger)
			eg.SetLimit(concurrency.TimesFloatGOMAXPROCS(d.config.ObjectsTTLConcurrencyFactor.Get()))

			for pos, classPayload := range body {
				if context.Cause(ttlCtx) != nil {
					swept = pos
					break
				}
				className := classPayload.Class
				// captured by value: a delete goroutine indexing objsDeletedCounters
				// by name would race the next iteration's map write, which is fatal.
				counter := &atomic.Int32{}
				objsDeletedCounters[className] = counter
				countDeleted := func(count int32) { counter.Add(count) }

				idx, err := d.remoteIndex.IndexForIncomingWrite(ttlCtx, className, classPayload.ClassVersion)
				if err != nil {
					// the schema wait reports its own deadline whether it timed out
					// or was cancelled, so ask the context which one happened
					if context.Cause(ttlCtx) != nil {
						swept = pos
						break
					}
					ec.AddGroups(fmt.Errorf("get index: %w", err), className)
					continue
				}

				idx.IncomingDeleteObjectsExpired(ttlCtx, eg, ec, classPayload.Prop, time.UnixMilli(classPayload.TtlMilli),
					time.UnixMilli(classPayload.DelMilli), countDeleted, classPayload.ClassVersion)
			}

			eg.WaitAndCollect(ec)

			err = ec.ToErrorLimited(objectttl.MaxReportedErrors)
		}, d.logger)

		w.WriteHeader(http.StatusAccepted)
	})
}

func (d *ObjectTTL) incomingAbort() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		response := objectttl.ObjectsExpiredAbortResponse{
			Aborted: d.localStatus.Abort(),
		}

		d.logger.WithFields(logrus.Fields{
			"action":  "objects_ttl_deletion",
			"aborted": response.Aborted,
		}).Info("incoming abort ttl deletion on remote node")

		response.SetContentTypeHeader(w)
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "/object ttl marshal response: "+err.Error(),
				http.StatusInternalServerError)
		}
	})
}
