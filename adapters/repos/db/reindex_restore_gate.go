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

package db

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/reindex"
)

type decodedReindexTask struct {
	task *distributedtask.Task
	DecodedReindexTask
}

func decodeReindexTasksByID(tasks []*distributedtask.Task) []decodedReindexTask {
	out := make([]decodedReindexTask, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, decodedReindexTask{
			task:               task,
			DecodedReindexTask: DecodeReindexTaskPayload(task.Payload),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].task.ID < out[j].task.ID })
	return out
}

type ReindexActivity struct {
	Collection string
	TaskID     string
	// Unreadable is not the same answer as a migration being in flight.
	Unreadable bool
}

// An empty list means every collection: a restore naming none restores all.
type AnyReindexActivityLookup func(collections []string) (ReindexActivity, bool)

type AnyReindexActivityLookupBuilder func(ctx context.Context) AnyReindexActivityLookup

func NewAnyReindexActivityLookup(tasks []*distributedtask.Task) AnyReindexActivityLookup {
	var clusterWide []ReindexActivity
	byCollection := make(map[string]ReindexActivity, len(tasks))
	ordered := make([]ReindexActivity, 0, len(tasks))
	for _, decoded := range decodeReindexTasksByID(tasks) {
		if !IsLiveReindexTaskStatus(decoded.task.Status) {
			continue
		}
		activity := ReindexActivity{Collection: decoded.Collection, TaskID: decoded.task.ID}
		ordered = append(ordered, activity)
		if decoded.Scope == ReindexPayloadScopeCluster {
			clusterWide = append(clusterWide, activity)
			continue
		}
		key := strings.ToLower(decoded.Collection)
		if _, seen := byCollection[key]; !seen {
			byCollection[key] = activity
		}
	}
	return func(collections []string) (ReindexActivity, bool) {
		if len(clusterWide) > 0 {
			return clusterWide[0], true
		}
		if len(collections) == 0 {
			if len(ordered) > 0 {
				return ordered[0], true
			}
			return ReindexActivity{}, false
		}
		for _, collection := range collections {
			if activity, ok := byCollection[strings.ToLower(collection)]; ok {
				return activity, true
			}
		}
		return ReindexActivity{}, false
	}
}

func (db *DB) SetAnyReindexActivityLookup(builder AnyReindexActivityLookupBuilder) {
	db.reindexAuditMu.Lock()
	defer db.reindexAuditMu.Unlock()
	db.anyReindexActivityLookupBuilder = builder
}

func (db *DB) RefuseIfAnyReindexInFlight(ctx context.Context, collections []string) error {
	if db.config.RuntimeReindexDisabled {
		return nil
	}
	if hold := db.ReindexHoldFor(collections...); hold != ReindexHoldNone {
		db.warnRestoreRefusal(collections, hold.String(), "")
		return restoreHoldRefusal(collections, hold)
	}
	db.reindexAuditMu.RLock()
	builder := db.anyReindexActivityLookupBuilder
	db.reindexAuditMu.RUnlock()
	if builder == nil {
		db.warnUnwiredGate(&restoreGateWarnBudget, "restore_reindex_gate", "restore",
			"Check the SetAnyReindexActivityLookup wiring in configure_api.go.")
		return nil
	}
	activity, blocked := builder(ctx)(collections)
	if !blocked {
		return nil
	}
	if activity.Unreadable {
		db.warnRestoreRefusal(collections, reindexReasonTaskListUnreadable, "")
		return restoreUnreadableRefusal()
	}
	db.warnRestoreRefusal(collections, reindexReasonLiveTask, activity.TaskID)
	return restoreLiveTaskRefusal(collections, activity)
}

func (db *DB) warnRestoreRefusal(collections []string, reason, taskID string) {
	db.warnRefusal("restore_reindex_gate", reason,
		"restore-reindex gate: refusing this restore; the published refusal names a collection only",
		logrus.Fields{
			"task_id":                 taskID,
			"requested_class_count":   len(collections),
			"requested_classes":       cappedSample(collections),
			"covers_every_collection": len(collections) == 0,
		})
}

func restoreRefusal(detail string) error {
	return fmt.Errorf("restore blocked: %w: %s", entitiesbackup.ErrReindexInFlight, detail)
}

func restoreLiveTaskRefusal(collections []string, activity ReindexActivity) error {
	subject, named := restoreSubject(collections, activity.Collection)
	// The remedy renders the collection into URL paths.
	remedy := ""
	if named {
		remedy = " " + reindex.MigrationRemedy(activity.Collection)
	}
	return restoreRefusal(fmt.Sprintf(
		"%s has an active runtime-reindex task; retry after the migration finishes.%s",
		subject, remedy))
}

func restoreUnreadableRefusal() error {
	return fmt.Errorf("restore blocked: the cluster task list could not be read, so whether a " +
		"runtime-reindex is in flight cannot be determined; retry once the cluster is reachable")
}

func restoreHoldRefusal(collections []string, hold ReindexHold) error {
	subject, _ := restoreSubject(collections, "")
	return restoreRefusal(reindexHoldDetail(subject, hold))
}

// Naming a collection the caller did not ask about discloses the cluster.
func restoreSubject(collections []string, blocking string) (string, bool) {
	if blocking != "" {
		for _, collection := range collections {
			if strings.EqualFold(collection, blocking) {
				return fmt.Sprintf("collection %q", collection), true
			}
		}
	}
	if len(collections) == 1 {
		return fmt.Sprintf("collection %q", collections[0]), false
	}
	return "a collection this restore covers", false
}
