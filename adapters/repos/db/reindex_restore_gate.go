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

type ReindexActivity struct {
	Collection string
	TaskID     string
	// Unreadable is not the same answer as a migration being in flight.
	Unreadable bool
}

// An empty list means every collection. The only caller that hands one is
// the cluster-wide probe, for a restore whose class list is not known yet; a
// participant asks about the classes it was given and skips the gate when it
// was given none.
type AnyReindexActivityLookup func(collections []string) (ReindexActivity, bool)

type AnyReindexActivityLookupBuilder func(ctx context.Context) AnyReindexActivityLookup

func NewAnyReindexActivityLookup(tasks []*distributedtask.Task) AnyReindexActivityLookup {
	// Terminal tasks are filtered before anything reads their payload: a
	// finished migration blocks nothing, and its payload holds one entry per
	// tenant.
	live := make([]*distributedtask.Task, 0, len(tasks))
	for _, task := range tasks {
		if IsLiveReindexTaskStatus(task.Status) {
			live = append(live, task)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].ID < live[j].ID })

	var clusterWide []ReindexActivity
	byCollection := make(map[string]ReindexActivity, len(live))
	ordered := make([]ReindexActivity, 0, len(live))
	for _, task := range live {
		collection, named := ExtractReindexTaskCollection(task.Payload)
		activity := ReindexActivity{Collection: collection, TaskID: task.ID}
		ordered = append(ordered, activity)
		if !named {
			clusterWide = append(clusterWide, activity)
			continue
		}
		key := strings.ToLower(collection)
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
	// Read before the cluster is asked, answered after it: a live task can be
	// ended by the operator and a hold cannot, so the arm that offers a remedy
	// wins when both apply.
	hold := db.ReindexHoldFor(collections...)
	var activity ReindexActivity
	var blocked bool
	db.reindexAuditMu.RLock()
	builder := db.anyReindexActivityLookupBuilder
	db.reindexAuditMu.RUnlock()
	if builder == nil {
		db.warnUnwiredGate(&restoreGateWarnBudget, "restore_reindex_gate", "restore",
			"Check the SetAnyReindexActivityLookup wiring in configure_api.go.")
	} else if lookup := builder(ctx); lookup != nil {
		activity, blocked = lookup(collections)
	}
	switch {
	case blocked && !activity.Unreadable:
		db.warnRestoreRefusal(collections, reindexReasonLiveTask, activity.TaskID)
		db.gateMetrics().Refused(reindex.GateRestore, reindex.VerdictLiveTask)
		return restoreLiveTaskRefusal(collections, activity)
	case hold != ReindexHoldNone:
		db.warnRestoreRefusal(collections, hold.String(), "")
		db.gateMetrics().Refused(reindex.GateRestore, reindexHoldVerdict(hold))
		return restoreHoldRefusal(collections, hold)
	case blocked:
		// An unreadable list observed nothing, so it ranks below a hold.
		db.warnRestoreRefusal(collections, reindexReasonTaskListUnreadable, "")
		db.gateMetrics().Refused(reindex.GateRestore, reindex.VerdictTaskListUnreadable)
		return restoreUnreadableRefusal()
	}
	return nil
}

func (db *DB) warnRestoreRefusal(collections []string, reason, taskID string) {
	db.warnRefusal("restore_reindex_gate", reason,
		"restore-reindex gate: refusing this restore; the published refusal names a collection only",
		logrus.Fields{
			"task_id":               taskID,
			"requested_class_count": len(collections),
			"requested_classes":     cappedSample(collections),
		})
}

func restoreRefusal(detail string) error {
	return fmt.Errorf("restore blocked: %w: %s", entitiesbackup.ErrReindexInFlight, detail)
}

func restoreLiveTaskRefusal(collections []string, activity ReindexActivity) error {
	subject, named := restoreSubject(collections, activity.Collection)
	if !named {
		// The subject is not the collection the task is on: either nothing
		// attributes the task to one, or attributing it would name a
		// collection the caller did not ask about.
		return restoreRefusal(fmt.Sprintf(
			"a runtime-reindex is in flight, and this refusal does not name the collection "+
				"it is on, so %s cannot be restored; retry after the migration finishes. %s",
			subject, reindex.ClusterMigrationRemedy()))
	}
	return restoreRefusal(fmt.Sprintf(
		"%s has an active runtime-reindex task; retry after the migration finishes. %s",
		subject, reindex.MigrationRemedy(activity.Collection)))
}

func restoreUnreadableRefusal() error {
	return fmt.Errorf("%w: the cluster task list could not be read; retry once the cluster is reachable",
		entitiesbackup.ErrReindexActivityUndetermined)
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
