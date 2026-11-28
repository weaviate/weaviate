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

package cluster

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// refuseUnfinishedLegacyRuns stops the process on its first DB load when a reindex run without
// a generation is unfinished. This version serves only generations, so its tenants serve neither side.
func refuseUnfinishedLegacyRuns(log *logrus.Logger, tasks []*distributedtask.Task) {
	for _, task := range tasks {
		if msg := unfinishedLegacyRun(task, tasks); msg != "" {
			log.WithField("action", "reindex_upgrade_check").WithField("task_id", task.ID).Fatal(msg)
		}
	}
}

func unfinishedLegacyRun(task *distributedtask.Task, tasks []*distributedtask.Task) string {
	h, ok := db.ReindexHeaderOf(task)
	if !ok || h.SwitchedByView() {
		return ""
	}
	if task.Status.IsTerminal() && (!db.ReindexLeftPartial(task) || legacyRunCovered(task, h, tasks)) {
		return ""
	}
	switched := len(task.SwapDecidedGroups)
	way := "resubmit the same migration and let it finish"
	if switched == 0 {
		way += ", or cancel it there"
	}
	return fmt.Sprintf("cannot start: reindex task %q on property %s of collection %q was left %s by an older version, "+
		"with %d tenants switched. Roll back to v1.39.17, %s, then upgrade again",
		task.ID, strings.Join(h.Properties, ","), h.Collection(), task.Status, switched, way)
}

// A newer finished run covers a partial one when it built all its index types for its tenants.
func legacyRunCovered(partial *distributedtask.Task, h db.ReindexTaskHeader, tasks []*distributedtask.Task) bool {
	types, _ := db.ReindexIndexTypes(h.MigrationType)
	return slices.ContainsFunc(tasks, func(t *distributedtask.Task) bool {
		o, ok := db.ReindexHeaderOf(t)
		if !ok || t.Status != distributedtask.TaskStatusFinished || t.Version <= partial.Version ||
			!strings.EqualFold(o.Collection(), h.Collection()) {
			return false
		}
		oTypes, _ := db.ReindexIndexTypes(o.MigrationType)
		return containsAll(o.Properties, h.Properties) && containsAll(oTypes, types) &&
			(len(o.Tenants) == 0 || containsAll(o.Tenants, slices.Collect(maps.Keys(partial.SwapDecidedGroups))))
	})
}

func containsAll(have, want []string) bool {
	return !slices.ContainsFunc(want, func(s string) bool { return !slices.Contains(have, s) })
}
