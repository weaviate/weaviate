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

package distributedtask

// compactInPlace keeps an ended task's switched groups and replaces its units
// with a summary, so a kept task costs memory per switched tenant, not per unit.
func (t *Task) compactInPlace() {
	t.Summary = t.summarizeUnits()
	t.Units = nil
	t.groups, t.groupCount, t.decidedWithUnits = nil, 0, 0
	t.unitCounts = nil
	t.PreparedGroups = nil
	t.NodePreparationAcks = nil
	t.PostCompletionAcks = nil
}
