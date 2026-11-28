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
	"fmt"

	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

// refuseIfReindexRunsHere reads local tasks: a shard changes its folders only for those.
func (s *Shard) refuseIfReindexRunsHere() error {
	src := s.index.db.reindexTaskStateSource()
	if src == nil || s.index.db.config.RuntimeReindexDisabled {
		return nil
	}
	collection := s.index.Config.ClassName.String()
	for _, st := range src.ShardTaskStates(ReindexNamespace, collection, s.name, s.index.db.localNodeName) {
		if st.HasUnit && !st.Status.IsTerminal() {
			return reindexInFlightError(fmt.Sprintf("shard %q (collection %q)", s.name, collection))
		}
	}
	return nil
}

// reindexInFlightError states the cancel remedy with its condition, since the check does not branch on status.
func reindexInFlightError(scope string) error {
	return fmt.Errorf(
		"%w: %s has an active runtime-reindex task in DTM; retry once that task reaches a terminal state, which GET /v1/schema/<class>/indexes reports by moving the index off status=\"pending\" and status=\"indexing\". A cancel via POST /v1/schema/<class>/properties/<prop>/index/<indexType>/cancel is accepted while the task is STARTED or PREPARING: it is refused with 409 once the task is SWAPPING, and for a status this node cannot classify, which has to terminate on the nodes that do recognize it",
		entitiesbackup.ErrBackupBlockedByInFlightReindex, scope,
	)
}

// NoSearchableIndexError formats the 400 for a searchable-index operation
// (rebuild/algorithm change) on a property with no searchable index.
// Centralised for identical phrasing across call sites; not used for the
// inverse case (already has one), which carries the opposite meaning.
func NoSearchableIndexError(propertyName string) string {
	return fmt.Sprintf(
		"property %q has no searchable index; PUT /v1/schema/{className}/properties/%s/index/searchable with a tokenization to add one first",
		propertyName, propertyName,
	)
}
