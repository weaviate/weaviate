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

// Package reindex renders the routes a gate refusal points an operator at.
package reindex

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

const (
	cancelPropertySegment  = "<property>"
	cancelIndexTypeSegment = "<indexType>"
)

// From the generated enum, so a refusal cannot name a spelling the API rejects.
var indexTypeSegments = strings.Join([]string{
	models.IndexStatusTypeFilterable,
	models.IndexStatusTypeSearchable,
	models.IndexStatusTypeRangeFilters,
}, ", ")

func IndexesRoute(collection string) string {
	return fmt.Sprintf("GET /v1/schema/%s/indexes", collection)
}

func CancelRoute(collection, property, indexType string) string {
	return fmt.Sprintf("POST /v1/schema/%s/properties/%s/index/%s/cancel",
		collection, property, indexType)
}

// The API accepts a cancel only while the task is STARTED.
func MigrationRemedy(collection string) string {
	return fmt.Sprintf(
		`%s reports when it is done, by moving the index off status="pending" and status="indexing". `+
			`To stop it instead, that same call names the property and the index type (one of %s) `+
			`still migrating, and %s cancels that one. A cancel is accepted only while the task is `+
			`STARTED: it answers 409 in a coordination phase, and for a status this node cannot `+
			`classify, which has to terminate on the nodes that do recognize it`,
		IndexesRoute(collection),
		indexTypeSegments,
		CancelRoute(collection, cancelPropertySegment, cancelIndexTypeSegment),
	)
}
