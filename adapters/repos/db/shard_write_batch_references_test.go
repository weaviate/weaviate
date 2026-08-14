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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestMergeDocFromBatchReference_UsesCoordinatorUpdateTime(t *testing.T) {
	const stamped int64 = 1_700_000_000_000

	ref := objects.BatchReference{
		From: &crossref.RefSource{
			Class:    schema.ClassName("Authors"),
			TargetID: strfmt.UUID("11111111-1111-1111-1111-111111111111"),
			Property: schema.PropertyName("wroteBooks"),
		},
		To:         &crossref.Ref{Class: "Books"},
		UpdateTime: stamped,
	}

	got := mergeDocFromBatchReference(ref)

	assert.Equal(t, stamped, got.UpdateTime,
		"replica must apply the coordinator-assigned UpdateTime")
}

func TestMergeDocFromBatchReference_FallsBackWhenUpdateTimeUnset(t *testing.T) {
	ref := objects.BatchReference{
		From: &crossref.RefSource{
			Class:    schema.ClassName("Authors"),
			TargetID: strfmt.UUID("22222222-2222-2222-2222-222222222222"),
			Property: schema.PropertyName("wroteBooks"),
		},
		To: &crossref.Ref{Class: "Books"},
	}

	before := time.Now().UnixMilli()
	got := mergeDocFromBatchReference(ref)
	after := time.Now().UnixMilli()

	assert.GreaterOrEqual(t, got.UpdateTime, before)
	assert.LessOrEqual(t, got.UpdateTime, after)
}
