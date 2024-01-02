//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
)

// BatchObject is a helper type that groups all the info about one object in a
// batch that belongs together, i.e. uuid, object body and error state.
//
// Consumers of an Object (i.e. database connector) should always check
// whether an error is already present by the time they receive a batch object.
// Errors can be introduced at all levels, e.g. validation.
//
// However, error'd objects are not removed to make sure that the list in
// Objects matches the order and content of the incoming batch request
type BatchObject struct {
	OriginalIndex int
	Err           error
	Object        *models.Object
	UUID          strfmt.UUID
	Vector        []float32
}

// BatchObjects groups many Object items together. The order matches the
// order from the original request. It can be turned into the expected response
// type using the .Response() method
type BatchObjects []BatchObject

// BatchReference is a helper type that groups all the info about one references in a
// batch that belongs together, i.e. from, to, original index and error state
//
// Consumers of an Object (i.e. database connector) should always check
// whether an error is already present by the time they receive a batch object.
// Errors can be introduced at all levels, e.g. validation.
//
// However, error'd objects are not removed to make sure that the list in
// Objects matches the order and content of the incoming batch request
type BatchReference struct {
	OriginalIndex int                 `json:"originalIndex"`
	Err           error               `json:"err"`
	From          *crossref.RefSource `json:"from"`
	To            *crossref.Ref       `json:"to"`
	Tenant        string              `json:"tenant"`
}

// BatchReferences groups many Reference items together. The order matches the
// order from the original request. It can be turned into the expected response
// type using the .Response() method
type BatchReferences []BatchReference

type BatchSimpleObject struct {
	UUID strfmt.UUID
	Err  error
}

type BatchSimpleObjects []BatchSimpleObject

type BatchDeleteParams struct {
	ClassName schema.ClassName     `json:"className"`
	Filters   *filters.LocalFilter `json:"filters"`
	DryRun    bool
	Output    string
}

type BatchDeleteResult struct {
	Matches int64
	Limit   int64
	DryRun  bool
	Objects BatchSimpleObjects
}

type BatchDeleteResponse struct {
	Match  *models.BatchDeleteMatch
	DryRun bool
	Output string
	Params BatchDeleteParams
	Result BatchDeleteResult
}
