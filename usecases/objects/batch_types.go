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

package objects

import (
	"encoding/json"
	"errors"
	"time"

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
	// UpdateTime is stamped by the coordinator so all replicas apply the
	// same LastUpdateTime. Zero means unset; replicas fall back to time.Now().
	UpdateTime int64 `json:"updateTime,omitempty"`
}

// BatchReferences groups many Reference items together. The order matches the
// order from the original request. It can be turned into the expected response
// type using the .Response() method
type BatchReferences []BatchReference

type BatchSimpleObject struct {
	UUID strfmt.UUID
	Err  error
}

// MarshalJSON carries Err as text in ErrMsg, and keeps writing the legacy Err
// key beside it. An error has no exported fields, so stock JSON writes it as
// {}, which a decoder without this codec fails on loudly. Dropping the key
// would instead let that decoder read a failed delete as a nil Err.
func (b BatchSimpleObject) MarshalJSON() ([]byte, error) {
	var (
		msg    string
		legacy json.RawMessage
	)
	if b.Err != nil {
		msg = b.Err.Error()
		legacy = json.RawMessage("{}")
	}
	return json.Marshal(struct {
		UUID   strfmt.UUID     `json:"UUID"`
		Err    json.RawMessage `json:"Err,omitempty"`
		ErrMsg string          `json:"ErrMsg,omitempty"`
	}{b.UUID, legacy, msg})
}

// UnmarshalJSON rebuilds Err from ErrMsg. A peer without the codec writes a
// non-nil error as "Err":{}, carrying no text, so a stand-in takes its place.
// A slot that failed is never read as one that succeeded.
func (b *BatchSimpleObject) UnmarshalJSON(in []byte) error {
	var row struct {
		UUID   strfmt.UUID     `json:"UUID"`
		Err    json.RawMessage `json:"Err"`
		ErrMsg string          `json:"ErrMsg"`
	}
	if err := json.Unmarshal(in, &row); err != nil {
		return err
	}
	b.UUID = row.UUID
	switch {
	case row.ErrMsg != "":
		b.Err = errors.New(row.ErrMsg)
	case len(row.Err) > 0 && string(row.Err) != "null":
		b.Err = errors.New("remote shard reported an error it could not transmit")
	default:
		b.Err = nil
	}
	return nil
}

type BatchSimpleObjects []BatchSimpleObject

type BatchDeleteParams struct {
	ClassName    schema.ClassName     `json:"className"`
	Filters      *filters.LocalFilter `json:"filters"`
	DeletionTime time.Time
	DryRun       bool
	Output       string
}

type BatchDeleteResult struct {
	// Matches is how many objects the filter matched, counted no further than one above
	// Limit. At or below Limit it is exact and every match was handled by this call;
	// above Limit it means more objects match than one call deletes, so the caller
	// repeats the request.
	Matches int64
	// Limit is the QUERY_MAXIMUM_RESULTS that bounds the count above. Zero or less means
	// two things: the count is exact and unbounded, and nothing was deleted.
	Limit        int64
	DeletionTime time.Time
	DryRun       bool
	Objects      BatchSimpleObjects
}

type BatchDeleteResponse struct {
	Match        *models.BatchDeleteMatch
	DeletionTime time.Time
	DryRun       bool
	Output       string
	Params       BatchDeleteParams
	Result       BatchDeleteResult
}
