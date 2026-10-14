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
	"unicode/utf8"

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

// BatchSimpleObject is one slot of a batch delete's answer. A nil Err means no delete of UUID failed,
// as for a dry run or an already-absent object; a failed WAL write sets Err on deleted objects too.
// Err crosses the cluster wire through its own codec, since encoding/json cannot decode an error.
type BatchSimpleObject struct {
	UUID strfmt.UUID
	Err  error
}

// MarshalJSON carries Err as text in ErrMsg, capped, and keeps writing the Err key a decoder
// without this codec expects: the {} it fails on loudly, where dropping the key would let it read
// a failed delete as a nil Err.
func (b BatchSimpleObject) MarshalJSON() ([]byte, error) {
	var (
		msg       string
		errObject json.RawMessage
	)
	if b.Err != nil {
		msg = truncateBatchErrMsg(b.Err.Error())
		errObject = json.RawMessage("{}")
	}
	return json.Marshal(struct {
		UUID   strfmt.UUID     `json:"UUID"`
		Err    json.RawMessage `json:"Err,omitempty"`
		ErrMsg string          `json:"ErrMsg,omitempty"`
	}{b.UUID, errObject, msg})
}

// maxBatchErrMsg caps a slot's error text on the wire. A flush failure writes one error into every
// slot of the batch, so an uncapped message is multiplied by the batch size on a response that is
// already reporting a failure.
const maxBatchErrMsg = 256

// truncateBatchErrMsg cuts msg to maxBatchErrMsg on a rune boundary, so a capped message stays
// valid UTF-8 rather than decoding to U+FFFD on the far side.
func truncateBatchErrMsg(msg string) string {
	if len(msg) <= maxBatchErrMsg {
		return msg
	}
	cut := maxBatchErrMsg
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + "…"
}

// ErrRemoteDeleteUnreadable stands in for a slot a peer reported as failed in a shape this node
// cannot read the message out of, which is what a peer older than this codec writes. Match on it to
// tell a failure whose text was lost to version skew during a rolling upgrade; the delete failed
// either way.
var ErrRemoteDeleteUnreadable = errors.New("remote shard reported a failed delete without a readable message")

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
		b.Err = ErrRemoteDeleteUnreadable
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
