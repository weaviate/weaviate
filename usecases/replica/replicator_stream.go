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

package replica

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/usecases/objects"
)

type (
	// replicatorStream represents an incoming stream of responses
	// to replication requests sent to replicas
	replicatorStream struct{}
)

// readErrors reads errors from incoming responses.
// It returns as soon as the specified consistency level l has been reached
func (r replicatorStream) readErrors(batchSize int,
	level int,
	ch <-chan _Result[SimpleResponse],
) []error {
	urs := make([]SimpleResponse, 0, level)
	var firstError error
	for x := range ch {
		if x.Err != nil {
			urs = append(urs, x.Value)
			if len(x.Value.Errors) == 0 && firstError == nil {
				firstError = x.Err
			}
		} else {
			level--
			if level == 0 { // consistency level reached
				return make([]error, batchSize)
			}
		}
	}
	if level > 0 && firstError == nil {
		firstError = fmt.Errorf("broadcast: %w", errReplicas)
	}
	return r.flattenErrors(batchSize, urs, firstError)
}

// readDeletions reads deletion results from incoming responses.
// It returns as soon as the specified consistency level l has been reached
func (r replicatorStream) readDeletions(batchSize int,
	level int,
	ch <-chan _Result[DeleteBatchResponse],
) []objects.BatchSimpleObject {
	rs := make([]DeleteBatchResponse, 0, level)
	urs := make([]DeleteBatchResponse, 0, level)
	var firstError error
	for x := range ch {
		if x.Err != nil {
			urs = append(urs, x.Value)
			if len(x.Value.Batch) == 0 && firstError == nil {
				firstError = x.Err
			}
		} else {
			level--
			rs = append(rs, x.Value)
			if level == 0 { // consistency level reached
				return r.flattenDeletions(batchSize, rs, nil)
			}
		}
	}
	if level > 0 && firstError == nil {
		firstError = fmt.Errorf("broadcast: %w", errReplicas)
	}
	urs = append(urs, rs...)
	return r.flattenDeletions(batchSize, urs, firstError)
}

// flattenErrors extracts errors from responses

func (replicatorStream) flattenErrors(batchSize int,
	rs []SimpleResponse,
	defaultErr error,
) []error {
	errs := make([]error, batchSize)
	n := 0
	for _, resp := range rs {
		if len(resp.Errors) != batchSize {
			continue
		}
		n++
		for i, err := range resp.Errors {
			if !err.Empty() && errs[i] == nil {
				errs[i] = err.Clone()
			}
		}
	}
	if n == 0 || n != len(rs) {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = defaultErr
			}
		}
	}
	return errs
}

// flattenDeletions extracts deletion results from responses
func (replicatorStream) flattenDeletions(batchSize int,
	rs []DeleteBatchResponse,
	defaultErr error,
) []objects.BatchSimpleObject {
	ret := make([]objects.BatchSimpleObject, batchSize)
	n := 0
	for _, resp := range rs {
		if len(resp.Batch) != batchSize {
			continue
		}
		n++
		for i, x := range resp.Batch {
			if !x.Error.Empty() && ret[i].Err == nil {
				ret[i].Err = x.Error.Clone()
			}
			if ret[i].UUID == "" && x.UUID != "" {
				ret[i].UUID = strfmt.UUID(x.UUID)
			}
		}
	}
	if n == 0 || n != len(rs) {
		for i := range ret {
			if ret[i].Err == nil {
				ret[i].Err = defaultErr
			}
		}
	}
	return ret
}

func firstError(es []error) error {
	for _, e := range es {
		if e != nil {
			return e
		}
	}
	return nil
}

func firstBatchError(xs []objects.BatchSimpleObject) error {
	for _, x := range xs {
		if x.Err != nil {
			return x.Err
		}
	}
	return nil
}
