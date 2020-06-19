//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

type bulkControlObject struct {
	Index  *bulkID `json:"index,omitempty"`
	Update *bulkID `json:"update,omitempty"`
}

type bulkID struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

// warning: only use if all bulk requests are of type index, as this particular
// struct might not catch errors of other operations
type bulkIndexResponse struct {
	Errors bool       `json:"errors"`
	Items  []bulkItem `json:"items"`
}

type bulkItem struct {
	Index  *bulkIndexItem `json:"index"`
	Update *bulkIndexItem `json:"update"`
}

type bulkIndexItem struct {
	Error interface{}
}

func (r *Repo) BatchPutActions(ctx context.Context, batch kinds.BatchActions) (kinds.BatchActions, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	r.encodeBatchActions(enc, batch)

	if buf.Len() == 0 {
		// we cannot send an empty request to ES, as it will error. However, if the
		// user had a validation error in every single batch item, we'd end up with
		// an empty body. In this case, simply return the original results
		return batch, nil
	}

	req := esapi.BulkRequest{
		Body: &buf,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("batch put action request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return nil, fmt.Errorf("batch put action request: %v", err)
	}

	return mergeBatchActionsWithErrors(batch, res)
}

func (r Repo) encodeBatchActions(enc *json.Encoder, batch kinds.BatchActions) error {
	for _, single := range batch {
		if single.Err != nil {
			// ignore concepts that already have an error
			continue
		}

		a := single.Action
		var vectorWeights map[string]string
		if a.VectorWeights != nil {
			vectorWeights = a.VectorWeights.(map[string]string)
		}
		bucket := r.objectBucket(kind.Action, a.ID.String(), a.Class, a.Schema,
			a.Meta, vectorWeights, single.Vector, a.CreationTimeUnix, a.LastUpdateTimeUnix)

		index := classIndexFromClassName(kind.Action, a.Class)
		control := r.bulkIndexControlObject(index, a.ID.String())

		err := enc.Encode(control)
		if err != nil {
			return err
		}

		err = enc.Encode(bucket)
		if err != nil {
			return err
		}
	}

	return nil
}

func mergeBatchActionsWithErrors(batch kinds.BatchActions, res *esapi.Response) (kinds.BatchActions, error) {
	var parsed bulkIndexResponse
	err := json.NewDecoder(res.Body).Decode(&parsed)
	if err != nil {
		return nil, err
	}

	if !parsed.Errors {
		// no need to check for error positions if there are none
		return batch, nil
	}

	bulkIndex := 0
	for i, action := range batch {
		if action.Err != nil {
			// already had a validation error, was therefore never sent off to es
			continue
		}

		err := parsed.Items[bulkIndex].Index.Error
		if err != nil {
			batch[i].Err = fmt.Errorf("%v", err)
		}

		bulkIndex++
	}

	return batch, nil
}

func (r *Repo) BatchPutThings(ctx context.Context, batch kinds.BatchThings) (kinds.BatchThings, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	r.encodeBatchThings(enc, batch)

	if buf.Len() == 0 {
		// we cannot send an empty request to ES, as it will error. However, if the
		// user had a validation error in every single batch item, we'd end up with
		// an empty body. In this case, simply return the original results
		return batch, nil
	}

	req := esapi.BulkRequest{
		Body: &buf,
	}
	res, err := req.Do(ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("batch put thing request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return nil, fmt.Errorf("batch put thing request: %v", err)
	}

	return mergeBatchThingsWithErrors(batch, res)
}

func (r Repo) encodeBatchThings(enc *json.Encoder, batch kinds.BatchThings) error {
	for _, single := range batch {
		if single.Err != nil {
			// ignore concepts that already have an error
			continue
		}

		t := single.Thing
		var vectorWeights map[string]string
		if t.VectorWeights != nil {
			vectorWeights = t.VectorWeights.(map[string]string)
		}
		bucket := r.objectBucket(kind.Thing, t.ID.String(), t.Class, t.Schema,
			t.Meta, vectorWeights, single.Vector, t.CreationTimeUnix, t.LastUpdateTimeUnix)

		index := classIndexFromClassName(kind.Thing, t.Class)
		control := r.bulkIndexControlObject(index, t.ID.String())

		err := enc.Encode(control)
		if err != nil {
			return err
		}

		err = enc.Encode(bucket)
		if err != nil {
			return err
		}
	}

	return nil
}

func mergeBatchThingsWithErrors(batch kinds.BatchThings, res *esapi.Response) (kinds.BatchThings, error) {
	var parsed bulkIndexResponse
	err := json.NewDecoder(res.Body).Decode(&parsed)
	if err != nil {
		return nil, err
	}

	if !parsed.Errors {
		// no need to check for error positions if there are none
		return batch, nil
	}

	bulkIndex := 0
	for i, thing := range batch {
		if thing.Err != nil {
			// already had a validation error, was therefore never sent off to es
			continue
		}

		err := parsed.Items[bulkIndex].Index.Error
		if err != nil {
			batch[i].Err = fmt.Errorf("%v", err)
		}

		bulkIndex++
	}

	return batch, nil
}

func (r *Repo) bulkIndexControlObject(index, id string) bulkControlObject {
	return bulkControlObject{
		Index: &bulkID{
			Index: index,
			ID:    id,
		},
	}
}
