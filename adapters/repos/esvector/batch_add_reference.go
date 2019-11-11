package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func (r *Repo) AddBatchReferences(ctx context.Context, list kinds.BatchReferences) (kinds.BatchReferences, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	r.encodeBatchReferences(enc, list)

	if buf.Len() == 0 {
		// we cannot send an empty request to ES, as it will error. However, if the
		// user had a validation error in every single batch item, we'd end up with
		// an empty body. In this case, simply return the original results
		return list, nil
	}

	req := esapi.BulkRequest{
		Body: &buf,
	}
	res, err := req.Do(ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("batch add reference request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return nil, fmt.Errorf("batch add reference request: %v", err)
	}

	return mergeBatchReferencesWithErrors(list, res)
}

func (r Repo) encodeBatchReferences(enc *json.Encoder, batch kinds.BatchReferences) error {
	for _, single := range batch {
		if single.Err != nil {
			// ignore concepts that already have an error
			continue
		}

		bucket := r.upsertReferenceBucket(single.From.Property.String(), single.To.SingleRef())
		index := classIndexFromClassName(single.From.Kind, single.From.Class.String())
		control := r.bulkUpdateControlObject(index, single.From.TargetID.String())

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

func (r *Repo) bulkUpdateControlObject(index, id string) bulkControlObject {
	return bulkControlObject{
		Update: &bulkID{
			Index: index,
			ID:    id,
		},
	}
}

func mergeBatchReferencesWithErrors(batch kinds.BatchReferences, res *esapi.Response) (kinds.BatchReferences, error) {
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
	for i, obj := range batch {
		if obj.Err != nil {
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
