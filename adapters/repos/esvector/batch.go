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
	Index bulkIndex `json:"index"`
}

type bulkIndex struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

func (r *Repo) BatchPutAction(ctx context.Context, batch kinds.BatchActions) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	r.encodeBatchActions(enc, batch)
	req := esapi.BulkRequest{
		Body: &buf,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("batch put action request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return fmt.Errorf("batch put action request: %v", err)
	}

	return nil
}

func (r Repo) encodeBatchActions(enc *json.Encoder, batch kinds.BatchActions) error {
	for _, single := range batch {
		if single.Err != nil {
			// ignore concepts that already have an error
			continue
		}

		a := single.Action
		bucket := r.objectBucket(kind.Action, a.ID.String(), a.Class, a.Schema,
			[]float32{}, a.CreationTimeUnix, a.LastUpdateTimeUnix)

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

func (r *Repo) BatchPutThing(ctx context.Context, batch kinds.BatchThings) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	r.encodeBatchThings(enc, batch)
	req := esapi.BulkRequest{
		Body: &buf,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("batch put thing request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return fmt.Errorf("batch put thing request: %v", err)
	}

	return nil
}

func (r Repo) encodeBatchThings(enc *json.Encoder, batch kinds.BatchThings) error {
	for _, single := range batch {
		if single.Err != nil {
			// ignore concepts that already have an error
			continue
		}

		t := single.Thing
		bucket := r.objectBucket(kind.Thing, t.ID.String(), t.Class, t.Schema,
			[]float32{}, t.CreationTimeUnix, t.LastUpdateTimeUnix)

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

func (r *Repo) bulkIndexControlObject(index, id string) bulkControlObject {
	return bulkControlObject{
		Index: bulkIndex{
			Index: index,
			ID:    id,
		},
	}
}
