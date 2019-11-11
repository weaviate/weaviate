package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

func (r *Repo) Merge(ctx context.Context, merge kinds.MergeDocument) error {

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := r.encodeMerge(enc, merge)
	if err != nil {
		return fmt.Errorf("merge: encode: %v", err)
	}

	req := esapi.BulkRequest{
		Body: &buf,
	}
	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("merge: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		return fmt.Errorf("merge: %v", err)
	}

	return r.errorsInBulkResponse(res)
}

func (r *Repo) errorsInBulkResponse(res *esapi.Response) error {
	var parsed bulkIndexResponse
	err := json.NewDecoder(res.Body).Decode(&parsed)
	if err != nil {
		return err
	}

	if !parsed.Errors {
		// no need to check for error positions if there are none
		return nil
	}

	var errors []string
	for _, item := range parsed.Items {
		err := item.Update.Error
		if err != nil {
			errors = append(errors, err.(string))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, ", "))
	}
	return nil
}

func (r *Repo) encodeMerge(enc *json.Encoder, merge kinds.MergeDocument) error {
	if merge.PrimitiveSchema != nil && len(merge.PrimitiveSchema) > 0 {
		if err := r.encodeMergePrimitive(enc, merge); err != nil {
			return fmt.Errorf("encode primitive: %v", err)
		}
	}

	if merge.References != nil && len(merge.References) > 0 {
		if err := r.encodeMergeRefs(enc, merge); err != nil {
			return fmt.Errorf("encode refs: %v", err)
		}
	}

	return nil
}

func (r *Repo) encodeMergePrimitive(enc *json.Encoder, merge kinds.MergeDocument) error {
	index := classIndexFromClassName(merge.Kind, merge.Class)
	control := r.bulkUpdateControlObject(index, merge.ID.String())
	props := r.addPropsToBucket(map[string]interface{}{}, merge.PrimitiveSchema)

	bucket := r.primitiveUpsertBucket(props)

	err := enc.Encode(control)
	if err != nil {
		return err
	}

	err = enc.Encode(bucket)
	if err != nil {
		return err
	}

	return nil
}

func (r *Repo) encodeMergeRefs(enc *json.Encoder, merge kinds.MergeDocument) error {
	return r.encodeBatchReferences(enc, merge.References)
}

func (r *Repo) primitiveUpsertBucket(schema map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"doc": schema,
	}
}
