package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
)

func (r *Repo) classNameFromID(ctx context.Context, k kind.Kind, id strfmt.UUID) (string, error) {
	var res *search.Result
	var err error
	switch k {
	case kind.Thing:
		res, err = r.ThingByID(ctx, id, nil, false)
	case kind.Action:
		res, err = r.ThingByID(ctx, id, nil, false)
	default:
		return "", fmt.Errorf("impossible kind: %v", k)
	}
	if err != nil {
		return "", err
	}

	if res == nil {
		return "", fmt.Errorf("source not found")
	}

	return res.ClassName, nil
}

func (r *Repo) AddReference(ctx context.Context, k kind.Kind, source strfmt.UUID,
	refProp string, ref *models.SingleRef) error {
	// we need to first search for the source, as we need the class name to
	// reference the exact ES index
	className, err := r.classNameFromID(ctx, k, source)
	if err != nil {
		return fmt.Errorf("look up class name: %v", err)
	}

	body := map[string]interface{}{
		"upsert": map[string]interface{}{
			refProp: []interface{}{},
		},
		"script": map[string]interface{}{
			"source": fmt.Sprintf(`
				if (ctx._source.containsKey("%s")) { 
					ctx._source.%s.add(params.refs)
				} else { 
					ctx._source.%s = [params.refs]
				} 
			  ctx._source.%s.%s = false
			`, refProp, refProp, refProp, keyCache, keyCacheHot),
			"lang": "painless",
			"params": map[string]interface{}{
				"refs": ref,
			},
		},
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return fmt.Errorf("index request: encode json: %v", err)
	}

	retries := 3
	req := esapi.UpdateRequest{
		Index:           classIndexFromClassName(k, className),
		DocumentID:      source.String(),
		RetryOnConflict: &retries,
		Body:            &buf,
	}

	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("index request: %v", err)
	}

	if err := errorResToErr(res, r.logger); err != nil {
		r.logger.WithField("action", "vector_index_update_concept_with_reference").
			WithError(err).
			WithField("request", req).
			WithField("res", res).
			WithField("body_before_marshal", body).
			WithField("body", buf.String()).
			Errorf("put concept failed")

		return fmt.Errorf("update refererence request: %v", err)
	}

	go r.invalidateCache(className, source.String())

	return nil
}
