//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
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
		res, err = r.ActionByID(ctx, id, nil, false)
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

func (r *Repo) upsertReferenceBucket(refProp string, ref *models.SingleRef) map[string]interface{} {
	return map[string]interface{}{
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
			`, refProp, refProp, refProp),
			"lang": "painless",
			"params": map[string]interface{}{
				"refs": ref,
			},
		},
	}

}

func (r *Repo) AddReference(ctx context.Context, k kind.Kind, source strfmt.UUID,
	refProp string, ref *models.SingleRef) error {
	// we need to first search for the source, as we need the class name to
	// reference the exact ES index
	className, err := r.classNameFromID(ctx, k, source)
	if err != nil {
		return fmt.Errorf("look up class name: %v", err)
	}

	body := r.upsertReferenceBucket(refProp, ref)

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

	return nil
}
