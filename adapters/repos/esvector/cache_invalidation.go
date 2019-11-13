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
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
)

type invalidator struct {
	repo       *Repo
	className  string
	id         string
	depthLimit int
}

func (r *Repo) invalidateCache(className, id string) {
	invalidator := &invalidator{r, className, id, r.denormalizationDepthLimit}
	err := invalidator.do()
	if err != nil {
		// log and ignore, we are doing an async operation here, we can't return
		// feedback to anyone
		r.logger.WithError(err).WithField("action", "esvector_cache_invalidation_build_query").
			Error("invalidating cache errord")
		return
	}

}

func (inv *invalidator) do() error {

	for i := 1; i <= inv.depthLimit; i++ {
		indexAndIds, err := inv.findIDs(i)
		if err != nil {
			return err
		}
		if len(indexAndIds) == 0 {
			return nil
		}

		err = inv.bulk(indexAndIds)
		if err != nil {
			return err
		}
	}

	return nil
}

func (inv *invalidator) findIDs(level int) ([]bulkID, error) {
	query, err := inv.buildFindIDQuery(level)
	if err != nil {
		return nil, err
	}

	if query == nil {
		return nil, nil
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(query)
	if err != nil {
		return nil, fmt.Errorf("encoding json: %v", err)
	}
	res, err := inv.repo.client.Search(
		inv.repo.client.Search.WithIndex(allClassIndices),
		inv.repo.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("id search: %v", err)
	}

	if err := errorResToErr(res, inv.repo.logger); err != nil {
		return nil, fmt.Errorf("id search: %v", err)
	}

	var sr searchResponse
	defer res.Body.Close()
	err = json.NewDecoder(res.Body).Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("id search: decode json: %v", err)
	}

	ids := make([]bulkID, len(sr.Hits.Hits), len(sr.Hits.Hits))
	for i, hit := range sr.Hits.Hits {

		ids[i] = bulkID{
			ID:    hit.ID,
			Index: hit.Index,
		}
	}

	return ids, nil
}

func (inv *invalidator) buildFindIDQuery(level int) (map[string]interface{}, error) {
	paths := inv.repo.schemaRefFinder.Find(schema.ClassName(inv.className))

	var queries []map[string]interface{}

	for _, path := range paths {
		if len(path.Slice()) != (2*level + 1) {
			// path is always an odd number, as always contains a primitive property
			// (last element), optionally it can contain always a combination of
			// refProp+class, that*s why the check is for 2*level+1
			continue
		}
		clause := &filters.Clause{
			On:       &path,
			Value:    &filters.Value{Value: inv.id, Type: schema.DataTypeString},
			Operator: filters.OperatorEqual,
		}

		query, err := refFilterFromClause(clause)
		if err != nil {
			return nil, fmt.Errorf("build clause: %v", err)
		}
		queries = append(queries, query)
	}

	if len(queries) == 0 {
		return nil, nil
	}

	body := map[string]interface{}{
		"_source": false,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": queries,
			},
		},
	}

	return body, nil

}

func (inv *invalidator) bulk(targets []bulkID) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	err := inv.encodeBatchUpdate(enc, targets)
	if err != nil {
		return fmt.Errorf("inavlidate cache: encode request: %v", err)
	}

	req := esapi.BulkRequest{
		Body: &buf,
	}

	res, err := req.Do(context.Background(), inv.repo.client)
	if err != nil {
		return fmt.Errorf("inavlidate cache: bulk request: %v", err)
	}

	if err := errorResToErr(res, inv.repo.logger); err != nil {
		return fmt.Errorf("inavlidate cache: bulk request: %v", err)
	}

	return nil
}

func (inv *invalidator) encodeBatchUpdate(enc *json.Encoder, targets []bulkID) error {
	for _, target := range targets {

		control := inv.bulkUpdateControlObject(target)
		err := enc.Encode(control)
		if err != nil {
			return err
		}

		payload := inv.bulkUpdatePayloadObject()
		err = enc.Encode(payload)
		if err != nil {
			return err
		}
	}

	return nil
}

type bulkUpdateControl struct {
	Update bulkID `json:"update"`
}

func (inv *invalidator) bulkUpdateControlObject(target bulkID) bulkUpdateControl {
	return bulkUpdateControl{
		Update: target,
	}
}

func (inv *invalidator) bulkUpdatePayloadObject() map[string]interface{} {
	return map[string]interface{}{
		"doc": map[string]interface{}{
			keyCache.String(): map[string]interface{}{
				keyCacheHot.String(): false,
			},
		},
	}
}
