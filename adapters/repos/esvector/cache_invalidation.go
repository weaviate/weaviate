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
	repo      *Repo
	className string
	id        string
}

func (r *Repo) invalidateCache(className, id string) {
	invalidator := &invalidator{r, className, id}
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
	indexAndIds, err := inv.findIDs()
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

	return nil
}

func (inv *invalidator) findIDs() ([]bulkIndex, error) {
	query, err := inv.buildFindIDQuery()
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

	ids := make([]bulkIndex, len(sr.Hits.Hits), len(sr.Hits.Hits))
	for i, hit := range sr.Hits.Hits {

		// TODO include index
		ids[i] = bulkIndex{
			ID:    hit.ID,
			Index: hit.Index,
		}
	}

	return ids, nil
}

func (inv *invalidator) buildFindIDQuery() (map[string]interface{}, error) {
	paths := inv.repo.schemaRefFinder.Find(schema.ClassName(inv.className))

	var queries []map[string]interface{}

	for _, path := range paths {
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

func (inv *invalidator) bulk(targets []bulkIndex) error {
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

func (inv *invalidator) encodeBatchUpdate(enc *json.Encoder, targets []bulkIndex) error {
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
	Update bulkIndex `json:"update"`
}

func (inv *invalidator) bulkUpdateControlObject(target bulkIndex) bulkUpdateControl {
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
