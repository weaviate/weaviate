package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

type SubQueryNoResultsErr struct{}

func (e SubQueryNoResultsErr) Error() string {
	return "sub-query found no results"
}

type storageIdentifier struct {
	id        string
	kind      kind.Kind
	className string
}

type subQueryBuilder struct {
	repo *Repo
}

func newSubQueryBuilder(r *Repo) *subQueryBuilder {
	return &subQueryBuilder{r}
}

func (s *subQueryBuilder) fromClause(ctx context.Context, clause *filters.Clause) ([]storageIdentifier, error) {
	innerFilter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: clause.Operator,
			On:       clause.On.Child,
			Value:    clause.Value,
		},
	}

	filterQuery, err := s.repo.queryFromFilter(ctx, innerFilter)
	if err != nil {
		return nil, err
	}

	k := s.kindOfClass(clause.On.Child.Class.String())
	index := classIndexFromClassName(k, clause.On.Child.Class.String())
	s.logRequest(index, innerFilter)

	res, err := s.buildBodyAndDoRequest(ctx, filterQuery, k, index)
	if err != nil {
		return nil, fmt.Errorf("subquery: %v", err)
	}

	results, err := s.extractStorageIdentifierFromResults(res)
	if err != nil {
		if _, ok := err.(SubQueryNoResultsErr); ok {
			return nil, err
		}
		return nil, fmt.Errorf("subquery result extraction: %v", err)
	}

	return results, nil
}

func (s *subQueryBuilder) buildBodyAndDoRequest(ctx context.Context,
	filterQuery map[string]interface{}, k kind.Kind, index string) (*esapi.Response, error) {

	body := map[string]interface{}{
		"query":   filterQuery,
		"size":    10000,
		"_source": []string{keyKind.String(), keyClassName.String()},
	}

	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return nil, fmt.Errorf("encode json: %v", err)
	}
	return s.repo.client.Search(
		s.repo.client.Search.WithContext(ctx),
		s.repo.client.Search.WithIndex(index),
		s.repo.client.Search.WithBody(&buf),
	)
}

func (s subQueryBuilder) extractStorageIdentifierFromResults(res *esapi.Response) ([]storageIdentifier, error) {
	if err := errorResToErr(res, s.repo.logger); err != nil {
		return nil, err
	}

	var sr searchResponse
	defer res.Body.Close()
	err := json.NewDecoder(res.Body).Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("decode json: %v", err)
	}

	if len(sr.Hits.Hits) == 0 {
		return nil, SubQueryNoResultsErr{}
	}

	out := make([]storageIdentifier, len(sr.Hits.Hits), len(sr.Hits.Hits))
	for i, hit := range sr.Hits.Hits {
		k, err := kind.Parse(hit.Source[keyKind.String()].(string))
		if err != nil {
			return nil, fmt.Errorf("result %d: %v", i, err)
		}

		out[i] = storageIdentifier{
			id:        hit.ID,
			kind:      k,
			className: hit.Source[keyClassName.String()].(string),
		}
	}

	return out, nil
}

func (s *subQueryBuilder) kindOfClass(className string) kind.Kind {
	sch := s.repo.schemaGetter.GetSchemaSkipAuth()
	kind, _ := sch.GetKindOfClass(schema.ClassName(className))
	return kind
}

func storageIdentifiersToBeaconBoolFilter(in []storageIdentifier, propName string) map[string]interface{} {
	shoulds := make([]interface{}, len(in), len(in))
	for i, sid := range in {
		shoulds[i] = map[string]interface{}{
			"match": map[string]interface{}{
				fmt.Sprintf("%s.beacon", propName): crossref.New("localhost", strfmt.UUID(sid.id), sid.kind).String(),
			},
		}
	}

	return map[string]interface{}{
		"bool": map[string]interface{}{
			"should": shoulds,
		},
	}
}

func (s *subQueryBuilder) logRequest(index string, innerFilter *filters.LocalFilter) {
	s.repo.logger.
		WithField("action", "esvector_filter_subquery").
		WithField("index", index).
		WithField("filters", innerFilter).
		Debug("starting subquery to build search query to esvector")

	s.repo.requestCounter.Inc()
}
