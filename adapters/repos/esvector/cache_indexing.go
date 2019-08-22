package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

func (r *Repo) InitCacheIndexing(bulkSize int, wait time.Duration) {
	i := newCacheIndexer(bulkSize, wait, r)
	i.init()
}

type cacheIndexer struct {
	bulkSize int
	wait     time.Duration
	repo     *Repo
}

func newCacheIndexer(bulkSize int, wait time.Duration, repo *Repo) *cacheIndexer {
	return &cacheIndexer{bulkSize: bulkSize, wait: wait, repo: repo}
}

func (i *cacheIndexer) init() {
	fmt.Print(i.singleCycle())

}

func (i *cacheIndexer) singleCycle() error {
	worklist, err := i.listWork()
	if err != nil {
		return fmt.Errorf("caching cycle: %v", err)
	}

	err = i.populateWork(worklist)
	if err != nil {
		return fmt.Errorf("caching cycle: %v", err)
	}

	return nil
}

func (i *cacheIndexer) listWork() ([]hit, error) {
	ctx := context.Background()

	body := map[string]interface{}{
		"size":    i.bulkSize,
		"_source": []interface{}{keyKind},
		"sort": []interface{}{
			map[string]interface{}{
				keyCreated.String(): map[string]interface{}{
					"order": "asc",
				},
			},
		},
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				fmt.Sprintf("%s.%s", keyCache, keyCacheHot): false,
			},
		},
	}

	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(body)
	if err != nil {
		return nil, fmt.Errorf("vector search: encode json: %v", err)
	}

	res, err := i.repo.client.Search(
		i.repo.client.Search.WithContext(ctx),
		i.repo.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	return i.indexAndIDsFromRes(res)
}

func (i *cacheIndexer) indexAndIDsFromRes(res *esapi.Response) ([]hit, error) {
	if err := errorResToErr(res, i.repo.logger); err != nil {
		return nil, fmt.Errorf("extract ids and indices: %v", err)
	}

	var sr searchResponse
	defer res.Body.Close()
	err := json.NewDecoder(res.Body).Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("decode json: %v", err)
	}

	return sr.Hits.Hits, nil
}

func (i *cacheIndexer) populateWork(worklist []hit) error {
	for _, obj := range worklist {
		kindString, ok := obj.Source[keyKind.String()]
		if !ok {
			return fmt.Errorf("missing kind on %#v", obj)
		}

		k, _ := kind.Parse(kindString.(string))
		err := i.repo.PopulateCache(context.Background(), k, strfmt.UUID(obj.ID))
		if err != nil {
			return err
		}
	}

	return nil
}
