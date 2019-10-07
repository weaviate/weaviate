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
	"time"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

func (r *Repo) InitCacheIndexing(bulkSize int, waitOnIdle time.Duration, waitOnBusy time.Duration) {
	i := newCacheIndexer(bulkSize, waitOnIdle, waitOnBusy, r)
	r.cacheIndexer = i
	r.cacheIndexer.init()
}

func (r *Repo) StopCacheIndexing() {
	r.cacheIndexer.stop <- struct{}{}
}

type cacheIndexer struct {
	bulkSize   int
	waitOnIdle time.Duration
	waitOnBusy time.Duration
	repo       *Repo
	stop       chan struct{}
}

func newCacheIndexer(bulkSize int, waitOnIdle, waitOnBusy time.Duration, repo *Repo) *cacheIndexer {
	return &cacheIndexer{
		bulkSize:   bulkSize,
		waitOnIdle: waitOnIdle,
		waitOnBusy: waitOnBusy,
		repo:       repo,
		stop:       make(chan struct{}),
	}
}

func (i *cacheIndexer) init() {
	go func() {
		for {

			select {
			case <-i.stop:
				return
			default:
			}

			count, err := i.singleCycle()
			if err != nil {
				i.repo.logger.WithError(err).
					WithField("action", "esvector_cache_cycle").
					Error("could not complete caching cycle")
			}

			if count == 0 {
				// nothing to do at the moment, let's sleep for the configured duration
				// before trying again
				time.Sleep(i.waitOnIdle)

				continue
			}

			i.repo.logger.
				WithField("action", "esvector_cache_cycle_complete").
				WithField("count", count).
				Debugf("succesfully populated cache for %d items", count)

				// don't immediately take up work, as it is more effecient to work to
				// off a longer queue, so wait some time for the queue to gain a few
				// items
			time.Sleep(i.waitOnBusy)
		}
	}()
}

func (i *cacheIndexer) singleCycle() (int, error) {
	worklist, err := i.listWork()
	if err != nil {
		return 0, fmt.Errorf("caching cycle: %v", err)
	}

	err = i.populateWork(worklist)
	if err != nil {
		return 0, fmt.Errorf("caching cycle: %v", err)
	}

	err = i.refreshIndices()
	if err != nil {
		return len(worklist), fmt.Errorf("caching cycle: refresh indices: %v", err)
	}

	return len(worklist), nil
}

func (i *cacheIndexer) listWork() ([]hit, error) {
	ctx, cancel := ctx()
	defer cancel()

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
		i.repo.client.Search.WithIndex(allClassIndices),
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

		ctx, cancel := ctx()
		defer cancel()

		err := i.repo.PopulateCache(ctx, k, strfmt.UUID(obj.ID))
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *cacheIndexer) refreshIndices() error {
	req := esapi.IndicesRefreshRequest{
		Index: []string{allClassIndices},
	}

	ctx, cancel := ctx()
	defer cancel()

	res, err := req.Do(ctx, i.repo.client)
	if err != nil {
		return fmt.Errorf("index refresh request: %v", err)
	}

	if err := errorResToErr(res, i.repo.logger); err != nil {
		i.repo.logger.WithField("action", "esvector_cache_cycle_final_refresh").
			WithError(err).
			WithField("request", req).
			WithField("res", res).
			Errorf("final refresh after populating work list failed")

		return fmt.Errorf("index refresh request: %v", err)
	}

	return nil
}

func ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}
