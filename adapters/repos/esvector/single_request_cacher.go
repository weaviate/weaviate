package esvector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
)

func newCacher(repo *Repo) *cacher {
	return &cacher{
		logger: repo.logger,
		repo:   repo,
		store:  map[storageIdentifier]search.Result{},
	}
}

type cacherJob struct {
	si       storageIdentifier
	props    traverser.SelectProperties
	complete bool
}

type cacher struct {
	sync.Mutex
	jobs   []cacherJob
	logger logrus.FieldLogger
	repo   *Repo
	store  map[storageIdentifier]search.Result
}

func (c *cacher) get(si storageIdentifier) (search.Result, bool) {
	c.Lock()
	defer c.Unlock()

	sr, ok := c.store[si]
	return sr, ok
}

func (c *cacher) findJobsFromResponse(sr searchResponse, properties traverser.SelectProperties) error {
	for _, hit := range sr.Hits.Hits {
		for key, value := range hit.Source {
			if isInternal(key) {
				continue
			}

			asSlice, ok := value.([]interface{})
			if !ok {
				// not a slice, can't be ref, not interested
				continue
			}

			refKey := uppercaseFirstLetter(key)
			selectProp := properties.FindProperty(refKey)
			if selectProp == nil {
				// user is not interested in this prop
				continue
			}

			for _, selectPropRef := range selectProp.Refs {
				innerProperties := selectPropRef.RefProperties

				for _, item := range asSlice {
					refMap, ok := item.(map[string]interface{})
					if !ok {
						return fmt.Errorf("expected ref item to be a map, but got %T", item)
					}

					beacon, ok := refMap["beacon"]
					if !ok {
						return fmt.Errorf("expected ref object to have field beacon, but got %#v", refMap)
					}

					ref, err := crossref.Parse(beacon.(string))
					if err != nil {
						return err
					}
					c.addJob(storageIdentifier{ref.TargetID.String(), ref.Kind, selectPropRef.ClassName}, innerProperties)
				}
			}
		}
	}

	return nil
}

func (c *cacher) addJob(si storageIdentifier, props traverser.SelectProperties) {
	c.Lock()
	defer c.Unlock()

	c.jobs = append(c.jobs, cacherJob{si, props, false})
}

func (c *cacher) incompleteJobs() []cacherJob {
	c.Lock()
	defer c.Unlock()

	out := make([]cacherJob, len(c.jobs))
	n := 0
	for _, job := range c.jobs {
		if job.complete == false {
			out[n] = job
			n++
		}
	}

	return out[:n]
}

func (c *cacher) dedupJobList() {
	c.Lock()
	defer c.Unlock()

	before := len(c.jobs)
	c.logger.
		WithFields(logrus.Fields{
			"action": "request_cacher_dedup_joblist_start",
			"jobs":   before,
		}).
		Debug("starting job list deduplication")
	deduped := make([]cacherJob, len(c.jobs))
	found := map[string]struct{}{}

	n := 0
	for _, job := range c.jobs {
		if _, ok := found[job.si.id]; ok {
			continue
		}

		found[job.si.id] = struct{}{}
		deduped[n] = job
		n++
	}

	c.jobs = deduped[:n]

	c.logger.
		WithFields(logrus.Fields{
			"action":      "request_cacher_dedup_joblist_complete",
			"jobs":        n,
			"removedJobs": before - n,
		}).
		Debug("completed job list deduplication")
}

type mgetBody struct {
	Docs []mgetDoc `json:"docs"`
}

type mgetDoc struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

func (c *cacher) fetchJobs() error {
	before := time.Now()
	jobs := c.incompleteJobs()
	if len(jobs) == 0 {
		c.logger.
			WithFields(
				logrus.Fields{
					"action": "request_cacher_fetch_jobs_skip",
				}).
			Debug("skip fetch jobs, have no incomplete jobs")
		return nil
	}

	docs := make([]mgetDoc, len(jobs))
	for i, job := range jobs {
		docs[i] = mgetDoc{
			Index: classIndexFromClassName(job.si.kind, job.si.className),
			ID:    job.si.id,
		}

	}
	body := mgetBody{docs}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return err
	}

	req := esapi.MgetRequest{
		Body: &buf,
	}

	ctx := context.Background()
	res, err := req.Do(ctx, c.repo.client) // TODO: don't spawn new context
	if err != nil {
		return err
	}

	if err := errorResToErr(res, c.logger); err != nil {
		return err
	}

	took := time.Since(before)

	c.logger.
		WithFields(
			logrus.Fields{
				"action": "request_cacher_fetch_jobs_complete",
				"took":   took,
				"jobs":   len(jobs),
			}).
		Debug("fetch jobs complete")

	return c.parseAndStore(res)
}

type mgetResponse struct {
	Docs []hit `json:"docs"`
}

func (c *cacher) parseAndStore(res *esapi.Response) error {
	if err := errorResToErr(res, c.logger); err != nil {
		return err
	}

	var mgetRes mgetResponse
	defer res.Body.Close()
	err := json.NewDecoder(res.Body).Decode(&mgetRes)
	if err != nil {
		return fmt.Errorf("decode json: %v", err)
	}

	sr := mgetResToSearchResponse(mgetRes)

	// // TODO: don't let this spawn a nested cacher (use global cacher per request)
	// // TODO: don't hardcode selectprops as nil, as we'll never get recursion
	// // TODO: don't hardcode meta==false
	asResults, err := sr.toResults(c.repo, nil, false)
	if err != nil {
		return err
	}

	return c.storeResults(asResults)
}

// remaps from x.docs to x.hits.hits, so we can use existing infrastructure to
// parse it
func mgetResToSearchResponse(in mgetResponse) searchResponse {
	return searchResponse{
		Hits: struct {
			Hits []hit `json:"hits"`
		}{
			Hits: in.Docs,
		},
	}

}

func (c *cacher) storeResults(res search.Results) error {
	c.Lock()
	defer c.Unlock()

	for _, item := range res {
		c.store[storageIdentifier{
			id:        item.ID.String(),
			kind:      item.Kind,
			className: item.ClassName,
		}] = item

	}

	return nil
}
