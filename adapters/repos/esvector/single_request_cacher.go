//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
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
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
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
	meta   *bool // meta is immutable for the lifetime of the request cacher, so we can safely store it
}

func (c *cacher) get(si storageIdentifier) (search.Result, bool) {
	sr, ok := c.store[si]
	return sr, ok
}

// build builds the lookup cache recursive and tries to be smart about it. This
// means that it aims to send only a single request (mget) to elasticsearch per
// level. the recursion exit condition is jobs marked as done. At some point
// the cacher will realise that for every nested prop there is already a
// complete job, so it it stop the recursion.
//
// build is called on a "level" i.e. the raw schema before parse. After working
// on the job list for the first time if the resolved items still contain
// references and the user set the SelectProperty to indicate they want to
// resolve them, build is called again on all the results (plural!) from the
// previous run. We thus end up with one request to the backend per level
// regardless of the amount of lookups per level.
//
// This keeps request times to a minimum even on deeply nested requests.
//
// parseSchema which will be called on the root element subsequently does not
// do any more lookups against the backend on its own, it only asks the cacher.
// Thus it is important that a cacher.build() runs before the first parseSchema
// is started.
func (c *cacher) build(ctx context.Context, sr searchResponse,
	properties traverser.SelectProperties, meta bool) error {
	if c.meta == nil {
		// store meta prop if we haven't yet
		c.meta = &meta
	}

	err := c.findJobsFromResponse(sr, properties)
	if err != nil {
		return fmt.Errorf("build request cache: %v", err)
	}

	c.dedupJobList()
	err = c.fetchJobs(ctx)
	if err != nil {
		return fmt.Errorf("build request cache: %v", err)
	}

	return nil
}

// a response is a raw (unparsed) search response from elasticsearch.
// findJobsFromResponse will traverse through it and  check if there are
// references. In a recursive lookup this can both be done on the rootlevel to
// start the first lookup as well as recursively on the results of a lookup to
// further look if a next-level call is required.
func (c *cacher) findJobsFromResponse(sr searchResponse, properties traverser.SelectProperties) error {
	for _, hit := range sr.Hits.Hits {
		var err error

		// we can only set SelectProperties on the rootlevel since this is the only
		// place where we have a singel root class. In nested lookups we need to
		// first identify the correct path in the SelectProperties graph which
		// correspends with the path we're currently traversing through. Thus we
		// always cache the original SelectProps with the job. This call goes
		// thorough the job history and looks up the correct SelectProperties
		// subpath to use in this place.
		// tl;dr: On root level (root=base) take props from the outside, on a
		// nested level lookup the SelectProps matching the current base element
		properties, err = c.replaceInitialPropertiesWithSpecific(hit, properties)
		if err != nil {
			return err
		}

		for key, value := range hit.Source {
			refKey := uppercaseFirstLetter(key)
			selectProp := properties.FindProperty(refKey)

			skip, propSlice := c.skipProperty(key, value, selectProp)
			if skip {
				continue
			}

			for _, selectPropRef := range selectProp.Refs {
				innerProperties := selectPropRef.RefProperties

				for _, item := range propSlice {
					ref, err := c.extractAndParseBeacon(item)
					if err != nil {
						return err
					}
					c.addJob(storageIdentifier{
						id:        ref.TargetID.String(),
						kind:      ref.Kind,
						className: selectPropRef.ClassName}, innerProperties)
				}
			}
		}
	}

	return nil
}

func (c *cacher) skipProperty(key string, value interface{}, selectProp *traverser.SelectProperty) (bool, []interface{}) {
	if isInternal(key) {
		return true, nil
	}

	asSlice, ok := value.([]interface{})
	if !ok {
		// not a slice, can't be ref, not interested
		return true, nil
	}

	if selectProp == nil {
		// user is not interested in this prop
		return true, nil
	}

	return false, asSlice
}

func (c *cacher) extractAndParseBeacon(item interface{}) (*crossref.Ref, error) {
	refMap, ok := item.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected ref item to be a map, but got %T", item)
	}

	beacon, ok := refMap["beacon"]
	if !ok {
		return nil, fmt.Errorf("expected ref object to have field beacon, but got %#v", refMap)
	}

	return crossref.Parse(beacon.(string))
}

func (c *cacher) replaceInitialPropertiesWithSpecific(hit hit,
	properties traverser.SelectProperties) (traverser.SelectProperties, error) {

	if properties != nil {
		// don't overwrite the properties if the caller has explicitly set them,
		// this can only mean they're at the root level
		return properties, nil
	}

	// this is a nested level, we cannot rely on global initialSelectProperties
	// anymore, instead we need to find the selectProperties for exactly this
	// ID
	id := hit.ID

	k, err := kind.Parse(hit.Source[keyKind.String()].(string))
	if err != nil {
		return nil, err
	}

	className := hit.Source[keyClassName.String()].(string)
	job, ok := c.findJob(storageIdentifier{id, k, className})
	if ok {
		return job.props, nil
	}

	return properties, nil
}

func (c *cacher) addJob(si storageIdentifier, props traverser.SelectProperties) {
	c.jobs = append(c.jobs, cacherJob{si, props, false})
}

func (c *cacher) findJob(si storageIdentifier) (cacherJob, bool) {
	for _, job := range c.jobs {
		if job.si == si {
			return job, true

		}
	}

	return cacherJob{}, false
}

// finds incompleteJobs without altering the original job list
func (c *cacher) incompleteJobs() []cacherJob {
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

// finds complete jobs  without altering the original job list
func (c *cacher) completeJobs() []cacherJob {
	out := make([]cacherJob, len(c.jobs))
	n := 0
	for _, job := range c.jobs {
		if job.complete == true {
			out[n] = job
			n++
		}
	}

	return out[:n]
}

// alters the list, removes duplicates. Ignores complete jobs, as a job could
// already marked as complete, but not yet stored since the completion is the
// exit condition for the recursion. However, the storage can only happen once
// the schema was parsed. If the schema contains more refs to an item that is
// already in the joblist we are in a catch-22. To resolve that, we allow
// duplicates with already complete jobs since retrieving the required item
// again (with different SelectProperties) comes at minimal cost and is the
// only way out of that deadlock situation.
func (c *cacher) dedupJobList() {
	incompleteJobs := c.incompleteJobs()
	before := len(incompleteJobs)
	if before == 0 {
		// nothing to do
		return
	}
	c.logger.
		WithFields(logrus.Fields{
			"action": "request_cacher_dedup_joblist_start",
			"jobs":   before,
		}).
		Debug("starting job list deduplication")
	deduped := make([]cacherJob, len(incompleteJobs))
	found := map[storageIdentifier]struct{}{}

	n := 0
	for _, job := range incompleteJobs {
		if _, ok := found[job.si]; ok {
			continue
		}

		found[job.si] = struct{}{}
		deduped[n] = job
		n++
	}

	c.jobs = append(c.completeJobs(), deduped[:n]...)

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

func (c *cacher) fetchJobs(ctx context.Context) error {
	before := time.Now()
	jobs := c.incompleteJobs()
	if len(jobs) == 0 {
		c.logSkipFetchJobs()
		return nil
	}

	c.repo.requestCounter.Inc()
	body := jobListToMgetBody(jobs)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return err
	}

	req := esapi.MgetRequest{
		Body: &buf,
	}

	res, err := req.Do(ctx, c.repo.client)
	if err != nil {
		return err
	}

	if err := errorResToErr(res, c.logger); err != nil {
		return err
	}

	c.logCompleteFetchJobs(before, len(jobs))
	return c.parseAndStore(ctx, res)
}

func (c *cacher) logSkipFetchJobs() {
	c.logger.
		WithFields(
			logrus.Fields{
				"action": "request_cacher_fetch_jobs_skip",
			}).
		Debug("skip fetch jobs, have no incomplete jobs")
}

func (c *cacher) logCompleteFetchJobs(start time.Time, amount int) {
	took := time.Since(start)

	c.logger.
		WithFields(
			logrus.Fields{
				"action": "request_cacher_fetch_jobs_complete",
				"took":   took,
				"jobs":   amount,
			}).
		Debug("fetch jobs complete")
}

type mgetResponse struct {
	Docs []hit `json:"docs"`
}

func (c *cacher) parseAndStore(ctx context.Context, res *esapi.Response) error {
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

	// this is our exit condition for the recursion
	c.markAllJobsAsDone()

	err = c.build(ctx, sr, nil, *c.meta)
	if err != nil {
		return fmt.Errorf("build nested cache: %v", err)
	}

	asResults, err := sr.toResults(c.repo, nil, false, c)
	if err != nil {
		return err
	}

	err = c.storeResults(asResults)
	if err != nil {
		return err
	}

	return nil
}

// remaps from x.docs to x.hits.hits, so we can use existing infrastructure to
// parse it
func mgetResToSearchResponse(in mgetResponse) searchResponse {
	return searchResponse{
		Hits: struct {
			Hits []hit `json:"hits"`
		}{
			Hits: removeEmptyResults(in.Docs),
		},
	}
}

func removeEmptyResults(in []hit) []hit {

	out := make([]hit, len(in))
	n := 0
	for _, hit := range in {
		if hit.Source != nil {
			out[n] = hit
			n++
		}
	}

	return out[0:n]
}

func (c *cacher) storeResults(res search.Results) error {
	for _, item := range res {
		c.store[storageIdentifier{
			id:        item.ID.String(),
			kind:      item.Kind,
			className: item.ClassName,
		}] = item
	}

	return nil
}

func (c *cacher) markAllJobsAsDone() {
	for i := range c.jobs {
		c.jobs[i].complete = true
	}
}

func jobListToMgetBody(jobs []cacherJob) mgetBody {
	docs := make([]mgetDoc, len(jobs))
	for i, job := range jobs {
		docs[i] = mgetDoc{
			Index: classIndexFromClassName(job.si.kind, job.si.className),
			ID:    job.si.id,
		}

	}

	return mgetBody{docs}
}
