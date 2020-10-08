//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package refcache

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
)

type repo interface {
	MultiGet(ctx context.Context, query []multi.Identifier) ([]search.Result, error)
}

func NewCacher(repo repo, logger logrus.FieldLogger) *Cacher {
	return &Cacher{
		logger: logger,
		repo:   repo,
		store:  map[multi.Identifier]search.Result{},
	}
}

type cacherJob struct {
	si       multi.Identifier
	props    traverser.SelectProperties
	complete bool
}

type Cacher struct {
	sync.Mutex
	jobs   []cacherJob
	logger logrus.FieldLogger
	repo   repo
	store  map[multi.Identifier]search.Result
	meta   *bool // meta is immutable for the lifetime of the request cacher, so we can safely store it
}

func (c *Cacher) Get(si multi.Identifier) (search.Result, bool) {
	sr, ok := c.store[si]
	return sr, ok
}

// Build builds the lookup cache recursively and tries to be smart about it. This
// means that it aims to use only a single (multiget) transaction per layer.
// The recursion exit condition is jobs marked as done. At some point
// the cacher will realise that for every nested prop there is already a
// complete job, so it it stop the recursion.
//
// build is called on a "level" i.e. the search result. After working
// on the job list for the first time if the resolved items still contain
// references and the user set the SelectProperty to indicate they want to
// resolve them, build is called again on all the results (plural!) from the
// previous run. We thus end up with one request to the backend per level
// regardless of the amount of lookups per level.
//
// This keeps request times to a minimum even on deeply nested requests.
func (c *Cacher) Build(ctx context.Context, objects []search.Result,
	properties traverser.SelectProperties, meta bool) error {
	if c.meta == nil {
		// store meta prop if we haven't yet
		c.meta = &meta
	}

	err := c.findJobsFromResponse(objects, properties)
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

// A response is a []search.Result which has all primitive props parsed (and
// even ref-beacons parsed into their respective types, but not resolved!)
// findJobsFromResponse will traverse through it and  check if there are
// references. In a recursive lookup this can both be done on the rootlevel to
// start the first lookup as well as recursively on the results of a lookup to
// further look if a next-level call is required.
func (c *Cacher) findJobsFromResponse(objects []search.Result, properties traverser.SelectProperties) error {
	for _, obj := range objects {
		var err error

		// we can only set SelectProperties on the rootlevel since this is the only
		// place where we have a single root class. In nested lookups we need to
		// first identify the correct path in the SelectProperties graph which
		// correspends with the path we're currently traversing through. Thus we
		// always cache the original SelectProps with the job. This call goes
		// through the job history and looks up the correct SelectProperties
		// subpath to use in this place.
		// tl;dr: On root level (root=base) take props from the outside, on a
		// nested level lookup the SelectProps matching the current base element
		properties, err = c.ReplaceInitialPropertiesWithSpecific(obj, properties)
		if err != nil {
			return err
		}

		if obj.Schema == nil {
			return nil
		}

		schemaMap, ok := obj.Schema.(map[string]interface{})
		if !ok {
			return fmt.Errorf("object schema is present, but not a map: %T", obj)
		}

		for key, value := range schemaMap {
			refKey := uppercaseFirstLetter(key)
			selectProp := properties.FindProperty(refKey)
			skip, unresolved := c.skipProperty(key, value, selectProp)
			if skip {
				continue
			}

			for _, selectPropRef := range selectProp.Refs {
				innerProperties := selectPropRef.RefProperties

				for _, item := range unresolved {
					ref, err := c.extractAndParseBeacon(item)
					if err != nil {
						return err
					}
					c.addJob(multi.Identifier{
						ID:        ref.TargetID.String(),
						Kind:      ref.Kind,
						ClassName: selectPropRef.ClassName}, innerProperties)
				}
			}
		}
	}

	return nil
}

func (c *Cacher) skipProperty(key string, value interface{}, selectProp *traverser.SelectProperty) (bool, models.MultipleRef) {
	// the cacher runs at a point where primitive props have already been
	// parsed, so we can simply look for parsed, but not resolved refenereces
	parsed, ok := value.(models.MultipleRef)
	if !ok {
		// must be another kind of prop, not interesting for us
		return true, nil
	}

	if selectProp == nil {
		// while we did hit a ref propr, the user is not interested in resolving
		// this prop
		return true, nil
	}

	return false, parsed
}

func (c *Cacher) extractAndParseBeacon(item *models.SingleRef) (*crossref.Ref, error) {
	return crossref.Parse(item.Beacon.String())
}

func (c *Cacher) ReplaceInitialPropertiesWithSpecific(obj search.Result,
	properties traverser.SelectProperties) (traverser.SelectProperties, error) {
	if properties != nil {
		// don't overwrite the properties if the caller has explicitly set them,
		// this can only mean they're at the root level
		return properties, nil
	}

	// this is a nested level, we cannot rely on global initialSelectProperties
	// anymore, instead we need to find the selectProperties for exactly this
	// ID
	job, ok := c.findJob(multi.Identifier{
		ID:        obj.ID.String(),
		Kind:      obj.Kind,
		ClassName: obj.ClassName,
	})
	if ok {
		return job.props, nil
	}

	return properties, nil
}

func (c *Cacher) addJob(si multi.Identifier, props traverser.SelectProperties) {
	c.jobs = append(c.jobs, cacherJob{si, props, false})
}

func (c *Cacher) findJob(si multi.Identifier) (cacherJob, bool) {
	for _, job := range c.jobs {
		if job.si == si {
			return job, true
		}
	}

	return cacherJob{}, false
}

// finds incompleteJobs without altering the original job list
func (c *Cacher) incompleteJobs() []cacherJob {
	out := make([]cacherJob, len(c.jobs))
	n := 0
	for _, job := range c.jobs {
		if !job.complete {
			out[n] = job
			n++
		}
	}

	return out[:n]
}

// finds complete jobs  without altering the original job list
func (c *Cacher) completeJobs() []cacherJob {
	out := make([]cacherJob, len(c.jobs))
	n := 0
	for _, job := range c.jobs {
		if job.complete {
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
func (c *Cacher) dedupJobList() {
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
	found := map[multi.Identifier]struct{}{}

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

func (c *Cacher) fetchJobs(ctx context.Context) error {
	jobs := c.incompleteJobs()
	if len(jobs) == 0 {
		c.logSkipFetchJobs()
		return nil
	}

	query := jobListToMultiGetQuery(jobs)
	res, err := c.repo.MultiGet(ctx, query)
	if err != nil {
		return errors.Wrap(err, "fetch job list")
	}

	return c.parseAndStore(ctx, res)
}

func (c *Cacher) logSkipFetchJobs() {
	c.logger.
		WithFields(
			logrus.Fields{
				"action": "request_cacher_fetch_jobs_skip",
			}).
		Trace("skip fetch jobs, have no incomplete jobs")
}

// parseAndStore parses the results for nested refs. Since it is already a
// []search.Result no other parsing is required, as we can expect this type to
// have all primitive props parsed correctly
//
// If nested refs are found, the recursion is started.
//
// Once no more nested refs can be found, the recursion triggers its exit
// condition and all jobs are stored.
func (c *Cacher) parseAndStore(ctx context.Context, res []search.Result) error {
	// mark all current jobs as done, as we use the amount of incomplete jobs as
	// the exit condition for the recursion. Next up, we will start a nested
	// Build() call. If the Build call returns no new jobs, we are done and the
	// recursion stops. If it does return more jobs, we will enter a nested
	// iteration which will eventually come to this place again
	c.markAllJobsAsDone()

	err := c.Build(ctx, removeEmptyResults(res), nil, *c.meta)
	if err != nil {
		return errors.Wrap(err, "build nested cache")
	}

	err = c.storeResults(res)
	if err != nil {
		return err
	}

	return nil
}

func removeEmptyResults(in []search.Result) []search.Result {
	out := make([]search.Result, len(in))
	n := 0
	for _, obj := range in {
		if obj.ID != "" {
			out[n] = obj
			n++
		}
	}

	return out[0:n]
}

func (c *Cacher) storeResults(res search.Results) error {
	for _, item := range res {
		c.store[multi.Identifier{
			ID:        item.ID.String(),
			Kind:      item.Kind,
			ClassName: item.ClassName,
		}] = item
	}

	return nil
}

func (c *Cacher) markAllJobsAsDone() {
	for i := range c.jobs {
		c.jobs[i].complete = true
	}
}

func jobListToMultiGetQuery(jobs []cacherJob) []multi.Identifier {
	query := make([]multi.Identifier, len(jobs))
	for i, job := range jobs {
		query[i] = job.si
	}

	return query
}

func uppercaseFirstLetter(in string) string {
	first := string(in[0])
	rest := string(in[1:])

	return strings.ToUpper(first) + rest
}
