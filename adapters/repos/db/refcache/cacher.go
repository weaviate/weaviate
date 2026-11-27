//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package refcache

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
)

type repo interface {
	// MultiGet reads each object of class c with the properties and vectors
	// selections[c] selects.
	MultiGet(ctx context.Context, query []multi.Identifier,
		selections map[string]search.SelectClass, tenant string) ([]search.Result, error)
}

func NewCacher(repo repo, logger logrus.FieldLogger, tenant string) *Cacher {
	return &Cacher{
		logger: logger,
		repo:   repo,
		jobIdx: map[multi.Identifier]int{},
		store:  map[multi.Identifier]search.Result{},
		tenant: tenant,
	}
}

// cacherJob holds in sel the union of what the reference paths reaching si
// select. Of their additional properties, sel keeps only the vectors.
type cacherJob struct {
	si       multi.Identifier
	sel      search.SelectClass
	complete bool
}

type Cacher struct {
	sync.Mutex
	jobs       []cacherJob
	jobIdx     map[multi.Identifier]int // position of each identifier in jobs
	logger     logrus.FieldLogger
	repo       repo
	store      map[multi.Identifier]search.Result
	tenant     string
	groupByIdx search.SelectPropertiesIndex
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
	properties search.SelectProperties, groupByIdx search.SelectPropertiesIndex,
) error {
	c.groupByIdx = groupByIdx
	err := c.findJobsFromResponse(objects, properties)
	if err != nil {
		return fmt.Errorf("build request cache: %w", err)
	}

	err = c.fetchJobs(ctx)
	if err != nil {
		return fmt.Errorf("build request cache: %w", err)
	}

	return nil
}

// A response is a []search.Result which has all primitive props parsed (and
// even ref-beacons parsed into their respective types, but not resolved!)
// findJobsFromResponse will traverse through it and  check if there are
// references. In a recursive lookup this can both be done on the rootlevel to
// start the first lookup as well as recursively on the results of a lookup to
// further look if a next-level call is required.
func (c *Cacher) findJobsFromResponse(objects []search.Result, properties search.SelectProperties) error {
	// root-level properties are shared by every object; index once here
	rootIdx := properties.Indexed()

	for _, obj := range objects {
		// we can only set SelectProperties on the rootlevel since this is the only
		// place where we have a single root class. In nested lookups we need to
		// first identify the correct path in the SelectProperties graph which
		// correspends with the path we're currently traversing through. Thus we
		// always cache the original SelectProps with the job. This call goes
		// through the job history and looks up the correct SelectProperties
		// subpath to use in this place.
		// tl;dr: On root level (root=base) take props from the outside, on a
		// nested level lookup the SelectProps matching the current base element
		idx := rootIdx
		if properties == nil {
			propertiesReplaced, err := c.ReplaceInitialPropertiesWithSpecific(obj, properties)
			if err != nil {
				return err
			}
			idx = propertiesReplaced.Indexed()
		}

		if obj.Schema == nil {
			// only this object has nothing to look up; siblings may still have refs
			continue
		}

		schemaMap, ok := obj.Schema.(map[string]interface{})
		if !ok {
			return fmt.Errorf("object schema is present, but not a map: %T", obj)
		}

		if err := c.parseSchemaMap(schemaMap, idx); err != nil {
			return err
		}

		if c.groupByIdx != nil {
			if err := c.parseAdditionalGroup(obj); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Cacher) parseAdditionalGroup(obj search.Result) error {
	if obj.AdditionalProperties != nil && obj.AdditionalProperties["group"] != nil {
		if group, ok := obj.AdditionalProperties["group"].(*additional.Group); ok {
			for _, hitMap := range group.Hits {
				if err := c.parseSchemaMap(hitMap, c.groupByIdx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *Cacher) parseSchemaMap(schemaMap map[string]interface{}, idx search.SelectPropertiesIndex) error {
	for key, value := range schemaMap {
		selectProp := idx.Find(key)
		skip, unresolved := c.skipProperty(key, value, selectProp)
		if skip {
			continue
		}

		for _, selectPropRef := range selectProp.Refs {
			for _, item := range unresolved {
				ref, err := c.extractAndParseBeacon(item)
				if err != nil {
					return err
				}
				c.addJob(multi.Identifier{
					ID:        ref.TargetID.String(),
					ClassName: selectPropRef.ClassName,
				}, selectPropRef)
			}
		}
	}
	return nil
}

func (c *Cacher) skipProperty(key string, value interface{}, selectProp *search.SelectProperty) (bool, models.MultipleRef) {
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
	properties search.SelectProperties,
) (search.SelectProperties, error) {
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
		ClassName: obj.ClassName,
	})
	if ok {
		return job.sel.RefProperties, nil
	}

	return properties, nil
}

// addJob adds what sel selects to the job for si. A job that already exists
// takes the union, and runs again if that grows it, because its earlier read
// decoded only what was selected then.
func (c *Cacher) addJob(si multi.Identifier, sel search.SelectClass) {
	i, ok := c.jobIdx[si]
	if !ok {
		i = len(c.jobs)
		c.jobIdx[si] = i
		c.jobs = append(c.jobs, cacherJob{si: si})
	}

	job := &c.jobs[i]
	var grew bool
	if job.sel, grew = mergeSelectClass(job.sel, sel); grew {
		job.complete = false
	}
}

func (c *Cacher) findJob(si multi.Identifier) (cacherJob, bool) {
	i, ok := c.jobIdx[si]
	if !ok {
		return cacherJob{}, false
	}
	return c.jobs[i], true
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

func (c *Cacher) fetchJobs(ctx context.Context) error {
	jobs := c.incompleteJobs()
	if len(jobs) == 0 {
		c.logSkipFetchJobs()
		return nil
	}

	query := make([]multi.Identifier, len(jobs))
	selections := map[string]search.SelectClass{}
	for i, job := range jobs {
		query[i] = job.si
		sel := selections[job.si.ClassName]
		sel.ClassName = job.si.ClassName
		sel, _ = mergeSelectClass(sel, job.sel)
		selections[job.si.ClassName] = sel
	}

	res, err := c.repo.MultiGet(ctx, query, selections, c.tenant)
	if err != nil {
		return errors.Wrap(err, "fetch job list")
	}

	// an object gone on its re-read must not keep the narrower earlier read
	for i, r := range res {
		if r.ID == "" {
			delete(c.store, query[i])
		}
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

// parseAndStore stores the results, then parses them for nested refs and
// builds the next level from them. Storing comes first because a nested level
// can read an object again with more properties, and that read must win.
func (c *Cacher) parseAndStore(ctx context.Context, res []search.Result) error {
	c.storeResults(res)

	// mark all current jobs as done, as we use the amount of incomplete jobs as
	// the exit condition for the recursion. Next up, we will start a nested
	// Build() call. If the Build call returns no new jobs, we are done and the
	// recursion stops. If it does return more jobs, we will enter a nested
	// iteration which will eventually come to this place again
	c.markAllJobsAsDone()

	err := c.Build(ctx, removeEmptyResults(res), nil, nil)
	if err != nil {
		return errors.Wrap(err, "build nested cache")
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

func (c *Cacher) storeResults(res search.Results) {
	for _, item := range res {
		c.store[multi.Identifier{
			ID:        item.ID.String(),
			ClassName: item.ClassName,
		}] = item
	}
}

func (c *Cacher) markAllJobsAsDone() {
	for i := range c.jobs {
		c.jobs[i].complete = true
	}
}

// mergeSelectProperties returns the union of a and b, merging the reference
// selections of a property both select. grew reports whether b selects anything
// a does not.
func mergeSelectProperties(a, b search.SelectProperties) (merged search.SelectProperties, grew bool) {
	return mergeByKey(a, b, func(p search.SelectProperty) string { return p.Name },
		func(x, y search.SelectProperty) (search.SelectProperty, bool) {
			var refsGrew bool
			x.Refs, refsGrew = mergeByKey(x.Refs, y.Refs,
				func(sc search.SelectClass) string { return sc.ClassName }, mergeSelectClass)
			return x, refsGrew
		})
}

func mergeSelectClass(x, y search.SelectClass) (search.SelectClass, bool) {
	var propsGrew, vectorsGrew bool
	x.RefProperties, propsGrew = mergeSelectProperties(x.RefProperties, y.RefProperties)
	x.AdditionalProperties, vectorsGrew = mergeVectors(x.AdditionalProperties, y.AdditionalProperties)
	return x, propsGrew || vectorsGrew
}

// mergeVectors adds the vectors b selects to a. The resolver renders only a
// referenced object's vectors and header fields, so vectors are the only
// additional properties its read needs to decode.
func mergeVectors(a, b additional.Properties) (merged additional.Properties, grew bool) {
	vectorGrew := b.Vector && !a.Vector
	a.Vector = a.Vector || b.Vector
	var namesGrew bool
	a.Vectors, namesGrew = mergeByKey(a.Vectors, b.Vectors,
		func(name string) string { return name },
		func(name, _ string) (string, bool) { return name, false })
	return a, vectorGrew || namesGrew
}

// mergeByKey returns the union of a and b, with merge combining two elements
// that share a key. It copies instead of writing to a or b, which alias the
// query's selections. grew reports whether b added anything to a.
func mergeByKey[T any](a, b []T, key func(T) string, merge func(x, y T) (T, bool)) (merged []T, grew bool) {
	if len(a) == 0 {
		return b, len(b) > 0
	}
	if len(b) == 0 || (len(b) <= len(a) && &a[0] == &b[0]) {
		return a, false
	}
	merged = a
	for _, elem := range b {
		k := key(elem)
		i := slices.IndexFunc(merged, func(m T) bool { return key(m) == k })
		if i < 0 {
			merged = append(slices.Clip(merged), elem)
			grew = true
			continue
		}
		if m, ok := merge(merged[i], elem); ok {
			merged = slices.Clone(merged)
			merged[i] = m
			grew = true
		}
	}
	return merged, grew
}
