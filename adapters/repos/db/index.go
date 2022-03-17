//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/aggregator"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/semi-technologies/weaviate/adapters/repos/db/sorter"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Index is the logical unit which contains all the data for one particular
// class. An index can be further broken up into self-contained units, called
// Shards, to allow for easy distribution across Nodes
type Index struct {
	classSearcher         inverted.ClassSearcher // to allow for nested by-references searches
	Shards                map[string]*Shard
	Config                IndexConfig
	vectorIndexUserConfig schema.VectorIndexConfig
	getSchema             schemaUC.SchemaGetter
	logger                logrus.FieldLogger
	remote                *sharding.RemoteIndex
	stopwords             *stopwords.Detector

	invertedIndexConfig     schema.InvertedIndexConfig
	invertedIndexConfigLock sync.Mutex
}

func (i *Index) ID() string {
	return indexID(i.Config.ClassName)
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

// NewIndex creates an index with the specified amount of shards, using only
// the shards that are local to a node
func NewIndex(ctx context.Context, config IndexConfig,
	shardState *sharding.State, invertedIndexConfig schema.InvertedIndexConfig,
	vectorIndexUserConfig schema.VectorIndexConfig, sg schemaUC.SchemaGetter,
	cs inverted.ClassSearcher, logger logrus.FieldLogger,
	nodeResolver nodeResolver, remoteClient sharding.RemoteIndexClient) (*Index, error) {
	sd, err := stopwords.NewDetectorFromConfig(invertedIndexConfig.Stopwords)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new index")
	}

	index := &Index{
		Config:                config,
		Shards:                map[string]*Shard{},
		getSchema:             sg,
		logger:                logger,
		classSearcher:         cs,
		vectorIndexUserConfig: vectorIndexUserConfig,
		invertedIndexConfig:   invertedIndexConfig,
		stopwords:             sd,
		remote: sharding.NewRemoteIndex(config.ClassName.String(), sg,
			nodeResolver, remoteClient),
	}

	if err := index.checkSingleShardMigration(shardState); err != nil {
		return nil, errors.Wrap(err, "migrating sharding state from previous version")
	}

	for _, shardName := range shardState.AllPhysicalShards() {

		if !shardState.IsShardLocal(shardName) {
			// do not create non-local shards
			continue
		}

		shard, err := NewShard(ctx, shardName, index)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %s of index %s", shardName, index.ID())
		}

		index.Shards[shardName] = shard
	}

	return index, nil
}

func (i *Index) addProperty(ctx context.Context, prop *models.Property) error {
	for name, shard := range i.Shards {
		if err := shard.addProperty(ctx, prop); err != nil {
			return errors.Wrapf(err, "add property to shard %q", name)
		}
	}

	return nil
}

func (i *Index) addUUIDProperty(ctx context.Context) error {
	for name, shard := range i.Shards {
		if err := shard.addIDProperty(ctx); err != nil {
			return errors.Wrapf(err, "add id property to shard %q", name)
		}
	}

	return nil
}

func (i *Index) updateVectorIndexConfig(ctx context.Context,
	updated schema.VectorIndexConfig) error {
	// an updated is not specific to one shard, but rather all
	for name, shard := range i.Shards {
		// At the moment, we don't do anything in an update that could fail, but
		// technically this should be part of some sort of a two-phase commit  or
		// have another way to rollback if we have updates that could potentially
		// fail in the future. For now that's not a realistic risk.
		if err := shard.updateVectorIndexConfig(ctx, updated); err != nil {
			return errors.Wrapf(err, "shard %s", name)
		}
	}

	return nil
}

func (i *Index) getInvertedIndexConfig() schema.InvertedIndexConfig {
	i.invertedIndexConfigLock.Lock()
	defer i.invertedIndexConfigLock.Unlock()

	return i.invertedIndexConfig
}

func (i *Index) updateInvertedIndexConfig(ctx context.Context,
	updated schema.InvertedIndexConfig) error {
	i.invertedIndexConfigLock.Lock()
	defer i.invertedIndexConfigLock.Unlock()

	i.invertedIndexConfig = updated

	return nil
}

type IndexConfig struct {
	RootPath                  string
	ClassName                 schema.ClassName
	QueryMaximumResults       int64
	DiskUseWarningPercentage  uint64
	DiskUseReadOnlyPercentage uint64
}

func indexID(class schema.ClassName) string {
	return strings.ToLower(string(class))
}

func (i *Index) shardFromUUID(in strfmt.UUID) (string, error) {
	uuid, err := uuid.Parse(in.String())
	if err != nil {
		return "", errors.Wrap(err, "parse id as uuid")
	}

	uuidBytes, _ := uuid.MarshalBinary() // cannot error

	return i.getSchema.ShardingState(i.Config.ClassName.String()).
		PhysicalShard(uuidBytes), nil
}

func (i *Index) putObject(ctx context.Context, object *storobj.Object) error {
	if i.Config.ClassName != object.Class() {
		return errors.Errorf("cannot import object of class %s into index of class %s",
			object.Class(), i.Config.ClassName)
	}

	shardName, err := i.shardFromUUID(object.ID())
	if err != nil {
		return err
	}

	localShard, ok := i.Shards[shardName]
	if !ok {
		// this must be a remote shard, try sending it remotely
		if err := i.remote.PutObject(ctx, shardName, object); err != nil {
			return errors.Wrap(err, "send to remote shard")
		}

		return nil
	}

	if err := localShard.putObject(ctx, object); err != nil {
		return errors.Wrapf(err, "shard %s", localShard.ID())
	}

	return nil
}

func (i *Index) IncomingPutObject(ctx context.Context, shardName string,
	object *storobj.Object) error {
	localShard, ok := i.Shards[shardName]
	if !ok {
		return errors.Errorf("shard %q does not exist locally", shardName)
	}

	// This is a bit hacky, the problem here is that storobj.Parse() currently
	// misses date fields as it has no way of knowing that a date-formatted
	// string was actually a date type. However, adding this functionality to
	// Parse() would break a lot of code, because it currently
	// schema-independent. To find out if a field is a date or date[], we need to
	// involve the schema, thus why we are doing it here. This was discovered as
	// part of https://github.com/semi-technologies/weaviate/issues/1775
	if err := i.parseDateFieldsInProps(object.Object.Properties); err != nil {
		return errors.Wrapf(err, "shard %s", localShard.ID())
	}

	if err := localShard.putObject(ctx, object); err != nil {
		return errors.Wrapf(err, "shard %s", localShard.ID())
	}

	return nil
}

// parseDateFieldsInProps checks the schema for the current class for which
// fields are date fields, then - if they are set - parses them accordingly.
// Works for both date and date[].
func (i *Index) parseDateFieldsInProps(props interface{}) error {
	if props == nil {
		return nil
	}

	propMap, ok := props.(map[string]interface{})
	if !ok {
		// don't know what to do with this
		return nil
	}

	schemaModel := i.getSchema.GetSchemaSkipAuth().Objects
	c, err := schema.GetClassByName(schemaModel, i.Config.ClassName.String())
	if err != nil {
		return err
	}

	for _, prop := range c.Properties {
		if prop.DataType[0] == string(schema.DataTypeDate) {
			raw, ok := propMap[prop.Name]
			if !ok {
				// prop is not set, nothing to do
				continue
			}

			parsed, err := parseAsStringToTime(raw)
			if err != nil {
				return errors.Wrapf(err, "time prop %q", prop.Name)
			}

			propMap[prop.Name] = parsed
		}

		if prop.DataType[0] == string(schema.DataTypeDateArray) {
			raw, ok := propMap[prop.Name]
			if !ok {
				// prop is not set, nothing to do
				continue
			}

			asSlice, ok := raw.([]string)
			if !ok {
				return errors.Errorf("parse as time array, expected []interface{} got %T",
					raw)
			}
			parsedSlice := make([]interface{}, len(asSlice))
			for j := range asSlice {
				parsed, err := parseAsStringToTime(interface{}(asSlice[j]))
				if err != nil {
					return errors.Wrapf(err, "time array prop %q at pos %d", prop.Name, j)
				}

				parsedSlice[j] = parsed
			}
			propMap[prop.Name] = parsedSlice

		}
	}

	return nil
}

func parseAsStringToTime(in interface{}) (time.Time, error) {
	var parsed time.Time
	var err error

	asString, ok := in.(string)
	if !ok {
		return parsed, errors.Errorf("parse as time, expected string got %T", in)
	}

	parsed, err = time.Parse(time.RFC3339, asString)
	if err != nil {
		return parsed, err
	}

	return parsed, nil
}

// return value []error gives the error for the index with the positions
// matching the inputs
func (i *Index) putObjectBatch(ctx context.Context,
	objects []*storobj.Object) []error {
	type objsAndPos struct {
		objects []*storobj.Object
		pos     []int
	}

	byShard := map[string]objsAndPos{}
	out := make([]error, len(objects))

	for pos, obj := range objects {
		shardName, err := i.shardFromUUID(obj.ID())
		if err != nil {
			out[pos] = err
			continue
		}

		group := byShard[shardName]
		group.objects = append(group.objects, obj)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	wg := &sync.WaitGroup{}

	for shardName, group := range byShard {
		wg.Add(1)
		go func(shardName string, group objsAndPos) {
			defer wg.Done()

			local := i.getSchema.
				ShardingState(i.Config.ClassName.String()).
				IsShardLocal(shardName)

			var errs []error
			if !local {
				errs = i.remote.BatchPutObjects(ctx, shardName, group.objects)
			} else {
				shard := i.Shards[shardName]
				errs = shard.putObjectBatch(ctx, group.objects)
			}
			for i, err := range errs {
				desiredPos := group.pos[i]
				out[desiredPos] = err
			}
		}(shardName, group)
	}

	wg.Wait()

	return out
}

func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}

	return out
}

func (i *Index) IncomingBatchPutObjects(ctx context.Context, shardName string,
	objects []*storobj.Object) []error {
	localShard, ok := i.Shards[shardName]
	if !ok {
		return duplicateErr(errors.Errorf("shard %q does not exist locally",
			shardName), len(objects))
	}

	// This is a bit hacky, the problem here is that storobj.Parse() currently
	// misses date fields as it has no way of knowing that a date-formatted
	// string was actually a date type. However, adding this functionality to
	// Parse() would break a lot of code, because it currently
	// schema-independent. To find out if a field is a date or date[], we need to
	// involve the schema, thus why we are doing it here. This was discovered as
	// part of https://github.com/semi-technologies/weaviate/issues/1775
	for j := range objects {
		if err := i.parseDateFieldsInProps(objects[j].Object.Properties); err != nil {
			return duplicateErr(errors.Wrapf(err, "shard %s", localShard.ID()),
				len(objects))
		}
	}

	return localShard.putObjectBatch(ctx, objects)
}

// return value map[int]error gives the error for the index as it received it
func (i *Index) addReferencesBatch(ctx context.Context,
	refs objects.BatchReferences) []error {
	type refsAndPos struct {
		refs objects.BatchReferences
		pos  []int
	}

	byShard := map[string]refsAndPos{}
	out := make([]error, len(refs))

	for pos, ref := range refs {
		shardName, err := i.shardFromUUID(ref.From.TargetID)
		if err != nil {
			out[pos] = err
			continue
		}

		group := byShard[shardName]
		group.refs = append(group.refs, ref)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	for shardName, group := range byShard {
		local := i.getSchema.
			ShardingState(i.Config.ClassName.String()).
			IsShardLocal(shardName)

		var errs []error
		if !local {
			errs = i.remote.BatchAddReferences(ctx, shardName, group.refs)
		} else {
			shard := i.Shards[shardName]
			errs = shard.addReferencesBatch(ctx, group.refs)
		}
		for i, err := range errs {
			desiredPos := group.pos[i]
			out[desiredPos] = err
		}
	}

	return out
}

func (i *Index) IncomingBatchAddReferences(ctx context.Context, shardName string,
	refs objects.BatchReferences) []error {
	localShard, ok := i.Shards[shardName]
	if !ok {
		return duplicateErr(errors.Errorf("shard %q does not exist locally",
			shardName), len(refs))
	}

	return localShard.addReferencesBatch(ctx, refs)
}

func (i *Index) objectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, additional additional.Properties) (*storobj.Object, error) {
	shardName, err := i.shardFromUUID(id)
	if err != nil {
		return nil, err
	}

	local := i.getSchema.
		ShardingState(i.Config.ClassName.String()).
		IsShardLocal(shardName)

	if !local {
		remote, err := i.remote.GetObject(ctx, shardName, id, props, additional)
		return remote, err
	}

	shard := i.Shards[shardName]
	obj, err := shard.objectByID(ctx, id, props, additional)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return obj, nil
}

func (i *Index) IncomingGetObject(ctx context.Context, shardName string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties) (*storobj.Object, error) {
	shard, ok := i.Shards[shardName]
	if !ok {
		return nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	obj, err := shard.objectByID(ctx, id, props, additional)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return obj, nil
}

func (i *Index) IncomingMultiGetObjects(ctx context.Context, shardName string,
	ids []strfmt.UUID) ([]*storobj.Object, error) {
	shard, ok := i.Shards[shardName]
	if !ok {
		return nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	objs, err := shard.multiObjectByID(ctx, wrapIDsInMulti(ids))
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return objs, nil
}

func (i *Index) multiObjectByID(ctx context.Context,
	query []multi.Identifier) ([]*storobj.Object, error) {
	type idsAndPos struct {
		ids []multi.Identifier
		pos []int
	}

	byShard := map[string]idsAndPos{}

	for pos, id := range query {
		shardName, err := i.shardFromUUID(strfmt.UUID(id.ID))
		if err != nil {
			return nil, err
		}

		group := byShard[shardName]
		group.ids = append(group.ids, id)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	out := make([]*storobj.Object, len(query))

	for shardName, group := range byShard {
		local := i.getSchema.
			ShardingState(i.Config.ClassName.String()).
			IsShardLocal(shardName)

		var objects []*storobj.Object
		var err error

		if local {
			shard := i.Shards[shardName]
			objects, err = shard.multiObjectByID(ctx, group.ids)
			if err != nil {
				return nil, errors.Wrapf(err, "shard %s", shard.ID())
			}
		} else {
			objects, err = i.remote.MultiGetObjects(ctx, shardName, extractIDsFromMulti(group.ids))
			if err != nil {
				return nil, errors.Wrapf(err, "remote shard %s", shardName)
			}
		}

		for i, obj := range objects {
			desiredPos := group.pos[i]
			out[desiredPos] = obj
		}
	}

	return out, nil
}

func extractIDsFromMulti(in []multi.Identifier) []strfmt.UUID {
	out := make([]strfmt.UUID, len(in))

	for i, id := range in {
		out[i] = strfmt.UUID(id.ID)
	}

	return out
}

func wrapIDsInMulti(in []strfmt.UUID) []multi.Identifier {
	out := make([]multi.Identifier, len(in))

	for i, id := range in {
		out[i] = multi.Identifier{ID: string(id)}
	}

	return out
}

func (i *Index) exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	shardName, err := i.shardFromUUID(id)
	if err != nil {
		return false, err
	}

	local := i.getSchema.
		ShardingState(i.Config.ClassName.String()).
		IsShardLocal(shardName)

	var ok bool
	if local {
		shard := i.Shards[shardName]
		ok, err = shard.exists(ctx, id)
	} else {
		ok, err = i.remote.Exists(ctx, shardName, id)
	}
	if err != nil {
		return false, errors.Wrapf(err, "shard %s", shardName)
	}

	return ok, nil
}

func (i *Index) IncomingExists(ctx context.Context, shardName string,
	id strfmt.UUID) (bool, error) {
	shard, ok := i.Shards[shardName]
	if !ok {
		return false, errors.Errorf("shard %q does not exist locally", shardName)
	}

	ok, err := shard.exists(ctx, id)
	if err != nil {
		return ok, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return ok, nil
}

func (i *Index) objectSearch(ctx context.Context, limit int, sort []filters.Sort,
	filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking,
	additional additional.Properties) ([]*storobj.Object, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()

	var err error
	// multiSearch := len(shardNames) > 1

	outObjects := make([]*storobj.Object, 0, len(shardNames)*limit)
	outScores := make([]float32, 0, len(shardNames)*limit)
	for _, shardName := range shardNames {
		local := i.getSchema.
			ShardingState(i.Config.ClassName.String()).
			IsShardLocal(shardName)

		var objs []*storobj.Object
		var scores []float32

		if local {
			shard := i.Shards[shardName]
			objs, scores, err = shard.objectSearch(ctx, limit, filters, keywordRanking, sort, additional)
			if err != nil {
				return nil, errors.Wrapf(err, "shard %s", shard.ID())
			}

		} else {
			objs, scores, err = i.remote.SearchShard(
				ctx, shardName, nil, limit, filters, keywordRanking, sort, additional)
			if err != nil {
				return nil, errors.Wrapf(err, "remote shard %s", shardName)
			}
			// multiSearch = true
		}
		outObjects = append(outObjects, objs...)
		outScores = append(outScores, scores...)
	}

	if keywordRanking != nil {
		outObjects, _, err = i.sort(outObjects, outScores, sort, keywordRanking != nil)
		if err != nil {
			return nil, errors.Wrap(err, "sort")
		}
	}

	if len(outObjects) > limit {
		outObjects = outObjects[:limit]
	}

	return outObjects, nil
}

func (i *Index) sortRankedResults(res *inverted.RankedResults) *inverted.RankedResults {
	sort.Sort(res)
	return res
}

func (i *Index) sort(objects []*storobj.Object, scores []float32,
	sort []filters.Sort, keywordRanking bool) ([]*storobj.Object, []float32, error) {
	return sorter.New(i.getSchema.GetSchemaSkipAuth()).
		Sort(objects, scores, -1, sort, keywordRanking, false)
}

func (i *Index) objectVectorSearch(ctx context.Context, searchVector []float32,
	dist float32, limit int, filters *filters.LocalFilter,
	sort []filters.Sort, additional additional.Properties) ([]*storobj.Object, []float32, error) {
	shardNames := i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards()

	errgrp := &errgroup.Group{}
	m := &sync.Mutex{}

	// a limit of -1 is used to signal a search by distance. if that is
	// the case we have to adjust how we calculate the outpout capacity
	var shardCap int
	if limit < 0 {
		shardCap = len(shardNames) * hnsw.DefaultSearchByDistInitialLimit
	} else {
		shardCap = len(shardNames) * limit
	}

	var err error
	multiSearch := len(shardNames) > 1

	out := make([]*storobj.Object, 0, shardCap)
	dists := make([]float32, 0, shardCap)
	for _, shardName := range shardNames {
		shardName := shardName
		errgrp.Go(func() error {
			local := i.getSchema.
				ShardingState(i.Config.ClassName.String()).
				IsShardLocal(shardName)

			var res []*storobj.Object
			var resDists []float32

			if local {
				shard := i.Shards[shardName]
				// TODO: add sort here
				res, resDists, err = shard.objectVectorSearch(
					ctx, searchVector, dist, limit, filters, additional)
				if err != nil {
					return errors.Wrapf(err, "shard %s", shard.ID())
				}
			} else {
				res, resDists, err = i.remote.SearchShard(
					ctx, shardName, searchVector, limit, filters, nil, sort, additional)
				if err != nil {
					return errors.Wrapf(err, "remote shard %s", shardName)
				}
				multiSearch = true
			}

			m.Lock()
			out = append(out, res...)
			dists = append(dists, resDists...)
			m.Unlock()

			return nil
		})
	}

	if err := errgrp.Wait(); err != nil {
		return nil, nil, err
	}

	if multiSearch {
		out, dists, err = sorter.New(i.getSchema.GetSchemaSkipAuth()).Sort(out, dists, limit, nil, false, true)
		if err != nil {
			return nil, nil, err
		}
	}

	// out, dists, err = sorter.New(i.getSchema.GetSchemaSkipAuth()).Sort(out, dists, limit, nil, false, true)
	// if err != nil {
	// 	return nil, nil, err
	// }

	return out, dists, nil
}

func (i *Index) IncomingSearch(ctx context.Context, shardName string,
	searchVector []float32, distance float32, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
	additional additional.Properties) ([]*storobj.Object, []float32, error) {
	shard, ok := i.Shards[shardName]
	if !ok {
		return nil, nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	if searchVector == nil {
		res, scores, err := shard.objectSearch(ctx, limit, filters, keywordRanking, sort, additional)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
		}

		return res, scores, nil
	}

	res, resDists, err := shard.objectVectorSearch(
		ctx, searchVector, distance, limit, filters, additional)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, resDists, nil
}

func (i *Index) deleteObject(ctx context.Context, id strfmt.UUID) error {
	shardName, err := i.shardFromUUID(id)
	if err != nil {
		return err
	}

	local := i.getSchema.
		ShardingState(i.Config.ClassName.String()).
		IsShardLocal(shardName)

	if local {
		shard := i.Shards[shardName]
		err = shard.deleteObject(ctx, id)
	} else {
		err = i.remote.DeleteObject(ctx, shardName, id)
	}
	if err != nil {
		return errors.Wrapf(err, "shard %s", shardName)
	}

	return nil
}

func (i *Index) IncomingDeleteObject(ctx context.Context, shardName string,
	id strfmt.UUID) error {
	shard, ok := i.Shards[shardName]
	if !ok {
		return errors.Errorf("shard %q does not exist locally", shardName)
	}

	err := shard.deleteObject(ctx, id)
	if err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) mergeObject(ctx context.Context, merge objects.MergeDocument) error {
	shardName, err := i.shardFromUUID(merge.ID)
	if err != nil {
		return err
	}

	local := i.getSchema.
		ShardingState(i.Config.ClassName.String()).
		IsShardLocal(shardName)

	if local {
		shard := i.Shards[shardName]
		err = shard.mergeObject(ctx, merge)
	} else {
		err = i.remote.MergeObject(ctx, shardName, merge)
	}
	if err != nil {
		return errors.Wrapf(err, "shard %s", shardName)
	}

	return nil
}

func (i *Index) IncomingMergeObject(ctx context.Context, shardName string,
	mergeDoc objects.MergeDocument) error {
	shard, ok := i.Shards[shardName]
	if !ok {
		return errors.Errorf("shard %q does not exist locally", shardName)
	}

	err := shard.mergeObject(ctx, mergeDoc)
	if err != nil {
		return errors.Wrapf(err, "shard %s", shard.ID())
	}

	return nil
}

func (i *Index) aggregate(ctx context.Context,
	params aggregation.Params) (*aggregation.Result, error) {
	shardState := i.getSchema.ShardingState(i.Config.ClassName.String())
	shardNames := shardState.AllPhysicalShards()

	results := make([]*aggregation.Result, len(shardNames))
	for j, shardName := range shardNames {
		local := shardState.IsShardLocal(shardName)

		var err error
		var res *aggregation.Result
		if !local {
			res, err = i.remote.Aggregate(ctx, shardName, params)
		} else {
			shard := i.Shards[shardName]
			res, err = shard.aggregate(ctx, params)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		results[j] = res
	}

	if len(shardNames) > 1 {
		return aggregator.NewShardCombiner().Do(results), nil
	}

	return results[0], nil
}

func (i *Index) IncomingAggregate(ctx context.Context, shardName string,
	params aggregation.Params) (*aggregation.Result, error) {
	shard, ok := i.Shards[shardName]
	if !ok {
		return nil, errors.Errorf("shard %q does not exist locally", shardName)
	}

	res, err := shard.aggregate(ctx, params)
	if err != nil {
		return nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, nil
}

func (i *Index) drop() error {
	for _, name := range i.getSchema.ShardingState(i.Config.ClassName.String()).
		AllPhysicalShards() {
		shard, ok := i.Shards[name]
		if !ok {
			// skip non-local, but do delete everything that exists - even if it
			// shouldn't
			continue
		}
		err := shard.drop(false)
		if err != nil {
			return errors.Wrapf(err, "delete shard %s", shard.ID())
		}
	}

	return nil
}

func (i *Index) Shutdown(ctx context.Context) error {
	for id, shard := range i.Shards {
		if err := shard.shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown shard %q", id)
		}
	}

	return nil
}

func (i *Index) getShardsStatus() map[string]string {
	shardsStatus := make(map[string]string)

	for _, shard := range i.Shards {
		shardsStatus[shard.name] = shard.getStatus().String()
	}

	return shardsStatus
}

func (i *Index) updateShardStatus(shardName, targetStatus string) error {
	shard, ok := i.Shards[shardName]
	if !ok {
		return errors.Errorf("shard %s does not exist locally", shardName)
	}

	return shard.updateStatus(targetStatus)
}

func (i *Index) notifyReady() {
	for _, shd := range i.Shards {
		shd.notifyReady()
	}
}
