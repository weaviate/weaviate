//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clusterapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	reposdb "github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

type indices struct {
	shards shards
	db     db
	auth   auth
	// maintenanceModeEnabled is an experimental feature to allow the system to be
	// put into a maintenance mode where all indices requests just return a 418
	maintenanceModeEnabled     func() bool
	regexpObjects              *regexp.Regexp
	regexpObjectsOverwrite     *regexp.Regexp
	regexObjectsDigest         *regexp.Regexp
	regexObjectsDigestsInRange *regexp.Regexp
	regexObjectsHashTreeLevel  *regexp.Regexp
	regexpObjectsSearch        *regexp.Regexp
	regexpObjectsFind          *regexp.Regexp

	regexpObjectsAggregations *regexp.Regexp
	regexpObject              *regexp.Regexp
	regexpReferences          *regexp.Regexp
	regexpShardsQueueSize     *regexp.Regexp
	regexpShardsStatus        *regexp.Regexp
	regexpShardFiles          *regexp.Regexp
	regexpShardFileMetadata   *regexp.Regexp
	regexpShard               *regexp.Regexp
	regexpShardReinit         *regexp.Regexp

	regexpPauseFileActivity  *regexp.Regexp
	regexpResumeFileActivity *regexp.Regexp
	regexpListFiles          *regexp.Regexp

	regexpAsyncReplicationTargetNode *regexp.Regexp

	logger logrus.FieldLogger
}

const (
	cl = entschema.ClassNameRegexCore
	sh = entschema.ShardNameRegexCore
	ob = `[A-Za-z0-9_+-]+`
	l  = "[0-9]+"

	urlPatternObjects = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects`
	urlPatternObjectsOverwrite = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects:overwrite`
	urlPatternObjectsDigest = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects:digest`
	urlPatternObjectsDigestsInRange = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects:digestsInRange`
	urlPatternHashTreeLevel = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects\/hashtree\/(` + l + `)`
	urlPatternObjectsSearch = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects\/_search`
	urlPatternObjectsFind = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects\/_find`
	urlPatternObjectsAggregations = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects\/_aggregations`
	urlPatternObject = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects\/(` + ob + `)`
	urlPatternReferences = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/references`
	urlPatternShardsQueueSize = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/queuesize`
	urlPatternShardsStatus = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/status`
	urlPatternShardFiles = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/files/(.*)`
	urlPatternShardFileMetadata = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/files:metadata/(.*)`
	urlPatternShard = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)$`
	urlPatternShardReinit = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `):reinit`
	urlPatternPauseFileActivity = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/background:pause`
	urlPatternResumeFileActivity = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/background:resume`
	urlPatternListFiles = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/background:list`
	urlPatternAsyncReplicationTargetNode = `\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/async-replication-target-node`
)

type shards interface {
	PutObject(ctx context.Context, indexName, shardName string,
		obj *storobj.Object, schemaVersion uint64) error
	BatchPutObjects(ctx context.Context, indexName, shardName string,
		objs []*storobj.Object, schemaVersion uint64) []error
	BatchAddReferences(ctx context.Context, indexName, shardName string,
		refs objects.BatchReferences, schemaVersion uint64) []error
	GetObject(ctx context.Context, indexName, shardName string,
		id strfmt.UUID, selectProperties search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, indexName, shardName string,
		id strfmt.UUID) (bool, error)
	DeleteObject(ctx context.Context, indexName, shardName string,
		id strfmt.UUID, deletionTime time.Time, schemaVersion uint64) error
	MergeObject(ctx context.Context, indexName, shardName string,
		mergeDoc objects.MergeDocument, schemaVersion uint64) error
	MultiGetObjects(ctx context.Context, indexName, shardName string,
		id []strfmt.UUID) ([]*storobj.Object, error)
	Search(ctx context.Context, indexName, shardName string,
		vectors []models.Vector, targetVectors []string, distance float32, limit int,
		filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking,
		sort []filters.Sort, cursor *filters.Cursor, groupBy *searchparams.GroupBy,
		additional additional.Properties, targetCombination *dto.TargetCombination, properties []string,
	) ([]*storobj.Object, []float32, error)
	Aggregate(ctx context.Context, indexName, shardName string,
		params aggregation.Params) (*aggregation.Result, error)
	FindUUIDs(ctx context.Context, indexName, shardName string,
		filters *filters.LocalFilter) ([]strfmt.UUID, error)
	DeleteObjectBatch(ctx context.Context, indexName, shardName string,
		uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) objects.BatchSimpleObjects
	GetShardQueueSize(ctx context.Context, indexName, shardName string) (int64, error)
	GetShardStatus(ctx context.Context, indexName, shardName string) (string, error)
	UpdateShardStatus(ctx context.Context, indexName, shardName,
		targetStatus string, schemaVersion uint64) error

	// Replication-specific
	OverwriteObjects(ctx context.Context, indexName, shardName string,
		vobjects []*objects.VObject) ([]types.RepairResponse, error)
	DigestObjects(ctx context.Context, indexName, shardName string,
		ids []strfmt.UUID) (result []types.RepairResponse, err error)
	DigestObjectsInRange(ctx context.Context, indexName, shardName string,
		initialUUID, finalUUID strfmt.UUID, limit int) (result []types.RepairResponse, err error)
	HashTreeLevel(ctx context.Context, indexName, shardName string,
		level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)

	// Scale-out Replication POC
	FilePutter(ctx context.Context, indexName, shardName,
		filePath string) (io.WriteCloser, error)
	CreateShard(ctx context.Context, indexName, shardName string) error
	ReInitShard(ctx context.Context, indexName, shardName string) error
	// PauseFileActivity See adapters/clients.RemoteIndex.PauseFileActivity
	PauseFileActivity(ctx context.Context, indexName, shardName string, schemaVersion uint64) error
	// ResumeFileActivity See adapters/clients.RemoteIndex.ResumeFileActivity
	ResumeFileActivity(ctx context.Context, indexName, shardName string) error
	// ListFiles See adapters/clients.RemoteIndex.ListFiles
	ListFiles(ctx context.Context, indexName, shardName string) ([]string, error)
	// GetFileMetadata See adapters/clients.RemoteIndex.GetFileMetadata
	GetFileMetadata(ctx context.Context, indexName, shardName,
		relativeFilePath string) (file.FileMetadata, error)
	// GetFile See adapters/clients.RemoteIndex.GetFile
	GetFile(ctx context.Context, indexName, shardName,
		relativeFilePath string) (io.ReadCloser, error)
	// AddAsyncReplicationTargetNode See adapters/clients.RemoteIndex.AddAsyncReplicationTargetNode
	AddAsyncReplicationTargetNode(ctx context.Context, indexName, shardName string,
		targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error
	// RemoveAsyncReplicationTargetNode See adapters/clients.RemoteIndex.RemoveAsyncReplicationTargetNode
	RemoveAsyncReplicationTargetNode(ctx context.Context, indexName, shardName string,
		targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error
}

type db interface {
	StartupComplete() bool
}

func NewIndices(shards shards, db db, auth auth, maintenanceModeEnabled func() bool, logger logrus.FieldLogger) *indices {
	return &indices{
		regexpObjects:              regexp.MustCompile(urlPatternObjects),
		regexpObjectsOverwrite:     regexp.MustCompile(urlPatternObjectsOverwrite),
		regexObjectsDigest:         regexp.MustCompile(urlPatternObjectsDigest),
		regexObjectsDigestsInRange: regexp.MustCompile(urlPatternObjectsDigestsInRange),
		regexObjectsHashTreeLevel:  regexp.MustCompile(urlPatternHashTreeLevel),
		regexpObjectsSearch:        regexp.MustCompile(urlPatternObjectsSearch),
		regexpObjectsFind:          regexp.MustCompile(urlPatternObjectsFind),

		regexpObjectsAggregations:        regexp.MustCompile(urlPatternObjectsAggregations),
		regexpObject:                     regexp.MustCompile(urlPatternObject),
		regexpReferences:                 regexp.MustCompile(urlPatternReferences),
		regexpShardsQueueSize:            regexp.MustCompile(urlPatternShardsQueueSize),
		regexpShardsStatus:               regexp.MustCompile(urlPatternShardsStatus),
		regexpShardFiles:                 regexp.MustCompile(urlPatternShardFiles),
		regexpShardFileMetadata:          regexp.MustCompile(urlPatternShardFileMetadata),
		regexpShard:                      regexp.MustCompile(urlPatternShard),
		regexpShardReinit:                regexp.MustCompile(urlPatternShardReinit),
		regexpPauseFileActivity:          regexp.MustCompile(urlPatternPauseFileActivity),
		regexpResumeFileActivity:         regexp.MustCompile(urlPatternResumeFileActivity),
		regexpListFiles:                  regexp.MustCompile(urlPatternListFiles),
		regexpAsyncReplicationTargetNode: regexp.MustCompile(urlPatternAsyncReplicationTargetNode),
		shards:                           shards,
		db:                               db,
		auth:                             auth,
		maintenanceModeEnabled:           maintenanceModeEnabled,
		logger:                           logger,
	}
}

func (i *indices) Indices() http.Handler {
	return i.auth.handleFunc(i.indicesHandler())
}

func (i *indices) indicesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if i.maintenanceModeEnabled() {
			http.Error(w, "418 Maintenance mode", http.StatusTeapot)
			return
		}
		// NOTE if you update any of these handler methods/paths, also update the indices_test.go
		// TestMaintenanceModeIndices test to include the new methods/paths.
		switch {
		case i.regexpObjectsSearch.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postSearchObjects().ServeHTTP(w, r)
			return
		case i.regexpObjectsFind.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postFindUUIDs().ServeHTTP(w, r)
			return
		case i.regexpObjectsAggregations.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postAggregateObjects().ServeHTTP(w, r)
			return
		case i.regexpObjectsOverwrite.MatchString(path):
			if r.Method != http.MethodPut {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.putOverwriteObjects().ServeHTTP(w, r)
		case i.regexObjectsDigest.MatchString(path):
			if r.Method != http.MethodGet {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.getObjectsDigest().ServeHTTP(w, r)
		case i.regexObjectsDigestsInRange.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			}

			i.getObjectsDigestsInRange().ServeHTTP(w, r)
		case i.regexObjectsHashTreeLevel.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			}

			i.getHashTreeLevel().ServeHTTP(w, r)
		case i.regexpObject.MatchString(path):
			if r.Method == http.MethodGet {
				i.getObject().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodDelete {
				i.deleteObject().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodPatch {
				i.mergeObject().ServeHTTP(w, r)
				return
			}

			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpObjects.MatchString(path):
			if r.Method == http.MethodGet {
				i.getObjectsMulti().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodPost {
				i.postObject().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodDelete {
				i.deleteObjects().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpReferences.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postReferences().ServeHTTP(w, r)
			return
		case i.regexpShardsQueueSize.MatchString(path):
			if r.Method == http.MethodGet {
				i.getGetShardQueueSize().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case i.regexpShardsStatus.MatchString(path):
			if r.Method == http.MethodGet {
				i.getGetShardStatus().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodPost {
				i.postUpdateShardStatus().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpShardFiles.MatchString(path):
			if r.Method == http.MethodPost {
				i.postShardFile().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodGet {
				i.getShardFile().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpShardFileMetadata.MatchString(path):
			if r.Method == http.MethodGet {
				i.getShardFileMetadata().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpShard.MatchString(path):
			if r.Method == http.MethodPost {
				i.postShard().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case i.regexpShardReinit.MatchString(path):
			if r.Method == http.MethodPut {
				i.putShardReinit().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case i.regexpPauseFileActivity.MatchString(path):
			if r.Method == http.MethodPost {
				i.postPauseFileActivity().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case i.regexpResumeFileActivity.MatchString(path):
			if r.Method == http.MethodPost {
				i.postResumeFileActivity().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case i.regexpListFiles.MatchString(path):
			if r.Method == http.MethodPost {
				i.postListFiles().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case i.regexpAsyncReplicationTargetNode.MatchString(path):
			if r.Method == http.MethodPost {
				i.postAddAsyncReplicationTargetNode().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodDelete {
				i.deleteAsyncReplicationTargetNode().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}
}

func (i *indices) postObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		ct := r.Header.Get("content-type")

		switch ct {
		case IndicesPayloads.ObjectList.MIME():
			i.postObjectBatch(w, r, index, shard)
			return

		case IndicesPayloads.SingleObject.MIME():
			i.postObjectSingle(w, r, index, shard)
			return

		default:
			http.Error(w, "415 Unsupported Media Type", http.StatusUnsupportedMediaType)
			return
		}
	})
}

func (i *indices) postObjectSingle(w http.ResponseWriter, r *http.Request,
	index, shard string,
) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	obj, err := IndicesPayloads.SingleObject.Unmarshal(bodyBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := i.shards.PutObject(r.Context(), index, shard, obj, schemaVersion); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (i *indices) postObjectBatch(w http.ResponseWriter, r *http.Request,
	index, shard string,
) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	objs, err := IndicesPayloads.ObjectList.Unmarshal(bodyBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	errs := i.shards.BatchPutObjects(r.Context(), index, shard, objs, schemaVersion)
	if len(errs) > 0 && errors.Is(errs[0], reposdb.ErrShardNotFound) {
		http.Error(w, errs[0].Error(), http.StatusInternalServerError)
		return
	}
	errsJSON, err := IndicesPayloads.ErrorList.Marshal(errs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	IndicesPayloads.ErrorList.SetContentTypeHeader(w)
	w.Write(errsJSON)
}

func (i *indices) getObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		defer r.Body.Close()

		if r.URL.Query().Get("check_exists") != "" {
			i.checkExists(w, r, index, shard, id)
			return
		}

		additionalEncoded := r.URL.Query().Get("additional")
		if additionalEncoded == "" {
			http.Error(w, "missing required url param 'additional'",
				http.StatusBadRequest)
			return
		}

		additionalBytes, err := base64.StdEncoding.DecodeString(additionalEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'additional' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		selectPropertiesEncoded := r.URL.Query().Get("selectProperties")
		if selectPropertiesEncoded == "" {
			http.Error(w, "missing required url param 'selectProperties'",
				http.StatusBadRequest)
			return
		}

		selectPropertiesBytes, err := base64.StdEncoding.
			DecodeString(selectPropertiesEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'selectProperties' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var additional additional.Properties
		if err := json.Unmarshal(additionalBytes, &additional); err != nil {
			http.Error(w, "unmarshal 'additional' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var selectProperties search.SelectProperties
		if err := json.Unmarshal(selectPropertiesBytes, &selectProperties); err != nil {
			http.Error(w, "unmarshal 'selectProperties' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}
		if !i.db.StartupComplete() {
			http.Error(w, "startup is not complete", http.StatusServiceUnavailable)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"shard":  shard,
			"action": "GetObject",
		}).Debug("getting object ...")

		obj, err := i.shards.GetObject(r.Context(), index, shard, strfmt.UUID(id),
			selectProperties, additional)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if obj == nil {
			// this is a legitimate case - the requested ID doesn't exist, don't try
			// to marshal anything
			w.WriteHeader(http.StatusNotFound)
			return
		}

		objBytes, err := IndicesPayloads.SingleObject.Marshal(obj)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.SingleObject.SetContentTypeHeader(w)
		w.Write(objBytes)
	})
}

func (i *indices) checkExists(w http.ResponseWriter, r *http.Request,
	index, shard, id string,
) {
	i.logger.WithFields(logrus.Fields{
		"shard":  shard,
		"action": "checkExists",
	}).Debug("checking if shard exists ...")
	ok, err := i.shards.Exists(r.Context(), index, shard, strfmt.UUID(id))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if ok {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (i *indices) deleteObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObject.FindStringSubmatch(r.URL.Path)
		if len(args) < 4 || len(args) > 5 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		var deletionTime time.Time

		if len(args) == 5 {
			deletionTimeUnixMilli, err := strconv.ParseInt(args[4], 10, 64)
			if err != nil {
				http.Error(w, "invalid URI", http.StatusBadRequest)
			}
			deletionTime = time.UnixMilli(deletionTimeUnixMilli)
		}

		defer r.Body.Close()

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = i.shards.DeleteObject(r.Context(), index, shard, strfmt.UUID(id), deletionTime, schemaVersion)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *indices) mergeObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, _ := args[1], args[2], args[3]

		defer r.Body.Close()
		ct, ok := IndicesPayloads.MergeDoc.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		mergeDoc, err := IndicesPayloads.MergeDoc.Unmarshal(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err = i.shards.MergeObject(r.Context(), index, shard, mergeDoc, schemaVersion); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *indices) getObjectsMulti() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, fmt.Sprintf("invalid URI: %s", r.URL.Path),
				http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		idsEncoded := r.URL.Query().Get("ids")
		if idsEncoded == "" {
			http.Error(w, "missing required url param 'ids'",
				http.StatusBadRequest)
			return
		}

		idsBytes, err := base64.StdEncoding.DecodeString(idsEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'ids' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var ids []strfmt.UUID
		if err := json.Unmarshal(idsBytes, &ids); err != nil {
			http.Error(w, "unmarshal 'ids' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"shard":  shard,
			"action": "MultiGetObjects",
		}).Debug("get multiple objects ...")

		objs, err := i.shards.MultiGetObjects(r.Context(), index, shard, ids)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		objsBytes, err := IndicesPayloads.ObjectList.Marshal(objs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.ObjectList.SetContentTypeHeader(w)
		w.Write(objsBytes)
	})
}

func (i *indices) postSearchObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjectsSearch.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.SearchParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		vector, targetVector, certainty, limit, filters, keywordRanking, sort, cursor, groupBy, additional, targetCombination, props, err := IndicesPayloads.SearchParams.
			Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal search params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"shard":  shard,
			"action": "Search",
		}).Debug("searching ...")

		results, dists, err := i.shards.Search(r.Context(), index, shard,
			vector, targetVector, certainty, limit, filters, keywordRanking, sort, cursor, groupBy, additional, targetCombination, props)
		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resBytes, err := IndicesPayloads.SearchResults.Marshal(results, dists)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.SearchResults.SetContentTypeHeader(w)
		w.Write(resBytes)
	})
}

func (i *indices) postReferences() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpReferences.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.ReferenceList.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		refs, err := IndicesPayloads.ReferenceList.Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		errs := i.shards.BatchAddReferences(r.Context(), index, shard, refs, schemaVersion)
		errsJSON, err := IndicesPayloads.ErrorList.Marshal(errs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.ErrorList.SetContentTypeHeader(w)
		w.Write(errsJSON)
	})
}

func (i *indices) postAggregateObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjectsAggregations.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.AggregationParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		params, err := IndicesPayloads.AggregationParams.Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"shard":  shard,
			"action": "Aggregate",
		}).Debug("aggregate ...")

		aggRes, err := i.shards.Aggregate(r.Context(), index, shard, params)

		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		aggResBytes, err := IndicesPayloads.AggregationResult.Marshal(aggRes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.AggregationResult.SetContentTypeHeader(w)
		w.Write(aggResBytes)
	})
}

func (i *indices) postFindUUIDs() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjectsFind.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.FindUUIDsParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		filters, err := IndicesPayloads.FindUUIDsParams.
			Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal find doc ids params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"shard":  shard,
			"action": "FindUUIDs",
		}).Debug("find UUIDs ...")

		results, err := i.shards.FindUUIDs(r.Context(), index, shard, filters)

		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resBytes, err := IndicesPayloads.FindUUIDsResults.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.FindUUIDsResults.SetContentTypeHeader(w)
		w.Write(resBytes)
	})
}

func (i *indices) putOverwriteObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjectsOverwrite.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.VersionedObjectList.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		vobjs, err := IndicesPayloads.VersionedObjectList.Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal overwrite objects params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results, err := i.shards.OverwriteObjects(r.Context(), index, shard, vobjs)
		if err != nil {
			http.Error(w, "overwrite objects: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(resBytes)
	})
}

func (i *indices) getObjectsDigest() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexObjectsDigest.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var ids []strfmt.UUID
		if err := json.Unmarshal(reqPayload, &ids); err != nil {
			http.Error(w, "unmarshal digest objects params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"shard":  shard,
			"action": "DigestObjects",
		}).Debug("digest objects ...")

		results, err := i.shards.DigestObjects(r.Context(), index, shard, ids)
		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		if err != nil {
			http.Error(w, "digest objects: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(resBytes)
	})
}

func (i *indices) getObjectsDigestsInRange() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexObjectsDigestsInRange.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var rangeReq replica.DigestObjectsInRangeReq
		if err := json.Unmarshal(reqPayload, &rangeReq); err != nil {
			http.Error(w, "unmarshal digest objects in token range params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		digests, err := i.shards.DigestObjectsInRange(r.Context(),
			index, shard, rangeReq.InitialUUID, rangeReq.FinalUUID, rangeReq.Limit)
		if err != nil {
			http.Error(w, "digest objects in range: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(replica.DigestObjectsInRangeResp{
			Digests: digests,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(resBytes)
	})
}

func (i *indices) getHashTreeLevel() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexObjectsHashTreeLevel.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, level := args[1], args[2], args[3]

		l, err := strconv.Atoi(level)
		if err != nil {
			http.Error(w, "unmarshal hashtree level params: "+err.Error(), http.StatusInternalServerError)
			return
		}

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var discriminant hashtree.Bitset
		if err := discriminant.Unmarshal(reqPayload); err != nil {
			http.Error(w, "unmarshal hashtree level params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results, err := i.shards.HashTreeLevel(r.Context(), index, shard, l, &discriminant)
		if err != nil {
			http.Error(w, "hashtree level: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(resBytes)
	})
}

func (i *indices) deleteObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.BatchDeleteParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		uuids, deletionTimeUnix, dryRun, err := IndicesPayloads.BatchDeleteParams.
			Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal find doc ids params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		results := i.shards.DeleteObjectBatch(r.Context(), index, shard, uuids, deletionTimeUnix, dryRun, schemaVersion)

		resBytes, err := IndicesPayloads.BatchDeleteResults.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.BatchDeleteResults.SetContentTypeHeader(w)
		w.Write(resBytes)
	})
}

func (i *indices) getGetShardQueueSize() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardsQueueSize.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		i.logger.WithFields(logrus.Fields{
			"shard":  shard,
			"action": "GetShardQueueSize",
		}).Debug("getting shard queue size ...")

		size, err := i.shards.GetShardQueueSize(r.Context(), index, shard)
		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		sizeBytes, err := IndicesPayloads.GetShardQueueSizeResults.Marshal(size)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.GetShardQueueSizeResults.SetContentTypeHeader(w)
		w.Write(sizeBytes)
	})
}

func (i *indices) getGetShardStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardsStatus.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		i.logger.WithFields(logrus.Fields{
			"shard":  shard,
			"action": "GetShardStatus",
		}).Debug("getting shard status ...")

		status, err := i.shards.GetShardStatus(r.Context(), index, shard)
		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		statusBytes, err := IndicesPayloads.GetShardStatusResults.Marshal(status)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.GetShardStatusResults.SetContentTypeHeader(w)
		w.Write(statusBytes)
	})
}

func (i *indices) postUpdateShardStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardsStatus.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.UpdateShardStatusParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		targetStatus, err := IndicesPayloads.UpdateShardStatusParams.
			Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal find doc ids params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = i.shards.UpdateShardStatus(r.Context(), index, shard, targetStatus, schemaVersion)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func (i *indices) postShardFile() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardFiles.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, filename := args[1], args[2], args[3]

		ct, ok := IndicesPayloads.ShardFiles.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		fp, err := i.shards.FilePutter(r.Context(), index, shard, filename)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		defer fp.Close()
		n, err := io.Copy(fp, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"index":    index,
			"shard":    shard,
			"fileName": filename,
			"n":        n,
		}).Debug()

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *indices) postShard() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShard.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		err := i.shards.CreateShard(r.Context(), index, shard)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	})
}

func (i *indices) putShardReinit() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardReinit.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		err := i.shards.ReInitShard(r.Context(), index, shard)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *indices) getShardFileMetadata() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardFileMetadata.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		indexName, shardName, relativeFilePath := args[1], args[2], args[3]

		ct, ok := IndicesPayloads.ShardFiles.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		md, err := i.shards.GetFileMetadata(r.Context(), indexName, shardName, relativeFilePath)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(md)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(resBytes)
		w.WriteHeader(http.StatusOK)
	})
}

func (i *indices) getShardFile() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardFiles.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		indexName, shardName, relativeFilePath := args[1], args[2], args[3]

		ct, ok := IndicesPayloads.ShardFiles.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		reader, err := i.shards.GetFile(r.Context(), indexName, shardName, relativeFilePath)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		n, err := io.Copy(w, reader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"action":        "replica_movement",
			"index":         indexName,
			"shard":         shardName,
			"fileName":      relativeFilePath,
			"fileSizeBytes": n,
		}).Debug("Copied replica file")

		w.WriteHeader(http.StatusOK)
	})
}

func (i *indices) postPauseFileActivity() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpPauseFileActivity.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		indexName, shardName := args[1], args[2]

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = i.shards.PauseFileActivity(r.Context(), indexName, shardName, schemaVersion)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"action": "replica_movement",
			"index":  indexName,
			"shard":  shardName,
		}).Debug("Paused replica file activity")

		w.WriteHeader(http.StatusOK)
	})
}

func (i *indices) postResumeFileActivity() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpPauseFileActivity.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		indexName, shardName := args[1], args[2]

		err := i.shards.ResumeFileActivity(r.Context(), indexName, shardName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"action": "replica_movement",
			"index":  indexName,
			"shard":  shardName,
		}).Debug("Resumed replica file activity")

		w.WriteHeader(http.StatusOK)
	})
}

func (i *indices) postListFiles() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpListFiles.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		indexName, shardName := args[1], args[2]

		relativeFilePaths, err := i.shards.ListFiles(r.Context(), indexName, shardName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(relativeFilePaths)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		i.logger.WithFields(logrus.Fields{
			"action":   "replica_movement",
			"index":    indexName,
			"shard":    shardName,
			"numFiles": len(relativeFilePaths),
		}).Debug("Listed replica files")

		w.Write(resBytes)
		w.WriteHeader(http.StatusOK)
	})
}

func (i *indices) postAddAsyncReplicationTargetNode() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpAsyncReplicationTargetNode.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		indexName, shardName := args[1], args[2]
		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var targetNodeOverride additional.AsyncReplicationTargetNodeOverride
		if err := json.NewDecoder(r.Body).Decode(&targetNodeOverride); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = i.shards.AddAsyncReplicationTargetNode(r.Context(), indexName, shardName, targetNodeOverride, schemaVersion)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func (i *indices) deleteAsyncReplicationTargetNode() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpAsyncReplicationTargetNode.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		indexName, shardName := args[1], args[2]

		var targetNodeOverride additional.AsyncReplicationTargetNodeOverride
		if err := json.NewDecoder(r.Body).Decode(&targetNodeOverride); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := i.shards.RemoveAsyncReplicationTargetNode(r.Context(), indexName, shardName, targetNodeOverride)
		if err != nil {
			// There's no easy to have a re-usable error type via all our interfaces to reach the shard/index
			if strings.Contains(err.Error(), "shard not found") {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}
