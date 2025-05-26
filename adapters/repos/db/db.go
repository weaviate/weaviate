package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/tenantactivity"
	"github.com/weaviate/weaviate/theOneTrueFileStore"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"time"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/usecases/classification"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type DB struct {
	schemaGetter schemaUC.SchemaGetter
	router       *router.Router
}

type Index struct{}
type diskUse struct{}
type ShardReindexerV3 interface{}

// Config holds database configuration values derived from the app state.
type Config struct {
	ServerVersion                       string
	GitHash                             string
	MemtablesFlushDirtyAfter            int
	MemtablesInitialSizeMB              int
	MemtablesMaxSizeMB                  int
	MemtablesMinActiveSeconds           int
	MemtablesMaxActiveSeconds           int
	SegmentsCleanupIntervalSeconds      int
	SeparateObjectsCompactions          bool
	MaxSegmentSize                      int64
	CycleManagerRoutinesFactor          int
	IndexRangeableInMemory              bool
	HNSWMaxLogSize                      int64
	HNSWWaitForCachePrefill             bool
	HNSWFlatSearchConcurrency           int
	HNSWAcornFilterRatio                float64
	VisitedListPoolMaxSize              int
	RootPath                            string
	QueryLimit                          int64
	QueryMaximumResults                 int64
	QueryNestedRefLimit                 int64
	MaxImportGoroutinesFactor           float64
	TrackVectorDimensions               bool
	ResourceUsage                       config.ResourceUsage
	AvoidMMap                           bool
	DisableLazyLoadShards               bool
	ForceFullReplicasSearch             bool
	LSMEnableSegmentsChecksumValidation bool
	Replication                         replication.GlobalConfig
	MaximumConcurrentShardLoads         int
}

func panicStub(stubname string) {
	fmt.Printf("I>%s called\n", stubname)
	fmt.Printf("E>%s not implemented\n", stubname)
	fmt.Printf("E>Exiting...\n")
	panic(stubname + " not implemented")
}

func New(logger logrus.FieldLogger, config Config,
	remoteIndex sharding.RemoteIndexClient,
	remoteNodesClient sharding.RemoteNodeClient, replicaClient replica.Client,
	promMetrics *monitoring.PrometheusMetrics, memMonitor *memwatch.Monitor,
) (*DB, error) {
	fmt.Printf("I>New DB called with config: %+v\n")
	return &DB{}, nil
}

func (db *DB) GetUnclassified(ctx context.Context, className string, properties, target []string, filter *filters.LocalFilter) ([]search.Result, error) {
	panicStub("GetUnclassified")
	return nil, nil
}
func (db *DB) ZeroShotSearch(ctx context.Context, vector []float32, className string, properties []string, filter *filters.LocalFilter) ([]search.Result, error) {
	panicStub("ZeroShotSearch")
	return nil, nil
}
func (db *DB) Backupable(ctx context.Context, classes []string) error {
	panicStub("Backupable")
	return nil
}
func (db *DB) ListBackupable() []string { panicStub("ListBackupable"); return nil }
func (db *DB) ShardsBackup(ctx context.Context, backupID, class string, shardNames []string) (backup.ClassDescriptor, error) {
	panicStub("ShardsBackup")
	return backup.ClassDescriptor{}, nil
}
func (db *DB) ReleaseBackup(ctx context.Context, bakID, class string) error {
	panicStub("ReleaseBackup")
	return nil
}
func (db *DB) ClassExists(name string) bool { panicStub("ClassExists"); return false }
func (db *DB) Shards(ctx context.Context, class string) ([]string, error) {
	panicStub("Shards")
	return nil, nil
}
func (db *DB) ListClasses(ctx context.Context) []string { panicStub("ListClasses"); return nil }
func (db *DB) ResourceUseWarn(mon *memwatch.Monitor, du diskUse, updateMappings bool) {
	panicStub("ResourceUseWarn")
}
func (db *DB) DiskUseWarn(du diskUse)           { panicStub("DiskUseWarn") }
func (db *DB) MemUseWarn(mon *memwatch.Monitor) { panicStub("MemUseWarn") }
func (db *DB) ResourceUseReadonly(mon *memwatch.Monitor, du diskUse) {
	panicStub("ResourceUseReadonly")
}
func (db *DB) DiskUseReadonly(du diskUse)           { panicStub("DiskUseReadonly") }
func (db *DB) MemUseReadonly(mon *memwatch.Monitor) { panicStub("MemUseReadonly") }
func (db *DB) SetShardsReadOnly(reason string)      { panicStub("SetShardsReadOnly") }

// Adds multiple objects at once, returning the objects that were successfully added.
func (db *DB) BatchPutObjects(ctx context.Context, objs objects.BatchObjects, additionals *additional.ReplicationProperties, schemaVersion uint64) (objects.BatchObjects, error) {
	fmt.Printf("I>BatchPutObjects called with %d objects\n", len(objs))
	for _, obj := range objs {
		// Unpack object vectors into a [][]float32
		var vectors map[string][]float32 = make(map[string][]float32)
		for k, v := range obj.Object.Vectors {
			vectors[k] = v.([]float32)
		}
		db.PutObject(context.Background(), obj.Object, obj.Object.Vector, vectors, nil, nil, 0)
	}
	var bo []objects.BatchObject = objs
	return bo, nil
}

// Deletes multiple objects at once, returning the objects that were successfully deleted.
func (db *DB) BatchDeleteObjects(ctx context.Context, params objects.BatchDeleteParams, timestamp time.Time, repl *additional.ReplicationProperties, tenant string, consistencyLevel uint64) (objects.BatchDeleteResult, error) {
	panicStub("BatchDeleteObjects")
	return objects.BatchDeleteResult{}, nil
}

func (db *DB) Aggregate(ctx context.Context, params aggregation.Params, modulesProvider *modules.Provider) (*aggregation.Result, error) {
	panicStub("Aggregate")
	return nil, nil
}
func (db *DB) GetQueryMaximumResults() int { panicStub("GetQueryMaximumResults"); return 0 }

func NetObjToStorObj(obj *NetObject) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Vector:            obj.Vector,
		Vectors:           obj.Vectors,
		MultiVectors:      obj.Multivectors,
		BelongsToNode:     "TheOneTrueNode",
		BelongsToShard:    "TheOneTrueShard",
		IsConsistent:      true,
		DocID:             0,
		Object: models.Object{
			Additional: obj.Additional,
			Class:      obj.Class,

			CreationTimeUnix: obj.CreationTimeUnix,
			ID:               obj.ID,

			LastUpdateTimeUnix: obj.LastUpdateTimeUnix,
			Properties:         obj.Properties,
			Tenant:             obj.Tenant,
			Vector:             obj.Vector,
			VectorWeights:      nil,
			Vectors:            mapFloat32ToVectors(obj.Vectors),
		},
	}
}

func NetObjectToResultObject(obj *NetObject) *search.Result {
	return &search.Result{
		ClassName:            obj.Class,
		ID:                   obj.ID,
		Vector:               obj.Vector,
		Created:              obj.CreationTimeUnix,
		Updated:              obj.LastUpdateTimeUnix,
		Tenant:               obj.Tenant,
		Vectors:              mapFloat32ToVectors(obj.Vectors),
		AdditionalProperties: obj.Additional,
		Schema:               obj.Properties,
	}
}

// Keyword search for objects, returning the results that match the search query, and their scores.
func (db *DB) SparseObjectSearch(ctx context.Context, params dto.GetParams) ([]*storobj.Object, []float32, error) {
	pretty, err := json.Marshal(params)
	fmt.Printf("I>SparseObjectSearch called with params: %+v\n", string(pretty))
	//Figure out which kind of search we are doing
	var searchQuery string
	if params.HybridSearch != nil {

		searchQuery = params.HybridSearch.Query
		fmt.Printf("I>HybridSearch called with query: %s\n", params.HybridSearch.Query)
	} else if params.KeywordRanking != nil {

		searchQuery = params.KeywordRanking.Query
		fmt.Printf("I>KeywordRanking called with query: %s\n", params.KeywordRanking.Query)
	} else {

		if err != nil {
			fmt.Printf("E>Failed to marshal params: %s\n", err)
			return nil, nil, err
		}
		fmt.Printf("E>Unknown search type in SparseObjectSearch: %s\n", pretty)
		return nil, nil, fmt.Errorf("unknown search type")
	}

	fmt.Printf("I>Searching for query: %s\n", searchQuery)

	results := make([]*storobj.Object, 0)
	var scores []float32
	var FoundObjects map[string]bool = make(map[string]bool)
	_, err = theOneTrueFileStore.TheOneTrueFileStore().MapFunc(func(k, v []byte) error {

		key := strings.TrimPrefix(string(k), "/")
		if !strings.HasPrefix(key, "keywords/") {
			return nil
		}
		//fmt.Printf("I>Processing key: %s\n", key)

		// Extract the class and tenant and object ID from the key
		parts := strings.Split(key, "/")
		if len(parts) < 4 {
			fmt.Printf("E>Invalid key format: %s\n", key)
			return nil
		}
		class := parts[1]
		//tenant := parts[2]
		word := parts[2]
		objectID := parts[3]
		//fmt.Printf("I>Examining key: class: %s, tenant: %s, word: %s, objectID: %s\n", class, "", word, objectID)
		if strings.HasPrefix(key, "keywords/") &&
			searchQuery == word {
			fmt.Printf("I>Found keyword match for class: %s, tenant: %s, objectID: %s\n", class, "", objectID)
			FoundObjects[objectID] = true
		}
		return nil
	})

	for objID := range FoundObjects {
		var obj NetObject
		key := "objects/" + objID
		objPack, err := theOneTrueFileStore.TheOneTrueFileStore().Get([]byte(key))
		if err != nil || objPack == nil {
			fmt.Printf("E>Failed to get object with ID %s: %s\n", objID, err)
			continue
		}

		fmt.Printf("I>Unmarshalling object with ID %s\n", objID)
		if err := json.Unmarshal(objPack, &obj); err != nil {
			fmt.Printf("E>Failed to unmarshal object with ID %s: %s: %s\n", objID, err, string(objPack))
			continue
		}
		fmt.Printf("I>Found object with ID %s, class %s, tenant %s\n", obj.ID, obj.Class, obj.Tenant)

		results = append(results, NetObjToStorObj(&obj))
		scores = append(scores, 1.0)
	}

	fmt.Printf("I>Found %d results\n", len(results))
	return results, scores, err
}

func mapFloat32ToVectors(vectors map[string][]float32) models.Vectors {
	// Convert the map[string][]float32 to models.Vectors
	result := make(models.Vectors, len(vectors))
	for k, v := range vectors {
		result[k] = v
	}
	return result
}

// Keyword search for objects, returning the results that match the search query.
func (db *DB) Search(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
	objs, scores, err := db.SparseObjectSearch(ctx, params)
	var results []search.Result
	for i, obj := range objs {
		results = append(results, search.Result{
			ClassName:            obj.Object.Class,
			ID:                   obj.ID(),
			Vector:               obj.Vector,
			Created:              obj.CreationTimeUnix(),
			Updated:              obj.LastUpdateTimeUnix(),
			Tenant:               obj.Object.Tenant,
			Score:                scores[i],
			SecondarySortValue:   scores[i],
			Vectors:              mapFloat32ToVectors(obj.Vectors),
			AdditionalProperties: obj.Object.Additional,
			Schema:               obj.Properties(),
		})
	}
	return results, err
}

// Vector search for objects, returning the results that match the search vectors.
func (db *DB) VectorSearch(ctx context.Context, params dto.GetParams, tenants []string, searchVectors []models.Vector) ([]search.Result, error) {
	fmt.Printf("I>VectorSearch called with params: %+v\n", params)
	results := make([]search.Result, 0)
	_, err := theOneTrueFileStore.TheOneTrueFileStore().MapFunc(func(k, v []byte) error {
		var obj NetObject
		if err := json.Unmarshal(v, &obj); err != nil {
			fmt.Printf("E>Failed to unmarshal object: %s\n", err)
		}
		// Iterate over the properties and search them for the search vector
		for _, sv := range searchVectors {
			searchVector := sv.([]float32)

			// Check if the object vector matches the search vector
			if obj.Vector != nil && len(obj.Vector) > 0 {
				// Calculate the distance between the object vector and the search vector
				distance := float32(0.0)
				for i := 0; i < len(obj.Vector); i++ {
					distance += (obj.Vector[i] - searchVector[i]) * (obj.Vector[i] - searchVector[i])
				}
				distance = distance / float32(len(obj.Vector))
				res :=NetObjectToResultObject(&obj)
				res.Score = float32(float32(0) - distance)
				results = append(results, *res)
			}
			return nil
		}
		// Sort the results by score
		sort.Slice(results, func(i, j int) bool {
			return results[i].Score > results[j].Score
		})
		return nil
	})
	return results, err
}

func (db *DB) CrossClassVectorSearch(ctx context.Context, vector models.Vector, targetVector string, offset, limit int, filters *filters.LocalFilter) ([]search.Result, error) {
	panicStub("CrossClassVectorSearch")
	return nil, nil
}
func (db *DB) Query(ctx context.Context, q *objects.QueryInput) (search.Results, *objects.Error) {
	panicStub("Query")
	return nil, nil
}
func (db *DB) ObjectSearch(context.Context, int, int, *filters.LocalFilter, []filters.Sort, additional.Properties, string) (search.Results, error) {
	panicStub("ObjectSearch")
	return nil, nil
}
func (db *DB) ResolveReferences(ctx context.Context, objs search.Results, props search.SelectProperties, groupBy *searchparams.GroupBy, additional additional.Properties, tenant string) (search.Results, error) {
	//FIXME resolve references
	return objs, nil
}
func (db *DB) ValidateSort(sort []filters.Sort) error { panicStub("ValidateSort"); return nil }
func (db *DB) GetSearchResults(found search.Results, paramOffset, paramLimit int) search.Results {
	panicStub("GetSearchResults")
	return nil
}
func (db *DB) GetStoreObjects(res []*storobj.Object, pagination *filters.Pagination) []*storobj.Object {
	panicStub("GetStoreObjects")
	return nil
}
func (db *DB) GetStoreObjectsWithScores(res []*storobj.Object, scores []float32, pagination *filters.Pagination) ([]*storobj.Object, []float32) {
	panicStub("GetStoreObjectsWithScores")
	return nil, nil
}
func (db *DB) GetDists(dists []float32, pagination *filters.Pagination) []float32 {
	panicStub("GetDists")
	return nil
}
func (db *DB) GetOffsetLimit(arraySize int, offset, limit int) (int, int) {
	panicStub("GetOffsetLimit")
	return 0, 0
}
func (db *DB) GetLimit(limit int) int { panicStub("GetLimit"); return 0 }
func (db *DB) GetSchemaGetter() schemaUC.SchemaGetter {
	return db.schemaGetter
}
func (db *DB) GetSchema() schema.Schema                   { panicStub("GetSchema"); return schema.Schema{} }
func (db *DB) GetConfig() Config                          { panicStub("GetConfig"); return Config{} }
func (db *DB) GetRemoteIndex() sharding.RemoteIndexClient { panicStub("GetRemoteIndex"); return nil }
func (db *DB) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	db.schemaGetter = sg
}
func (db *DB) SetRouter(r *router.Router) {
	db.router = r
}

func (db *DB) WaitForStartup(ctx context.Context) error    { panicStub("WaitForStartup"); return nil }
func (db *DB) StartupComplete() bool                       { panicStub("StartupComplete"); return false }
func (db *DB) GetIndex(className schema.ClassName) *Index  { panicStub("GetIndex"); return nil }
func (db *DB) IndexExists(className schema.ClassName) bool { panicStub("IndexExists"); return false }
func (db *DB) GetIndexForIncomingSharding(className schema.ClassName) sharding.RemoteIndexIncomingRepo {
	panicStub("GetIndexForIncomingSharding")
	return nil
}
func (db *DB) GetIndexForIncomingReplica(className schema.ClassName) replica.RemoteIndexIncomingRepo {
	panicStub("GetIndexForIncomingReplica")
	return nil
}
func (db *DB) DeleteIndex(className schema.ClassName) error { panicStub("DeleteIndex"); return nil }
func (db *DB) Shutdown(ctx context.Context) error           { return nil }
func (db *DB) WithReindexer(reindexer ShardReindexerV3) *DB { panicStub("WithReindexer"); return db }
func (db *DB) ReplicateObject(ctx context.Context, class string, obj *models.Object) {
	panicStub("ReplicateObject")
}
func (db *DB) ReplicateObjects(ctx context.Context, class string, objs []*models.Object) {
	panicStub("ReplicateObjects")
}
func (db *DB) ReplicateUpdate(ctx context.Context, class string, obj *models.Object) {
	panicStub("ReplicateUpdate")
}
func (db *DB) ReplicateDeletion(ctx context.Context, class string, id strfmt.UUID) {
	panicStub("ReplicateDeletion")
}
func (db *DB) ReplicateDeletions(ctx context.Context, class string, ids []strfmt.UUID) {
	panicStub("ReplicateDeletions")
}
func (db *DB) ReplicateReferences(ctx context.Context, class string, refs []*crossref.Ref) {
	panicStub("ReplicateReferences")
}
func (db *DB) CommitReplication(class string) { panicStub("CommitReplication") }
func (db *DB) AbortReplication(class string)  { panicStub("AbortReplication") }
func (db *DB) ReplicatedIndex(name string) (*Index, *replica.SimpleResponse) {
	panicStub("ReplicatedIndex")
	return nil, nil
}

type NetObject struct {
	// additional
	Additional models.AdditionalProperties `json:"additional,omitempty"`

	// Class of the Object, defined in the schema.
	Class string `json:"class,omitempty"`

	// (Response only) Timestamp of creation of this object in milliseconds since epoch UTC.
	CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

	// ID of the Object.
	// Format: uuid
	ID strfmt.UUID `json:"id,omitempty"`

	// (Response only) Timestamp of the last object update in milliseconds since epoch UTC.
	LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`

	// properties
	Properties interface{} `json:"properties,omitempty"`

	// Name of the Objects tenant.
	Tenant string `json:"tenant,omitempty"`

	// This field returns vectors associated with the Object. C11yVector, Vector or Vectors values are possible.
	Vector []float32 `json:"vector,omitempty"`

	// This field returns vectors associated with the Object.
	Vectors map[string][]float32 `json:"vectors,omitempty"`

	// Multivectors
	Multivectors map[string][][]float32 `json:"multivectors,omitempty"`
}

// PutObject stores an object in the database, including its vector and additional properties.
func (db *DB) PutObject(ctx context.Context, object *models.Object, vector []float32, vectors map[string][]float32, multivectors map[string][][]float32, repli *additional.ReplicationProperties, schemaVersion uint64) error {
	fmt.Printf("I>PutObject called with object: %+v\n", object.Properties)
	sto := NetObject{
		Additional:         object.Additional,
		Class:              object.Class,
		CreationTimeUnix:   object.CreationTimeUnix,
		ID:                 object.ID,
		LastUpdateTimeUnix: object.LastUpdateTimeUnix,
		Properties:         object.Properties,
		Tenant:             object.Tenant,
		Vector:             object.Vector,
		Vectors:            vectors,
		Multivectors:       multivectors,
	}
	stoData, err := json.Marshal(sto)
	if err != nil {
		return err
	}
	err = theOneTrueFileStore.TheOneTrueFileStore().Put([]byte(strings.Join([]string{"objects", object.ID.String()}, "/")), stoData)
	if err != nil {
		return err
	}

	// Break the object and properties into words and store them for keyword search
	words := strings.Fields(fmt.Sprintf("%v", object.Properties))
	for _, word := range words {
		word = strings.ToLower(word)
		//Strip non-alphanumeric characters
		word = strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
				return r
			}
			return -1 // Remove non-alphanumeric characters
		}, word)
		if len(word) == 0 {
			continue // Skip empty words
		}
		tenant := object.Tenant
		if tenant == "" {
			tenant = "default" // Use a default tenant if none is specified
		}
		class := object.Class
		if class == "" {
			class = "default" // Use a default class if none is specified
		}
		// Create a unique key for the word, class, and tenant

		wordKey := fmt.Sprintf("%s/%s/%s", object.Class, object.Tenant, word)
		storeKey := "/keywords/" + wordKey + "/" + object.ID.String()
		fmt.Printf("I>Storing %s¥n", storeKey)
		err = theOneTrueFileStore.TheOneTrueFileStore().Put([]byte(storeKey), stoData)
		if err != nil {
			fmt.Printf("E>Failed to store keyword %s for object %s: %s\n", word, object.ID.String(), err)
		}
	}
	fmt.Printf("I>Stored object %s in class %s with tenant %s\n", object.ID.String(), object.Class, object.Tenant)

	return nil
}
func (db *DB) MultiGet(ctx context.Context, query []multi.Identifier) { panicStub("MultiGet") }

// Updated method as per instructions
func (db *DB) ObjectsByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties, tenant string) (search.Results, error) {
	panicStub("ObjectsByID")
	return nil, nil
}

// Updated method as per instructions
func (db *DB) ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties, tenant string) (*search.Result, error) {
	panicStub("ObjectByID")
	return nil, nil
}

func (db *DB) Object(ctx context.Context, class string, id strfmt.UUID, props search.SelectProperties, additionals additional.Properties, additionalreps *additional.ReplicationProperties, tenant string) (*search.Result, error) {
	fmt.Printf("I>Object called with class: %s, id: %s, props: %+v, additionals: %+v, tenant: %s\n", class, id.String(), props, additionals, tenant)
	objData, err := theOneTrueFileStore.TheOneTrueFileStore().Get([]byte(strings.Join([]string{"objects", class, tenant, id.String()}, "/")))
	if err != nil {
		return nil, err
	}

	obj := &models.Object{}
	if err := json.Unmarshal(objData, obj); err != nil {
		return nil, err
	}

	result := &search.Result{
		ClassName: class,
		ID:        obj.ID,
		Vector:    obj.Vector,
		Vectors:   obj.Vectors,
		Created:   obj.CreationTimeUnix,
		Updated:   obj.LastUpdateTimeUnix,
	}
	return result, nil
}
func (db *DB) EnrichRefsForSingle(ctx context.Context, obj *search.Result) {
	panicStub("EnrichRefsForSingle")
}
func (db *DB) GetNodeStatus(ctx context.Context, className string, verbosity string) ([]*models.NodeStatus, error) {
	panicStub("GetNodeStatus")
	return nil, nil
}
func (db *DB) IncomingGetNodeStatus(ctx context.Context, className, verbosity string) (*models.NodeStatus, error) {
	panicStub("IncomingGetNodeStatus")
	return nil, nil
}
func (db *DB) LocalNodeStatus(ctx context.Context, className, output string) *models.NodeStatus {
	panicStub("LocalNodeStatus")
	return nil
}
func (db *DB) LocalNodeBatchStats() *models.BatchStats { panicStub("LocalNodeBatchStats"); return nil }
func (db *DB) GetNodeStatistics(ctx context.Context) ([]*models.Statistics, error) {
	panicStub("GetNodeStatistics")
	return nil, nil
}
func (db *DB) IncomingGetNodeStatistics() (*models.Statistics, error) {
	panicStub("IncomingGetNodeStatistics")
	return nil, nil
}
func (db *DB) LocalNodeStatistics() (*models.Statistics, error) {
	panicStub("LocalNodeStatistics")
	return nil, nil
}
func (db *DB) GetNodeStatisticsByName(ctx context.Context, nodeName string) (*models.Statistics, error) {
	panicStub("GetNodeStatisticsByName")
	return nil, nil
}
func (db *DB) Init(ctx context.Context) error { panicStub("Init"); return nil }
func (db *DB) LocalTenantActivity() tenantactivity.ByCollection {
	panicStub("LocalTenantActivity")
	return nil
}
func (db *DB) MigrateFileStructureIfNecessary() error {
	panicStub("MigrateFileStructureIfNecessary")
	return nil
}
func (db *DB) MigrateToHierarchicalFS() error { panicStub("MigrateToHierarchicalFS"); return nil }
func (db *DB) AddBatchReferences(context.Context, objects.BatchReferences, *additional.ReplicationProperties, uint64) (objects.BatchReferences, error) {
	panicStub("AddBatchReferences")
	return nil, nil
}
func (db *DB) AddReference(context.Context, *crossref.RefSource, *crossref.Ref, *additional.ReplicationProperties, string, uint64) error {
	panicStub("AddReference")
	return nil
}
func (db *DB) BackupDescriptors(context.Context, string, []string) <-chan backup.ClassDescriptor {
	panicStub("BackupDescriptors")
	return nil
}
func (db *DB) AggregateNeighbors(context.Context, []float32, string, []string, int, *filters.LocalFilter) ([]classification.NeighborRef, error) {
	panicStub("AggregateNeighbors")
	return nil, nil
}
func (db *DB) DeleteObject(context.Context, string, strfmt.UUID, time.Time, *additional.ReplicationProperties, string, uint64) error {
	panicStub("DeleteObject")
	return nil
}
func (db *DB) Exists(context.Context, string, strfmt.UUID, *additional.ReplicationProperties, string) (bool, error) {
	panicStub("Exists")
	return false, nil
}
func (db *DB) Merge(context.Context, objects.MergeDocument, *additional.ReplicationProperties, string, uint64) error {
	panicStub("Merge")
	return nil
}
