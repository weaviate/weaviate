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

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/entities/modelsext"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"golang.org/x/text/unicode/norm"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/ttl"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/classcache"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/restrictions"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingcfg "github.com/weaviate/weaviate/usecases/sharding/config"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

func (h *Handler) GetClass(ctx context.Context, principal *models.Principal, name string) (*models.Class, error) {
	if err := h.Authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsMetadata(name)...); err != nil {
		return nil, err
	}
	name = schema.UppercaseClassName(name)

	cl := h.schemaReader.ReadOnlyClass(name)
	return cl, nil
}

func (h *Handler) GetConsistentClass(ctx context.Context, principal *models.Principal,
	name string, consistency bool,
) (*models.Class, uint64, error) {
	// NOTE: Support getting class via alias name
	// Also we resolve before doing `Authorize` so that Authorizer will work
	// with correct `collectionName` for permissions and errors UX
	name = schema.UppercaseClassName(name)
	if rname := h.schemaReader.ResolveAlias(name); rname != "" {
		name = rname
	}

	if err := h.Authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsMetadata(name)...); err != nil {
		return nil, 0, err
	}

	if consistency {
		vclasses, err := h.schemaManager.QueryReadOnlyClasses(name)
		return vclasses[name].Class, vclasses[name].Version, err
	}
	class, err := h.schemaReader.ReadOnlyClassWithVersion(ctx, name, 0)
	return class, 0, err
}

// GetCachedClass will return the class from the cache if it exists. Note that the context cache
// will likely be at the "request" or "operation" level and not be shared between requests.
// Uses the Handler's getClassMethod to determine how to get the class data.
func (h *Handler) GetCachedClass(ctxWithClassCache context.Context,
	principal *models.Principal, names ...string,
) (map[string]versioned.Class, error) {
	if err := h.Authorizer.Authorize(ctxWithClassCache, principal, authorization.READ, authorization.CollectionsMetadata(names...)...); err != nil {
		return nil, err
	}

	return classcache.ClassesFromContext(ctxWithClassCache, func(names ...string) (map[string]versioned.Class, error) {
		return h.classGetter.getClasses(names)
	}, names...)
}

// GetCachedClassNoAuth will return the class from the cache if it exists. Note that the context cache
// will likely be at the "request" or "operation" level and not be shared between requests.
// Uses the Handler's getClassMethod to determine how to get the class data.
func (h *Handler) GetCachedClassNoAuth(ctxWithClassCache context.Context, names ...string) (map[string]versioned.Class, error) {
	return classcache.ClassesFromContext(ctxWithClassCache, func(names ...string) (map[string]versioned.Class, error) {
		return h.classGetter.getClasses(names)
	}, names...)
}

// AddClass to the schema
func (h *Handler) AddClass(ctx context.Context, principal *models.Principal,
	cls *models.Class,
) (*models.Class, uint64, error) {
	cls.Class = schema.UppercaseClassName(cls.Class)
	cls.Properties = schema.LowercaseAllPropertyNames(cls.Properties)

	err := h.Authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.CollectionsMetadata(cls.Class)...)
	if err != nil {
		return nil, 0, err
	}

	if cls.ObjectTTLConfig != nil && cls.ObjectTTLConfig.Enabled {
		if err := h.Authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.CollectionsData(cls.Class)...); err != nil {
			return nil, 0, err
		}
	}

	classGetterWithAuth := func(name string) (*models.Class, error) {
		if err := h.Authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsMetadata(name)...); err != nil {
			return nil, err
		}
		return h.schemaReader.ReadOnlyClass(name), nil
	}

	if err := h.setNewClassDefaults(cls, h.config.Replication); err != nil {
		return nil, 0, err
	}

	if err := h.validateCanAddClass(ctx, cls, classGetterWithAuth, true); err != nil {
		return nil, 0, err
	}
	// migrate only after validation in completed
	h.migrateClassSettings(cls)
	if err := h.parser.ParseClass(cls); err != nil {
		return nil, 0, err
	}

	existingCollectionsCount, err := h.schemaManager.QueryCollectionsCount()
	if err != nil {
		h.logger.WithField("error", err).Error("could not query the collections count")
	}

	limit := h.schemaConfig.MaximumAllowedCollectionsCount.Get()

	if limit != config.DefaultMaximumAllowedCollectionsCount && existingCollectionsCount >= limit {
		// Migrated from a free-text 422 to a typed 429 / RESOURCE_EXHAUSTED
		// in the usage-limits work; see docs/usage_limits.md for the wire
		// contract.
		return nil, 0, usagelimits.NewLimitExceededError(
			h.errorMessageTemplate(), usagelimits.LimitCollections, int64(limit))
	}

	// Per-collection shard cap. Config-time check only — shard count comes
	// straight from the create request, no live state to consult.
	// Multi-tenant collections set DesiredCount=0 (shards are created
	// per-tenant on demand) so the cap is naturally satisfied for those;
	// the check meaningfully constrains single-tenant configurations only.
	if dv := h.config.UsageLimits.MaxShardsPerCollection; dv != nil {
		shardCap := dv.Get()
		if shardCap >= 0 {
			requested := cls.ShardingConfig.(shardingcfg.Config).DesiredCount
			if requested > shardCap {
				return nil, 0, usagelimits.NewLimitExceededError(
					h.errorMessageTemplate(), usagelimits.LimitShards, int64(shardCap))
			}
		}
	}

	shardState, err := sharding.InitState(cls.Class,
		cls.ShardingConfig.(shardingcfg.Config),
		h.clusterState.LocalName(), h.schemaManager.StorageCandidates(), cls.ReplicationConfig.Factor,
		schema.MultiTenancyEnabled(cls))
	if err != nil {
		return nil, 0, errors.Wrap(err, "init sharding state")
	}

	defaultQuantization := h.config.DefaultQuantization
	h.enableQuantization(cls, defaultQuantization)

	version, err := h.schemaManager.AddClass(ctx, cls, shardState)
	if err != nil {
		return nil, 0, err
	}
	return cls, version, err
}

func (h *Handler) enableQuantization(class *models.Class, defaultQuantization *configRuntime.DynamicValue[string]) {
	compression := defaultQuantization.Get()

	if compression == "" {
		return
	}

	var err error
	if !hasTargetVectors(class) || class.VectorIndexType != "" {
		class.VectorIndexConfig, err = setDefaultQuantization(class.VectorIndexType, class.VectorIndexConfig.(schemaConfig.VectorIndexConfig), compression)
		if err != nil {
			h.logger.WithField("error", err).Error("error while setting default quantization")
		}
	}

	for k, vectorConfig := range class.VectorConfig {
		vectorConfig.VectorIndexConfig, err = setDefaultQuantization(vectorConfig.VectorIndexType, vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig), compression)
		class.VectorConfig[k] = vectorConfig
		if err != nil {
			h.logger.WithField("error", err).Error("error while setting default quantization")
		}
	}
}

func setDefaultQuantization(vectorIndexType string, vectorIndexConfig schemaConfig.VectorIndexConfig, compression string) (schemaConfig.VectorIndexConfig, error) {
	if len(vectorIndexType) == 0 {
		vectorIndexType = vectorindex.DefaultVectorIndexType
	}
	if vectorIndexType == vectorindex.VectorIndexTypeHNSW && vectorIndexConfig.IndexType() == vectorindex.VectorIndexTypeHNSW {
		return hnsw.ParseDefaultQuantization(vectorIndexConfig, compression)
	} else if vectorIndexType == vectorindex.VectorIndexTypeFLAT && vectorIndexConfig.IndexType() == vectorindex.VectorIndexTypeFLAT {
		return flat.ParseDefaultQuantization(vectorIndexConfig, compression)
	} else if vectorIndexType == vectorindex.VectorIndexTypeDYNAMIC && vectorIndexConfig.IndexType() == vectorindex.VectorIndexTypeDYNAMIC {
		return dynamic.ParseDefaultQuantization(vectorIndexConfig, compression)
	}
	return vectorIndexConfig, nil
}

func (h *Handler) RestoreClass(ctx context.Context, d *backup.ClassDescriptor, m map[string]string, overwriteAlias bool) error {
	// get schema and sharding state
	class := &models.Class{}
	if err := json.Unmarshal(d.Schema, &class); err != nil {
		return fmt.Errorf("unmarshal class schema: %w", err)
	}
	var shardingState sharding.State
	if d.ShardingState != nil {
		err := json.Unmarshal(d.ShardingState, &shardingState)
		if err != nil {
			return fmt.Errorf("unmarshal sharding state: %w", err)
		}
	}

	aliases := make([]*models.Alias, 0)
	if d.AliasesIncluded {
		if err := json.Unmarshal(d.Aliases, &aliases); err != nil {
			return fmt.Errorf("unmarshal aliases: %w", err)
		}
	}

	metric, err := monitoring.GetMetrics().BackupRestoreClassDurations.GetMetricWithLabelValues(class.Class)
	if err == nil {
		timer := prometheus.NewTimer(metric)
		defer timer.ObserveDuration()
	}

	class.Class = schema.UppercaseClassName(class.Class)
	class.Properties = schema.LowercaseAllPropertyNames(class.Properties)

	if err := h.setClassDefaults(class, h.config.Replication); err != nil {
		return err
	}

	// no validation of reference for restore
	classGetterWrapper := func(name string) (*models.Class, error) {
		return h.schemaReader.ReadOnlyClass(name), nil
	}

	err = h.validateClassInvariants(ctx, class, classGetterWrapper, true)
	if err != nil {
		return err
	}
	// migrate only after validation in completed
	h.migrateClassSettings(class)

	if err := h.parser.ParseClass(class); err != nil {
		return err
	}

	shardingState.MigrateFromOldFormat()
	err = shardingState.MigrateShardingStateReplicationFactor()
	if err != nil {
		return fmt.Errorf("error while migrating replication factor: %w", err)
	}
	shardingState.ApplyNodeMapping(m)
	_, err = h.schemaManager.RestoreClass(ctx, class, &shardingState)
	if err != nil {
		return fmt.Errorf("error when trying to restore class: %w", err)
	}

	for _, alias := range aliases {
		resolved := h.schemaReader.ResolveAlias(alias.Alias)

		// Alias do exist and don't want to overwrite
		if resolved != "" && !overwriteAlias {
			continue
		}

		if resolved != "" {
			_, err := h.schemaManager.DeleteAlias(ctx, alias.Alias)
			if err != nil {
				return fmt.Errorf("failed to restore alias for class: delete alias %s failed: %w", alias.Alias, err)
			}
		}

		_, err := h.schemaManager.CreateAlias(ctx, alias.Alias, class)
		if err != nil {
			return fmt.Errorf("failed to restore alias for class: create alias %s failed: %w", alias.Alias, err)
		}
	}

	return nil
}

// DeleteClass from the schema
func (h *Handler) DeleteClass(ctx context.Context, principal *models.Principal, class string) error {
	err := h.Authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.CollectionsMetadata(class)...)
	if err != nil {
		return err
	}

	class = schema.UppercaseClassName(class)

	if _, err = h.schemaManager.DeleteClass(ctx, class); err != nil {
		return err
	}

	return nil
}

func (h *Handler) UpdateClass(ctx context.Context, principal *models.Principal,
	className string, updated *models.Class,
) error {
	err := h.Authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.CollectionsMetadata(className)...)
	if err != nil || updated == nil {
		return err
	}

	// Require DELETE permission on data when any TTL setting is being changed,
	// but not when the user is updating other collection settings without
	// touching TTL configuration.
	initial := h.schemaReader.ReadOnlyClass(className)
	var initialTTLConfig *models.ObjectTTLConfig
	if initial != nil {
		initialTTLConfig = initial.ObjectTTLConfig
	}
	if ttl.IsTtlConfigChanged(initialTTLConfig, updated.ObjectTTLConfig) {
		if err := h.Authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.CollectionsData(className)...); err != nil {
			return err
		}
	}

	return UpdateClassInternal(h, ctx, className, updated)
}

// bypass the auth check for internal class update requests
func UpdateClassInternal(h *Handler, ctx context.Context, className string, updated *models.Class,
) error {
	// make sure unset optionals on 'updated' don't lead to an error, as all
	// optionals would have been set with defaults on the initial already
	if err := h.setClassDefaults(updated, h.config.Replication); err != nil {
		return err
	}

	if ttlConfig, _, err := ttl.ValidateObjectTTLConfig(updated, true, h.config); err != nil {
		return fmt.Errorf("ObjectTTLConfig: %w", err)
	} else {
		updated.ObjectTTLConfig = ttlConfig
	}

	if err := h.parser.ParseClass(updated); err != nil {
		return err
	}

	// ideally, these calls would be encapsulated in ParseClass but ParseClass is
	// used in many different areas of the codebase that may cause BC issues with the
	// new validation logic. Issue ref: gh-5860
	// As our testing becomes more comprehensive, we can move these calls into ParseClass
	if err := h.parser.parseModuleConfig(updated); err != nil {
		return fmt.Errorf("parse module config: %w", err)
	}

	if err := h.parser.parseVectorConfig(updated); err != nil {
		return fmt.Errorf("parse vector config: %w", err)
	}

	if err := h.validateVectorSettings(updated); err != nil {
		return err
	}

	initial := h.schemaReader.ReadOnlyClass(className)

	if initial != nil {
		_, err := validateUpdatingMT(initial, updated)
		if err != nil {
			return err
		}

		initialRF := initial.ReplicationConfig.Factor
		updatedRF := updated.ReplicationConfig.Factor

		if updatedRF <= 0 {
			return fmt.Errorf("replication factor must be at least 1, got %d", updatedRF)
		}

		if initialRF < updatedRF {
			var shardingState *sharding.State
			h.schemaReader.Read(className, true, func(c *models.Class, s *sharding.State) error {
				stateCopy := s.DeepCopy()
				shardingState = &stateCopy
				return nil
			})

			if shardingState == nil {
				return fmt.Errorf("sharding state not found for class %q", className)
			}

			for _, physical := range shardingState.Physical {
				if int64(len(physical.BelongsToNodes)) < updatedRF {
					return fmt.Errorf("not enough replicas in shard %q to increase replication factor to %d for class %q", physical.Name, updatedRF, className)
				}
			}
		}

		if err := validateImmutableFields(initial, updated, h.parser.modules); err != nil {
			return err
		}
	}
	// A nil sharding state means that the sharding state will not be updated.

	_, err := h.schemaManager.UpdateClass(ctx, updated, nil)
	return err
}

func (m *Handler) setNewClassDefaults(class *models.Class, globalCfg replication.GlobalConfig) error {
	if class.ShardingConfig != nil && schema.MultiTenancyEnabled(class) {
		return fmt.Errorf("cannot have both shardingConfig and multiTenancyConfig")
	} else if class.MultiTenancyConfig == nil {
		class.MultiTenancyConfig = &models.MultiTenancyConfig{}
	} else if class.MultiTenancyConfig.Enabled {
		class.ShardingConfig = shardingcfg.Config{DesiredCount: 0} // tenant shards will be created dynamically
	}

	if err := m.setClassDefaults(class, globalCfg); err != nil {
		return err
	}

	if class.ReplicationConfig == nil {
		class.ReplicationConfig = &models.ReplicationConfig{
			Factor:           int64(m.config.Replication.MinimumFactor),
			DeletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			AsyncEnabled:     false,
		}
		return nil
	}

	if class.ReplicationConfig.DeletionStrategy == "" {
		class.ReplicationConfig.DeletionStrategy = models.ReplicationConfigDeletionStrategyTimeBasedResolution
	}

	return nil
}

func (h *Handler) setClassDefaults(class *models.Class, globalCfg replication.GlobalConfig) error {
	// set legacy vector index defaults only when:
	// 	- no target vectors are configured
	//  - OR, there are target vectors configured AND there is a legacy vector configured
	if !hasTargetVectors(class) || modelsext.ClassHasLegacyVectorIndex(class) {
		if class.Vectorizer == "" {
			class.Vectorizer = h.config.DefaultVectorizerModule
		}

		if class.VectorIndexType == "" {
			if v := h.config.DefaultVectorIndexType.Get(); v != "" {
				class.VectorIndexType = v
			} else {
				class.VectorIndexType = vectorindex.DefaultVectorIndexType
			}
		}

		if h.config.DefaultVectorDistanceMetric != "" {
			if class.VectorIndexConfig == nil {
				class.VectorIndexConfig = map[string]interface{}{"distance": h.config.DefaultVectorDistanceMetric}
			} else if vIdxCfgMap, ok := class.VectorIndexConfig.(map[string]interface{}); ok && vIdxCfgMap["distance"] == nil {
				class.VectorIndexConfig.(map[string]interface{})["distance"] = h.config.DefaultVectorDistanceMetric
			}
		}
	}

	setInvertedConfigDefaults(class)
	for _, prop := range class.Properties {
		setPropertyDefaults(prop)
	}

	if class.ReplicationConfig == nil {
		class.ReplicationConfig = &models.ReplicationConfig{Factor: int64(globalCfg.MinimumFactor)}
	}

	if class.ReplicationConfig.Factor > 0 && class.ReplicationConfig.Factor < int64(globalCfg.MinimumFactor) {
		return fmt.Errorf("invalid replication factor: setup requires a minimum replication factor of %d: got %d",
			globalCfg.MinimumFactor, class.ReplicationConfig.Factor)
	}

	if globalCfg.MaximumFactor > 0 && class.ReplicationConfig.Factor > int64(globalCfg.MaximumFactor) {
		return fmt.Errorf("invalid replication factor: setup caps replication at %d: got %d",
			globalCfg.MaximumFactor, class.ReplicationConfig.Factor)
	}

	if class.ReplicationConfig.Factor < 1 {
		class.ReplicationConfig.Factor = int64(globalCfg.MinimumFactor)
	}

	h.moduleConfig.SetClassDefaults(class)
	return nil
}

func setPropertyDefaults(props ...*models.Property) {
	setPropertyDefaultTokenization(props...)
	setPropertyDefaultIndexing(props...)
	for _, prop := range props {
		setNestedPropertiesDefaults(prop.NestedProperties)
	}
}

func setPropertyDefaultTokenization(props ...*models.Property) {
	for _, prop := range props {
		switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
		case schema.DataTypeString, schema.DataTypeStringArray:
			// deprecated as of v1.19, default tokenization was word
			// which will be migrated to text+whitespace
			if prop.Tokenization == "" {
				prop.Tokenization = models.PropertyTokenizationWord
			}
		case schema.DataTypeText, schema.DataTypeTextArray:
			if prop.Tokenization == "" {
				if os.Getenv("DEFAULT_TOKENIZATION") != "" {
					prop.Tokenization = os.Getenv("DEFAULT_TOKENIZATION")
				} else {
					prop.Tokenization = models.PropertyTokenizationWord
				}
			}
		default:
			// tokenization not supported for other data types
		}
	}
}

func setPropertyDefaultIndexing(props ...*models.Property) {
	for _, prop := range props {
		// if IndexInverted is set but IndexFilterable and IndexSearchable are not
		// migrate IndexInverted later.
		if prop.IndexInverted != nil &&
			prop.IndexFilterable == nil &&
			prop.IndexSearchable == nil &&
			prop.IndexRangeFilters == nil {
			continue
		}

		vTrue := true
		vFalse := false
		if prop.IndexFilterable == nil {
			prop.IndexFilterable = &vTrue
		}
		if prop.IndexSearchable == nil {
			switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
			case schema.DataTypeString, schema.DataTypeStringArray:
				// string/string[] are migrated to text/text[] later,
				// at this point they are still valid data types, therefore should be handled here
				prop.IndexSearchable = &vTrue
			case schema.DataTypeText, schema.DataTypeTextArray:
				prop.IndexSearchable = &vTrue
			default:
				prop.IndexSearchable = &vFalse
			}
		}
		if prop.IndexRangeFilters == nil {
			prop.IndexRangeFilters = &vFalse
		}
	}
}

func setNestedPropertiesDefaults(properties []*models.NestedProperty) {
	for _, property := range properties {
		primitiveDataType, isPrimitive := schema.AsPrimitive(property.DataType)
		nestedDataType, isNested := schema.AsNested(property.DataType)

		setNestedPropertyDefaultTokenization(property, primitiveDataType, nestedDataType, isPrimitive, isNested)
		setNestedPropertyDefaultIndexing(property, primitiveDataType, nestedDataType, isPrimitive, isNested)

		if isNested {
			setNestedPropertiesDefaults(property.NestedProperties)
		}
	}
}

func setNestedPropertyDefaultTokenization(property *models.NestedProperty,
	primitiveDataType, nestedDataType schema.DataType,
	isPrimitive, isNested bool,
) {
	if property.Tokenization == "" && isPrimitive {
		switch primitiveDataType {
		case schema.DataTypeText, schema.DataTypeTextArray:
			property.Tokenization = models.NestedPropertyTokenizationWord
		default:
			// do nothing
		}
	}
}

func setNestedPropertyDefaultIndexing(property *models.NestedProperty,
	primitiveDataType, nestedDataType schema.DataType,
	isPrimitive, isNested bool,
) {
	vTrue := true
	vFalse := false

	if property.IndexFilterable == nil {
		property.IndexFilterable = &vTrue

		if isPrimitive && (primitiveDataType == schema.DataTypeBlob || primitiveDataType == schema.DataTypeBlobHash) {
			property.IndexFilterable = &vFalse
		}
	}

	if property.IndexSearchable == nil {
		property.IndexSearchable = &vFalse

		if isPrimitive {
			switch primitiveDataType {
			case schema.DataTypeText, schema.DataTypeTextArray:
				property.IndexSearchable = &vTrue
			default:
				// do nothing
			}
		}
	}

	if property.IndexRangeFilters == nil {
		property.IndexRangeFilters = &vFalse
	}
}

func (h *Handler) migrateClassSettings(class *models.Class) {
	for _, prop := range class.Properties {
		migratePropertySettings(prop)
	}
}

func migratePropertySettings(props ...*models.Property) {
	migratePropertyDataTypeAndTokenization(props...)
	migratePropertyIndexInverted(props...)
}

// as of v1.19 DataTypeString and DataTypeStringArray are deprecated
// here both are changed to Text/TextArray
// and proper, backward compatible tokenization
func migratePropertyDataTypeAndTokenization(props ...*models.Property) {
	for _, prop := range props {
		switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
		case schema.DataTypeString:
			prop.DataType = schema.DataTypeText.PropString()
		case schema.DataTypeStringArray:
			prop.DataType = schema.DataTypeTextArray.PropString()
		default:
			// other types need no migration and do not support tokenization
			continue
		}

		switch prop.Tokenization {
		case models.PropertyTokenizationWord:
			prop.Tokenization = models.PropertyTokenizationWhitespace
		case models.PropertyTokenizationField:
			// stays field
		}
	}
}

// as of v1.19 IndexInverted is deprecated and replaced with
// IndexFilterable (set inverted index)
// and IndexSearchable (map inverted index with term frequencies;
// therefore applicable only to text/text[] data types)
func migratePropertyIndexInverted(props ...*models.Property) {
	vFalse := false

	for _, prop := range props {
		// if none of new options is set, use inverted settings
		if prop.IndexInverted != nil &&
			prop.IndexFilterable == nil &&
			prop.IndexSearchable == nil &&
			prop.IndexRangeFilters == nil {
			prop.IndexFilterable = prop.IndexInverted
			switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
			// string/string[] are already migrated into text/text[], can be skipped here
			case schema.DataTypeText, schema.DataTypeTextArray:
				prop.IndexSearchable = prop.IndexInverted
			default:
				prop.IndexSearchable = &vFalse
			}
			prop.IndexRangeFilters = &vFalse
		}
		// new options have precedence so inverted can be reset
		prop.IndexInverted = nil
	}
}

func (h *Handler) validateProperty(
	class *models.Class, existingPropertyNames map[string]bool,
	relaxCrossRefValidation bool, classGetterWithAuth func(string) (*models.Class, error), props ...*models.Property,
) error {
	for _, property := range props {
		if _, err := schema.ValidatePropertyName(property.Name); err != nil {
			return err
		}

		if err := schema.ValidateReservedPropertyName(property.Name); err != nil {
			return err
		}

		if existingPropertyNames[strings.ToLower(property.Name)] {
			return fmt.Errorf("class %q: conflict for property %q: already in use or provided multiple times", class.Class, property.Name)
		}

		// Validate data type of property.
		propertyDataType, err := schema.FindPropertyDataTypeWithRefsAndAuth(classGetterWithAuth, property.DataType,
			relaxCrossRefValidation, schema.ClassName(class.Class))
		if err != nil {
			return fmt.Errorf("property '%s': invalid dataType: %v: %w", property.Name, property.DataType, err)
		}

		var userPresets map[string][]string
		if class.InvertedIndexConfig != nil {
			userPresets = class.InvertedIndexConfig.StopwordPresets
		}

		if propertyDataType.IsNested() {
			if err := validateNestedProperties(property.NestedProperties, property.Name, userPresets); err != nil {
				return err
			}
		} else {
			if len(property.NestedProperties) > 0 {
				return fmt.Errorf("property '%s': nestedProperties not allowed for data types other than object/object[]",
					property.Name)
			}
		}

		if err := h.validatePropertyTokenization(property.Tokenization, propertyDataType); err != nil {
			return err
		}

		if err := validatePropertyProcessing(property, propertyDataType, userPresets); err != nil {
			return err
		}

		if err := h.validatePropertyIndexing(property); err != nil {
			return err
		}

		if err := h.validatePropModuleConfig(class, property); err != nil {
			return err
		}
	}

	return nil
}

func setInvertedConfigDefaults(class *models.Class) {
	if class.InvertedIndexConfig == nil {
		class.InvertedIndexConfig = &models.InvertedIndexConfig{}
	}
	// force the default in case it was not set, as empty bool == false
	class.InvertedIndexConfig.UsingBlockMaxWAND = config.DefaultUsingBlockMaxWAND

	if class.InvertedIndexConfig.CleanupIntervalSeconds == 0 {
		class.InvertedIndexConfig.CleanupIntervalSeconds = config.DefaultCleanupIntervalSeconds
	}

	if class.InvertedIndexConfig.Bm25 == nil {
		class.InvertedIndexConfig.Bm25 = &models.BM25Config{
			K1: config.DefaultBM25k1,
			B:  config.DefaultBM25b,
		}
	}

	if class.InvertedIndexConfig.Stopwords == nil {
		class.InvertedIndexConfig.Stopwords = &models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		}
	}
}

func (h *Handler) validateCanAddClass(ctx context.Context, class *models.Class, classGetterWithAuth func(string) (*models.Class, error),
	relaxCrossRefValidation bool,
) error {
	if modelsext.ClassHasLegacyVectorIndex(class) && len(class.VectorConfig) > 0 {
		return fmt.Errorf("creating a class with both a class level vector index and named vectors is forbidden")
	}

	// Reject any vector config entry with the "none" sentinel on a brand-new
	// class. The "none" type is an internal marker set only by the
	// DeleteClassVectorIndex API for previously-existing indexes.
	for name, cfg := range class.VectorConfig {
		if modelsext.IsVectorIndexDropped(cfg) {
			return fmt.Errorf("vector %q: cannot create a new class with vectorIndexType %q; "+
				"this is an internal sentinel for dropped indexes", name, cfg.VectorIndexType)
		}
	}

	return h.validateClassInvariants(ctx, class, classGetterWithAuth, relaxCrossRefValidation)
}

func (h *Handler) validateClassInvariants(
	ctx context.Context, class *models.Class, classGetterWithAuth func(string) (*models.Class, error),
	relaxCrossRefValidation bool,
) error {
	if _, err := schema.ValidateClassName(class.Class); err != nil {
		return err
	}

	existingPropertyNames := map[string]bool{}
	for _, property := range class.Properties {
		if err := h.validateProperty(class, existingPropertyNames, relaxCrossRefValidation, classGetterWithAuth, property); err != nil {
			return err
		}
		existingPropertyNames[strings.ToLower(property.Name)] = true
	}

	if err := h.validateVectorSettings(class); err != nil {
		return err
	}

	if err := h.moduleConfig.ValidateClass(ctx, class); err != nil {
		return err
	}

	if err := validateMT(class); err != nil {
		return err
	}

	if err := replica.ValidateConfig(class, h.config.Replication); err != nil {
		return err
	}

	if ttlConfig, needsInvertedIndexTimestamp, err := ttl.ValidateObjectTTLConfig(class, false, h.config); err != nil {
		return fmt.Errorf("ObjectTTLConfig: %w", err)
	} else {
		class.ObjectTTLConfig = ttlConfig
		if needsInvertedIndexTimestamp {
			if class.InvertedIndexConfig == nil {
				class.InvertedIndexConfig = &models.InvertedIndexConfig{}
			}
			class.InvertedIndexConfig.IndexTimestamps = true
		}
	}

	if err := h.invertedConfigValidator(class.InvertedIndexConfig); err != nil {
		return err
	}

	// all is fine!
	return nil
}

func (h *Handler) validatePropertyTokenization(tokenization string, propertyDataType schema.PropertyDataType) error {
	if propertyDataType.IsPrimitive() {
		primitiveDataType := propertyDataType.AsPrimitive()

		switch primitiveDataType {
		case schema.DataTypeString, schema.DataTypeStringArray:
			// deprecated as of v1.19, will be migrated to DataTypeText/DataTypeTextArray
			switch tokenization {
			case models.PropertyTokenizationField, models.PropertyTokenizationWord:
				return nil
			}
		case schema.DataTypeText, schema.DataTypeTextArray:
			switch tokenization {
			case models.PropertyTokenizationField, models.PropertyTokenizationWord,
				models.PropertyTokenizationWhitespace, models.PropertyTokenizationLowercase,
				models.PropertyTokenizationTrigram:
				return nil
			case models.PropertyTokenizationGse:
				if !entcfg.Enabled(os.Getenv("USE_GSE")) && !entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_GSE")) {
					return fmt.Errorf("the GSE tokenizer is not enabled; set 'ENABLE_TOKENIZER_GSE' to 'true' to enable")
				}
				return nil
			case models.PropertyTokenizationGseCh:
				if !entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_GSE_CH")) {
					return fmt.Errorf("the Chinese tokenizer is not enabled; set 'ENABLE_TOKENIZER_GSE_CH' to 'true' to enable")
				}
				return nil
			case models.PropertyTokenizationKagomeKr:
				if !entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_KR")) {
					return fmt.Errorf("the Korean tokenizer is not enabled; set 'ENABLE_TOKENIZER_KAGOME_KR' to 'true' to enable")
				}
				return nil
			case models.PropertyTokenizationKagomeJa:
				if !entcfg.Enabled(os.Getenv("ENABLE_TOKENIZER_KAGOME_JA")) {
					return fmt.Errorf("the Japanese tokenizer is not enabled; set 'ENABLE_TOKENIZER_KAGOME_JA' to 'true' to enable")
				}
				return nil
			}
		default:
			if tokenization == "" {
				return nil
			}
			return fmt.Errorf("tokenization is not allowed for data type '%s'", primitiveDataType)
		}
		return fmt.Errorf("tokenization '%s' is not allowed for data type '%s'", tokenization, primitiveDataType)
	}

	if tokenization == "" {
		return nil
	}

	if propertyDataType.IsNested() {
		return fmt.Errorf("tokenization is not allowed for object/object[] data types")
	}
	return fmt.Errorf("tokenization is not allowed for reference data type")
}

func (h *Handler) validatePropertyIndexing(prop *models.Property) error {
	if prop.IndexInverted != nil {
		if prop.IndexFilterable != nil || prop.IndexSearchable != nil || prop.IndexRangeFilters != nil {
			return fmt.Errorf("`indexInverted` is deprecated and can not be set together with `indexFilterable`, " + "`indexSearchable` or `indexRangeFilters`")
		}
	}

	dataType, _ := schema.AsPrimitive(prop.DataType)
	if prop.IndexSearchable != nil {
		switch dataType {
		case schema.DataTypeString, schema.DataTypeStringArray:
			// string/string[] are migrated to text/text[] later,
			// at this point they are still valid data types, therefore should be handled here.
			// true or false allowed
		case schema.DataTypeText, schema.DataTypeTextArray:
			// true or false allowed
		default:
			if *prop.IndexSearchable {
				return fmt.Errorf("`indexSearchable` is allowed only for text/text[] data types. " +
					"For other data types set false or leave empty")
			}
		}
	}
	if prop.IndexRangeFilters != nil {
		switch dataType {
		case schema.DataTypeNumber, schema.DataTypeInt, schema.DataTypeDate:
			// true or false allowed
		case schema.DataTypeNumberArray, schema.DataTypeIntArray, schema.DataTypeDateArray:
			// not supported (yet?)
			fallthrough
		default:
			if *prop.IndexRangeFilters {
				return fmt.Errorf("`indexRangeFilters` is allowed only for number/int/date data types. " +
					"For other data types set false or leave empty")
			}
		}
	}

	return nil
}

func validatePropertyProcessing(prop *models.Property, propertyDataType schema.PropertyDataType, userPresets map[string][]string) error {
	// Treat an empty config as absent — some client generators emit
	// "textAnalyzer": {} by default and this should not block creation.
	if prop.TextAnalyzer != nil && !prop.TextAnalyzer.ASCIIFold && len(prop.TextAnalyzer.ASCIIFoldIgnore) == 0 && prop.TextAnalyzer.StopwordPreset == "" {
		prop.TextAnalyzer = nil
	}
	if prop.TextAnalyzer == nil {
		return nil
	}

	// processing only makes sense for text/text[] with searchable index
	if !propertyDataType.IsPrimitive() {
		return fmt.Errorf("property '%s': processing options are only allowed for text and text[] data types", prop.Name)
	}

	dt := propertyDataType.AsPrimitive()
	switch dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		// allowed
	default:
		return fmt.Errorf("property '%s': processing options are only allowed for text and text[] data types, got '%s'", prop.Name, dt)
	}

	if (prop.IndexSearchable == nil || !*prop.IndexSearchable) && (prop.IndexFilterable == nil || !*prop.IndexFilterable) {
		return fmt.Errorf("property '%s': processing options are only allowed for properties with an inverted index, got IndexSearchable=%s and IndexFilterable=%s",
			prop.Name, fmtBoolPtr(prop.IndexSearchable), fmtBoolPtr(prop.IndexFilterable))
	}

	if !prop.TextAnalyzer.ASCIIFold && len(prop.TextAnalyzer.ASCIIFoldIgnore) > 0 {
		return fmt.Errorf("property '%s': asciiFoldIgnore requires asciiFold to be enabled", prop.Name)
	}

	for _, entry := range prop.TextAnalyzer.ASCIIFoldIgnore {
		if utf8.RuneCountInString(norm.NFC.String(entry)) != 1 {
			return fmt.Errorf("property '%s': each asciiFoldIgnore entry must be a single character, got %q",
				prop.Name, entry)
		}
	}

	// explicitly check for support for tokenizers:
	if prop.Tokenization != "" {
		switch prop.Tokenization {
		case models.PropertyTokenizationLowercase,
			models.PropertyTokenizationTrigram,
			models.PropertyTokenizationWord,
			models.PropertyTokenizationWhitespace,
			models.PropertyTokenizationField: // supported tokenizers, do nothing
		default:
			return fmt.Errorf("property '%s': unsupported tokenization '%s'", prop.Name, prop.Tokenization)
		}
	}

	if prop.TextAnalyzer.StopwordPreset != "" {
		if prop.Tokenization != models.PropertyTokenizationWord {
			return fmt.Errorf("property '%s': stopwordPreset is only supported with tokenization %q, got %q",
				prop.Name, models.PropertyTokenizationWord, prop.Tokenization)
		}
		_, builtIn := stopwords.Presets[prop.TextAnalyzer.StopwordPreset]
		_, userDefined := userPresets[prop.TextAnalyzer.StopwordPreset]
		if !builtIn && !userDefined {
			return fmt.Errorf("property '%s': unknown stopword preset %q; must be a built-in preset ('en', 'none') or defined in invertedIndexConfig.stopwordPresets",
				prop.Name, prop.TextAnalyzer.StopwordPreset)
		}
	}

	return nil
}

// validateStopwordPresetsStillReferenced rejects an inverted-index-config
// update that would remove a user-defined stopwordPreset still referenced by
// any (top-level or nested) property's textAnalyzer.stopwordPreset. Without
// this check, the property would silently fall back to no stopwords at query
// time once the preset disappears from the collection config.
func validateStopwordPresetsStillReferenced(properties []*models.Property,
	updatedPresets map[string][]string,
) error {
	check := func(propName, presetName string) error {
		if presetName == "" {
			return nil
		}
		if _, builtIn := stopwords.Presets[presetName]; builtIn {
			return nil
		}
		if _, ok := updatedPresets[presetName]; ok {
			return nil
		}
		return fmt.Errorf("invertedIndexConfig.stopwordPresets: cannot remove preset %q because it is still used by property %q",
			presetName, propName)
	}

	var walkNested func(parentName string, nested []*models.NestedProperty) error
	walkNested = func(parentName string, nested []*models.NestedProperty) error {
		for _, np := range nested {
			if np == nil {
				continue
			}
			fullName := parentName + "." + np.Name
			if np.TextAnalyzer != nil {
				if err := check(fullName, np.TextAnalyzer.StopwordPreset); err != nil {
					return err
				}
			}
			if err := walkNested(fullName, np.NestedProperties); err != nil {
				return err
			}
		}
		return nil
	}

	for _, prop := range properties {
		if prop == nil {
			continue
		}
		if prop.TextAnalyzer != nil {
			if err := check(prop.Name, prop.TextAnalyzer.StopwordPreset); err != nil {
				return err
			}
		}
		if err := walkNested(prop.Name, prop.NestedProperties); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) validateVectorSettings(class *models.Class) error {
	if modelsext.ClassHasLegacyVectorIndex(class) {
		if err := h.validateVectorIndexType(class.VectorIndexType); err != nil {
			return err
		}

		if err := h.validateVectorizer(class.Vectorizer); err != nil {
			return err
		}

		if asMap, ok := class.VectorIndexConfig.(map[string]interface{}); ok && len(asMap) > 0 {
			parsed, err := h.parser.parseGivenVectorIndexConfig(class.VectorIndexType, class.VectorIndexConfig, h.parser.modules.IsMultiVector(class.Vectorizer), h.config.DefaultQuantization)
			if err != nil {
				return fmt.Errorf("class.VectorIndexConfig can not parse: %w", err)
			}
			if parsed.IsMultiVector() {
				return errors.New("class.VectorIndexConfig multi vector type index type is only configurable using named vectors")
			}
			if err := h.validateAllowedCompression(class.VectorIndexType, parsed); err != nil {
				return err
			}
		} else if typed, ok := class.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
			// UpdateClass parses before validation (see UpdateClassInternal),
			// so the legacy config is already a typed struct at validation
			// time. Run the compression allow-list against the typed form
			// directly — skipping it here would let a disallowed compression
			// slip through on update.
			if err := h.validateAllowedCompression(class.VectorIndexType, typed); err != nil {
				return err
			}
		}
	}

	for name, cfg := range class.VectorConfig {
		if modelsext.IsVectorIndexDropped(cfg) {
			continue
		}
		// check only if vectorizer correctly configured (map with single key being vectorizer name)
		// other cases are handled in module config validation
		if vm, ok := cfg.Vectorizer.(map[string]interface{}); ok && len(vm) == 1 {
			for vectorizer := range vm {
				if err := h.validateVectorizer(vectorizer); err != nil {
					return fmt.Errorf("target vector %q: %w", name, err)
				}
			}
		}
		if err := h.validateVectorIndexType(cfg.VectorIndexType); err != nil {
			return fmt.Errorf("target vector %q: %w", name, err)
		}
		// Named-vector compression check. AddClass reaches this path with
		// raw maps (parser.ParseClass runs after validation), so parse
		// inline here — mirroring the legacy block above. UpdateClass has
		// already converted to typed configs by the time we get here.
		var parsed schemaConfig.VectorIndexConfig
		switch v := cfg.VectorIndexConfig.(type) {
		case schemaConfig.VectorIndexConfig:
			parsed = v
		case map[string]interface{}:
			isMultiVector := false
			if vm, ok := cfg.Vectorizer.(map[string]interface{}); ok && len(vm) == 1 {
				for vectorizer := range vm {
					isMultiVector = h.parser.modules.IsMultiVector(vectorizer)
				}
			}
			p, err := h.parser.parseGivenVectorIndexConfig(cfg.VectorIndexType, v, isMultiVector, h.config.DefaultQuantization)
			if err != nil {
				// Surface the parse error against the named vector; the
				// existing parser path will hit the same error later, so
				// we're only changing where it surfaces, not whether.
				return fmt.Errorf("target vector %q: parse vector index config: %w", name, err)
			}
			parsed = p
		}
		if parsed != nil {
			if err := h.validateAllowedCompression(cfg.VectorIndexType, parsed); err != nil {
				return fmt.Errorf("target vector %q: %w", name, err)
			}
		}
	}
	return nil
}

// validateAllowedCompression rejects a class whose explicit compression
// configuration is outside the operator's ALLOWED_COMPRESSION_TYPES
// allow-list. Returns a typed *restrictions.ViolationError on rejection
// (mapped to HTTP 422 / gRPC FailedPrecondition by the handlers).
//
// hfresh classes are skipped entirely per the RFC: hfresh has no
// compression knobs that operators can policy on, so the allow-list is
// not applicable. Startup validation already ensures that the operator
// hasn't paired `ALLOWED_VECTOR_INDEX_TYPES=hfresh` (only) with a
// non-empty compression allow-list.
//
// Detection only inspects user-supplied / already-typed config. The
// `enableQuantization` path that applies the operator-configured
// DEFAULT_QUANTIZATION runs later in AddClass; startup validation
// guarantees that default is itself in the allow-list, so a class that
// reaches this validator with no compression set will end up with a
// compatible compression after defaults are applied.
func (h *Handler) validateAllowedCompression(vectorIndexType string, cfg schemaConfig.VectorIndexConfig) error {
	allow := h.allowedCompressionTypes()
	if allow == nil {
		return nil
	}
	if vectorIndexType == vectorindex.VectorIndexTypeHFresh {
		return nil
	}
	// compressionsFromIndexConfig returns every user-selected compression
	// in the parsed config. For HNSW/Flat there's at most one entry; for
	// Dynamic the user can configure HnswUC AND FlatUC independently and
	// both must be checked — collapsing to a single value would let a
	// disallowed compression on the non-returned branch slip through.
	for _, compression := range compressionsFromIndexConfig(cfg) {
		if !containsString(allow, compression) {
			return restrictions.NewViolationError(
				h.restrictionsErrorMessageTemplate(),
				restrictions.RestrictionCompression,
				compression,
				allow,
			)
		}
	}
	return nil
}

func containsString(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// compressionsFromIndexConfig returns every user-selected compression in
// the parsed vector index config. For HNSW / Flat the slice has 0 or 1
// entry; for Dynamic the slice may contain up to two entries (one each
// for HnswUC and FlatUC) so the caller can validate both halves.
//
// An empty slice means "user did not pick any compression explicitly";
// the operator's DEFAULT_QUANTIZATION will fill it in later, and startup
// validation guarantees that default is in the allow-list.
//
// `"none"` appears in the slice only when `SkipDefaultQuantization` is
// set on a branch — that is the explicit "opt out of all compression"
// signal, distinct from "I didn't pick one". `"none"` not being in the
// allow-list correctly rejects that branch.
func compressionsFromIndexConfig(cfg schemaConfig.VectorIndexConfig) []string {
	switch c := cfg.(type) {
	case hnsw.UserConfig:
		if v := compressionFromHnsw(c); v != "" {
			return []string{v}
		}
		return nil
	case flat.UserConfig:
		if v := compressionFromFlat(c); v != "" {
			return []string{v}
		}
		return nil
	case dynamic.UserConfig:
		var out []string
		if v := compressionFromHnsw(c.HnswUC); v != "" {
			out = append(out, v)
		}
		if v := compressionFromFlat(c.FlatUC); v != "" {
			out = append(out, v)
		}
		return out
	default:
		return nil
	}
}

func compressionFromHnsw(c hnsw.UserConfig) string {
	if c.PQ.Enabled {
		return "pq"
	}
	if c.SQ.Enabled {
		return "sq"
	}
	if c.RQ.Enabled {
		if c.RQ.Bits == 1 {
			return "rq-1"
		}
		return "rq-8"
	}
	if c.BQ.Enabled {
		return "bq"
	}
	if c.SkipDefaultQuantization {
		return "none"
	}
	return ""
}

func compressionFromFlat(c flat.UserConfig) string {
	if c.PQ.Enabled {
		return "pq"
	}
	if c.SQ.Enabled {
		return "sq"
	}
	if c.RQ.Enabled {
		if c.RQ.Bits == 1 {
			return "rq-1"
		}
		return "rq-8"
	}
	if c.BQ.Enabled {
		return "bq"
	}
	if c.SkipDefaultQuantization {
		return "none"
	}
	return ""
}

func (h *Handler) validateVectorizer(vectorizer string) error {
	if vectorizer == config.VectorizerModuleNone {
		return nil
	}

	if err := h.vectorizerValidator.ValidateVectorizer(vectorizer); err != nil {
		return errors.Wrap(err, "vectorizer")
	}

	return nil
}

func (h *Handler) validateVectorIndexType(vectorIndexType string) error {
	switch vectorIndexType {
	case vectorindex.VectorIndexTypeHNSW, vectorindex.VectorIndexTypeFLAT:
		// continue to allow-list check below
	case vectorindex.VectorIndexTypeDYNAMIC:
		if !h.asyncIndexingEnabled {
			return fmt.Errorf("the dynamic index can only be created when async indexing is enabled")
		}
	case vectorindex.VectorIndexTypeHFresh:
		if !h.config.HFreshEnabled {
			return fmt.Errorf("the hfresh index is available only in experimental mode")
		}
	default:
		return errors.Errorf("unrecognized or unsupported vectorIndexType %q",
			vectorIndexType)
	}

	if allow := h.allowedVectorIndexTypes(); allow != nil {
		found := false
		for _, t := range allow {
			if t == vectorIndexType {
				found = true
				break
			}
		}
		if !found {
			return restrictions.NewViolationError(
				h.restrictionsErrorMessageTemplate(),
				restrictions.RestrictionVectorIndexType,
				vectorIndexType,
				allow,
			)
		}
	}
	return nil
}

func validateMT(class *models.Class) error {
	enabled := schema.MultiTenancyEnabled(class)
	if !enabled && schema.AutoTenantCreationEnabled(class) {
		return fmt.Errorf("can't enable autoTenantCreation on a non-multi-tenant class")
	}

	if !enabled && schema.AutoTenantActivationEnabled(class) {
		return fmt.Errorf("can't enable autoTenantActivation on a non-multi-tenant class")
	}

	return nil
}

// validateUpdatingMT validates toggling MT and returns whether mt is enabled
func validateUpdatingMT(current, update *models.Class) (enabled bool, err error) {
	enabled = schema.MultiTenancyEnabled(current)
	if schema.MultiTenancyEnabled(update) != enabled {
		if enabled {
			err = fmt.Errorf("disabling multi-tenancy for an existing class is not supported")
		} else {
			err = fmt.Errorf("enabling multi-tenancy for an existing class is not supported")
		}
	} else {
		err = validateMT(update)
	}

	return enabled, err
}

func validateImmutableFields(initial, updated *models.Class, modulesProvider modulesProvider) error {
	immutableFields := []immutableText{
		{
			name:     "class name",
			accessor: func(c *models.Class) string { return c.Class },
		},
	}

	if err := validateImmutableTextFields(initial, updated, immutableFields...); err != nil {
		return err
	}

	for k, v := range updated.VectorConfig {
		if _, ok := initial.VectorConfig[k]; !ok {
			continue
		}
		initialVecCfg := initial.VectorConfig[k]
		if modelsext.IsVectorIndexDropped(v) || modelsext.IsVectorIndexDropped(initialVecCfg) {
			continue
		}

		// SkipDefaultQuantization and TrackDefaultQuantization must be effectively immutable
		// without enforcing that on the client. We just set these fields to their initial values.
		switch cfg := v.VectorIndexConfig.(type) {
		case hnsw.UserConfig:
			cfgInitial := initial.VectorConfig[k].VectorIndexConfig.(hnsw.UserConfig)
			cfg.SkipDefaultQuantization = cfgInitial.SkipDefaultQuantization
			cfg.TrackDefaultQuantization = cfgInitial.TrackDefaultQuantization
			v.VectorIndexConfig = cfg
		case flat.UserConfig:
			cfgInitial := initial.VectorConfig[k].VectorIndexConfig.(flat.UserConfig)
			cfg.SkipDefaultQuantization = cfgInitial.SkipDefaultQuantization
			cfg.TrackDefaultQuantization = cfgInitial.TrackDefaultQuantization
			v.VectorIndexConfig = cfg
		case dynamic.UserConfig:
			cfgInitial := initial.VectorConfig[k].VectorIndexConfig.(dynamic.UserConfig)
			cfg.HnswUC.SkipDefaultQuantization = cfgInitial.HnswUC.SkipDefaultQuantization
			cfg.HnswUC.TrackDefaultQuantization = cfgInitial.HnswUC.TrackDefaultQuantization
			cfg.FlatUC.SkipDefaultQuantization = cfgInitial.FlatUC.SkipDefaultQuantization
			cfg.FlatUC.TrackDefaultQuantization = cfgInitial.FlatUC.TrackDefaultQuantization
			v.VectorIndexConfig = cfg
		}
		updated.VectorConfig[k] = v

		if !deepEqualVectorizerSettings(initial.VectorConfig[k].Vectorizer, v.Vectorizer) {
			// There might be module settings that need to be migrated to new names, for example
			// if baseUrl property setting was renamed to baseURL then we need to adjust module settings
			// and migrate baseUrl to baseURL
			if modulesProvider.MigrateVectorizerSettings(initial.VectorConfig[k].Vectorizer, v.Vectorizer) {
				// Module settings have been migrated, let's recheck vectorizer settings
				if deepEqualVectorizerSettings(initial.VectorConfig[k].Vectorizer, v.Vectorizer) {
					continue
				}
			}

			return fmt.Errorf("vectorizer config of vector %q is immutable for class %s", k, updated.Class)
		}
	}

	// changing indexing not allowed
	compareIndexSetting := func(name string, extractVal func(config *models.InvertedIndexConfig) bool) error {
		initialVal := initial.InvertedIndexConfig != nil && extractVal(initial.InvertedIndexConfig)
		updatedVal := updated.InvertedIndexConfig != nil && extractVal(updated.InvertedIndexConfig)
		if initialVal != updatedVal {
			return fmt.Errorf("%q setting is immutable. Value changed from \"%v\" to \"%v\"", name, initialVal, updatedVal)
		}
		return nil
	}
	if err := compareIndexSetting("indexTimestamp", func(config *models.InvertedIndexConfig) bool { return config.IndexTimestamps }); err != nil {
		return err
	}
	if err := compareIndexSetting("indexNullState", func(config *models.InvertedIndexConfig) bool { return config.IndexNullState }); err != nil {
		return err
	}
	if err := compareIndexSetting("indexPropertyLength", func(config *models.InvertedIndexConfig) bool { return config.IndexPropertyLength }); err != nil {
		return err
	}

	return nil
}

func deepEqualVectorizerSettings(initial, updated any) bool {
	return reflect.DeepEqual(structToMap(initial), structToMap(updated))
}

func structToMap(obj any) (objMap map[string]any) {
	if obj == nil {
		return nil
	}
	data, _ := json.Marshal(obj)  // Convert to a json string
	json.Unmarshal(data, &objMap) // Convert to a map
	return objMap
}

func fmtBoolPtr(b *bool) string {
	if b == nil {
		return "<nil>"
	}
	if *b {
		return "true"
	}
	return "false"
}

type immutableText struct {
	accessor func(c *models.Class) string
	name     string
}

func validateImmutableTextFields(previous, next *models.Class,
	immutables ...immutableText,
) error {
	for _, immutable := range immutables {
		oldField := immutable.accessor(previous)
		newField := immutable.accessor(next)
		if oldField != newField {
			return errors.Errorf("%s is immutable: attempted change from %q to %q",
				immutable.name, oldField, newField)
		}
	}
	return nil
}

func validateLegacyVectorIndexConfigImmutableFields(initial, updated *models.Class) error {
	return validateImmutableTextFields(initial, updated, []immutableText{
		{
			name:     "vectorizer",
			accessor: func(c *models.Class) string { return c.Vectorizer },
		},
		{
			name:     "vector index type",
			accessor: func(c *models.Class) string { return c.VectorIndexType },
		},
	}...)
}
