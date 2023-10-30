//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (h *Handler) GetClass(ctx context.Context, principal *models.Principal,
	name string,
) (*models.Class, error) {
	err := h.Authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return nil, err
	}
	return h.metaReader.ReadOnlyClass(name), nil
}

// AddClass to the schema
func (h *Handler) AddClass(ctx context.Context, principal *models.Principal,
	cls *models.Class,
) error {
	err := h.Authorizer.Authorize(principal, "create", "schema/objects")
	if err != nil {
		return err
	}

	cls.Class = schema.UppercaseClassName(cls.Class)
	cls.Properties = schema.LowercaseAllPropertyNames(cls.Properties)
	if cls.ShardingConfig != nil && schema.MultiTenancyEnabled(cls) {
		return fmt.Errorf("cannot have both shardingConfig and multiTenancyConfig")
	} else if cls.MultiTenancyConfig == nil {
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{}
	} else if cls.MultiTenancyConfig.Enabled {
		cls.ShardingConfig = sharding.Config{DesiredCount: 0} // tenant shards will be created dynamically
	}

	h.setClassDefaults(cls)

	if err := h.validateCanAddClass(ctx, cls, false); err != nil {
		return err
	}
	// migrate only after validation in completed
	h.migrateClassSettings(cls)
	if err := h.parser.ParseClass(cls); err != nil {
		return err
	}

	err = h.invertedConfigValidator(cls.InvertedIndexConfig)
	if err != nil {
		return err
	}

	shardState, err := sharding.InitState(cls.Class,
		cls.ShardingConfig.(sharding.Config),
		h.clusterState, cls.ReplicationConfig.Factor,
		schema.MultiTenancyEnabled(cls))
	if err != nil {
		return fmt.Errorf("init sharding state: %w", err)
	}

	return h.metaWriter.AddClass(cls, shardState)
}

func (h *Handler) RestoreClass(ctx context.Context, d *backup.ClassDescriptor) error {
	// get schema and sharding state
	class := &models.Class{}
	if err := json.Unmarshal(d.Schema, &class); err != nil {
		return fmt.Errorf("marshal class schema: %w", err)
	}
	var shardingState sharding.State
	if d.ShardingState != nil {
		err := json.Unmarshal(d.ShardingState, &shardingState)
		if err != nil {
			return fmt.Errorf("marshal sharding state: %w", err)
		}
	}

	metric, err := monitoring.GetMetrics().BackupRestoreClassDurations.GetMetricWithLabelValues(class.Class)
	if err == nil {
		timer := prometheus.NewTimer(metric)
		defer timer.ObserveDuration()
	}

	class.Class = schema.UppercaseClassName(class.Class)
	class.Properties = schema.LowercaseAllPropertyNames(class.Properties)

	h.setClassDefaults(class)
	err = h.validateCanAddClass(ctx, class, true)
	if err != nil {
		return err
	}
	// migrate only after validation in completed
	h.migrateClassSettings(class)

	if err := h.parser.ParseClass(class); err != nil {
		return err
	}

	err = h.invertedConfigValidator(class.InvertedIndexConfig)
	if err != nil {
		return err
	}

	shardingState.MigrateFromOldFormat()
	return h.metaWriter.RestoreClass(class, &shardingState)
}

// DeleteClass from the schema
func (h *Handler) DeleteClass(ctx context.Context, principal *models.Principal, class string) error {
	err := h.Authorizer.Authorize(principal, "delete", "schema/objects")
	if err != nil {
		return err
	}

	return h.metaWriter.DeleteClass(class)
}

func (h *Handler) UpdateClass(ctx context.Context, principal *models.Principal,
	className string, updated *models.Class,
) error {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	initial := h.metaReader.ReadOnlyClass(className)
	if initial == nil {
		return ErrNotFound
	}
	mtEnabled, err := validateUpdatingMT(initial, updated)
	if err != nil {
		return err
	}

	// make sure unset optionals on 'updated' don't lead to an error, as all
	// optionals would have been set with defaults on the initial already
	h.setClassDefaults(updated)

	if err := h.validateImmutableFields(initial, updated); err != nil {
		return err
	}
	if err := h.parser.ParseClass(updated); err != nil {
		return err
	}

	if err := h.validator.ValidateVectorIndexConfigUpdate(ctx,
		initial.VectorIndexConfig.(schema.VectorIndexConfig),
		updated.VectorIndexConfig.(schema.VectorIndexConfig)); err != nil {
		return errors.Wrap(err, "vector index config")
	}

	if err := h.validator.ValidateInvertedIndexConfigUpdate(ctx,
		initial.InvertedIndexConfig, updated.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}
	if err := validateShardingConfig(initial, updated, mtEnabled, h.clusterState); err != nil {
		return fmt.Errorf("validate sharding config: %w", err)
	}

	if err := replica.ValidateConfigUpdate(initial, updated, h.clusterState); err != nil {
		return fmt.Errorf("replication config: %w", err)
	}

	initialRF := initial.ReplicationConfig.Factor
	updatedRF := updated.ReplicationConfig.Factor
	// TODO-RAFT: implement raft based scaling
	if initialRF != updatedRF {
		return fmt.Errorf("scaling is not implemented")
	}
	return h.metaWriter.UpdateClass(updated, nil)
}

func (h *Handler) setClassDefaults(class *models.Class) {
	if class.Vectorizer == "" {
		class.Vectorizer = h.config.DefaultVectorizerModule
	}

	if class.VectorIndexType == "" {
		class.VectorIndexType = "hnsw"
	}

	if h.config.DefaultVectorDistanceMetric != "" {
		if class.VectorIndexConfig == nil {
			class.VectorIndexConfig = map[string]interface{}{"distance": h.config.DefaultVectorDistanceMetric}
		} else if class.VectorIndexConfig.(map[string]interface{})["distance"] == nil {
			class.VectorIndexConfig.(map[string]interface{})["distance"] = h.config.DefaultVectorDistanceMetric
		}
	}

	setInvertedConfigDefaults(class)
	for _, prop := range class.Properties {
		setPropertyDefaults(prop)
	}

	h.moduleConfig.SetClassDefaults(class)
}

func setPropertyDefaults(prop *models.Property) {
	setPropertyDefaultTokenization(prop)
	setPropertyDefaultIndexing(prop)
}

func setPropertyDefaultTokenization(prop *models.Property) {
	switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
	case schema.DataTypeString, schema.DataTypeStringArray:
		// deprecated as of v1.19, default tokenization was word
		// which will be migrated to text+whitespace
		if prop.Tokenization == "" {
			prop.Tokenization = models.PropertyTokenizationWord
		}
	case schema.DataTypeText, schema.DataTypeTextArray:
		if prop.Tokenization == "" {
			prop.Tokenization = models.PropertyTokenizationWord
		}
	default:
		// tokenization not supported for other data types
	}
}

func setPropertyDefaultIndexing(prop *models.Property) {
	// if IndexInverted is set but IndexFilterable and IndexSearchable are not
	// migrate IndexInverted later.
	if prop.IndexInverted != nil &&
		prop.IndexFilterable == nil &&
		prop.IndexSearchable == nil {
		return
	}

	vTrue := true
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
			vFalse := false
			prop.IndexSearchable = &vFalse
		}
	}
}

func (h *Handler) migrateClassSettings(class *models.Class) {
	for _, prop := range class.Properties {
		migratePropertySettings(prop)
	}
}

func migratePropertySettings(prop *models.Property) {
	migratePropertyDataTypeAndTokenization(prop)
	migratePropertyIndexInverted(prop)
}

// as of v1.19 DataTypeString and DataTypeStringArray are deprecated
// here both are changed to Text/TextArray
// and proper, backward compatible tokenization
func migratePropertyDataTypeAndTokenization(prop *models.Property) {
	switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
	case schema.DataTypeString:
		prop.DataType = schema.DataTypeText.PropString()
	case schema.DataTypeStringArray:
		prop.DataType = schema.DataTypeTextArray.PropString()
	default:
		// other types need no migration and do not support tokenization
		return
	}

	switch prop.Tokenization {
	case models.PropertyTokenizationWord:
		prop.Tokenization = models.PropertyTokenizationWhitespace
	case models.PropertyTokenizationField:
		// stays field
	}
}

// as of v1.19 IndexInverted is deprecated and replaced with
// IndexFilterable (set inverted index)
// and IndexSearchable (map inverted index with term frequencies;
// therefore applicable only to text/text[] data types)
func migratePropertyIndexInverted(prop *models.Property) {
	// if none of new options is set, use inverted settings
	if prop.IndexInverted != nil &&
		prop.IndexFilterable == nil &&
		prop.IndexSearchable == nil {
		prop.IndexFilterable = prop.IndexInverted
		switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
		// string/string[] are already migrated into text/text[], can be skipped here
		case schema.DataTypeText, schema.DataTypeTextArray:
			prop.IndexSearchable = prop.IndexInverted
		default:
			vFalse := false
			prop.IndexSearchable = &vFalse
		}
	}
	// new options have precedence so inverted can be reset
	prop.IndexInverted = nil
}

func (h *Handler) validateProperty(
	property *models.Property, className string,
	existingPropertyNames map[string]bool, relaxCrossRefValidation bool,
) error {
	if _, err := schema.ValidatePropertyName(property.Name); err != nil {
		return err
	}

	if err := schema.ValidateReservedPropertyName(property.Name); err != nil {
		return err
	}

	if existingPropertyNames[strings.ToLower(property.Name)] {
		return fmt.Errorf("class %q: conflict for property %q: already in use or provided multiple times", property.Name, className)
	}

	// Validate data type of property.
	sch := h.getSchema()

	propertyDataType, err := (&sch).FindPropertyDataTypeWithRefs(property.DataType,
		relaxCrossRefValidation, schema.ClassName(className))
	if err != nil {
		return fmt.Errorf("property '%s': invalid dataType: %v", property.Name, err)
	}

	if err := h.validatePropertyTokenization(property.Tokenization, propertyDataType); err != nil {
		return err
	}

	if err := h.validatePropertyIndexing(property); err != nil {
		return err
	}

	// all is fine!
	return nil
}

func setInvertedConfigDefaults(class *models.Class) {
	if class.InvertedIndexConfig == nil {
		class.InvertedIndexConfig = &models.InvertedIndexConfig{}
	}

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

func (h *Handler) validateCanAddClass(
	ctx context.Context, class *models.Class,
	relaxCrossRefValidation bool,
) error {
	if err := h.validateClassName(class.Class); err != nil {
		return err
	}

	existingPropertyNames := map[string]bool{}
	for _, property := range class.Properties {
		if err := h.validateProperty(property, class.Class, existingPropertyNames, relaxCrossRefValidation); err != nil {
			return err
		}
		existingPropertyNames[strings.ToLower(property.Name)] = true
	}

	if err := h.validateVectorSettings(ctx, class); err != nil {
		return err
	}

	if err := h.moduleConfig.ValidateClass(ctx, class); err != nil {
		return err
	}

	if err := replica.ValidateConfig(class, h.config.Replication); err != nil {
		return err
	}

	// all is fine!
	return nil
}

func (h *Handler) validateClassName(name string) error {
	if _, err := schema.ValidateClassName(name); err != nil {
		return err
	}
	existingName := h.metaReader.ClassEqual(name)
	if existingName == "" {
		return nil
	}
	if name != existingName {
		return fmt.Errorf(
			"class name %q already exists as a permutation of: %q. class names must be unique when lowercased",
			name, existingName)
	}
	return fmt.Errorf("class name %q already exists", name)
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
				models.PropertyTokenizationWhitespace, models.PropertyTokenizationLowercase:
				return nil
			}
		default:
			if tokenization == "" {
				return nil
			}
			return fmt.Errorf("Tokenization is not allowed for data type '%s'", primitiveDataType)
		}
		return fmt.Errorf("Tokenization '%s' is not allowed for data type '%s'", tokenization, primitiveDataType)
	}

	if tokenization == "" {
		return nil
	}
	return fmt.Errorf("Tokenization is not allowed for reference data type")
}

func (h *Handler) validatePropertyIndexing(prop *models.Property) error {
	if prop.IndexInverted != nil {
		if prop.IndexFilterable != nil || prop.IndexSearchable != nil {
			return fmt.Errorf("`indexInverted` is deprecated and can not be set together with `indexFilterable` or `indexSearchable`.")
		}
	}

	if prop.IndexSearchable != nil {
		switch dataType, _ := schema.AsPrimitive(prop.DataType); dataType {
		case schema.DataTypeString, schema.DataTypeStringArray:
			// string/string[] are migrated to text/text[] later,
			// at this point they are still valid data types, therefore should be handled here
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

	return nil
}

func (h *Handler) validateVectorSettings(ctx context.Context, class *models.Class) error {
	if err := h.validateVectorizer(ctx, class); err != nil {
		return err
	}

	if err := h.validateVectorIndex(ctx, class); err != nil {
		return err
	}

	return nil
}

func (h *Handler) validateVectorizer(ctx context.Context, class *models.Class) error {
	if class.Vectorizer == config.VectorizerModuleNone {
		return nil
	}

	if err := h.vectorizerValidator.ValidateVectorizer(class.Vectorizer); err != nil {
		return errors.Wrap(err, "vectorizer")
	}

	return nil
}

func (h *Handler) validateVectorIndex(ctx context.Context, class *models.Class) error {
	switch class.VectorIndexType {
	case "hnsw":
		return nil
	default:
		return errors.Errorf("unrecognized or unsupported vectorIndexType %q",
			class.VectorIndexType)
	}
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
	}
	return
}

func validateShardingConfig(current, update *models.Class, mtEnabled bool, cl clusterState) error {
	if mtEnabled {
		return nil
	}
	first, ok := current.ShardingConfig.(sharding.Config)
	if !ok {
		return fmt.Errorf("current config is not well-formed")
	}
	second, ok := update.ShardingConfig.(sharding.Config)
	if !ok {
		return fmt.Errorf("updated config is not well-formed")
	}
	if err := sharding.ValidateConfigUpdate(first, second, cl); err != nil {
		return err
	}
	return nil
}

func (h *Handler) validateImmutableFields(initial, updated *models.Class) error {
	immutableFields := []immutableText{
		{
			name:     "class name",
			accessor: func(c *models.Class) string { return c.Class },
		},
		{
			name:     "vectorizer",
			accessor: func(c *models.Class) string { return c.Vectorizer },
		},
		{
			name:     "vector index type",
			accessor: func(c *models.Class) string { return c.VectorIndexType },
		},
	}

	for _, u := range immutableFields {
		if err := h.validateImmutableTextField(u, initial, updated); err != nil {
			return err
		}
	}

	if !reflect.DeepEqual(initial.Properties, updated.Properties) {
		return errors.Errorf(
			"properties cannot be updated through updating the class. Use the add " +
				"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
				"to add additional properties")
	}

	if !reflect.DeepEqual(initial.ModuleConfig, updated.ModuleConfig) {
		return errors.Errorf("module config is immutable")
	}

	return nil
}

type immutableText struct {
	accessor func(c *models.Class) string
	name     string
}

func (h *Handler) validateImmutableTextField(u immutableText,
	previous, next *models.Class,
) error {
	oldField := u.accessor(previous)
	newField := u.accessor(next)
	if oldField != newField {
		return errors.Errorf("%s is immutable: attempted change from %q to %q",
			u.name, oldField, newField)
	}

	return nil
}

func CreateClassPayload(class *models.Class,
	shardingState *sharding.State,
) (pl ClassPayload, err error) {
	pl.Name = class.Class
	if pl.Metadata, err = json.Marshal(class); err != nil {
		return pl, fmt.Errorf("marshal class %q metadata: %w", pl.Name, err)
	}
	if shardingState != nil {
		ss := *shardingState
		pl.Shards = make([]KeyValuePair, len(ss.Physical))
		i := 0
		for name, shard := range ss.Physical {
			data, err := json.Marshal(shard)
			if err != nil {
				return pl, fmt.Errorf("marshal shard %q metadata: %w", name, err)
			}
			pl.Shards[i] = KeyValuePair{Key: name, Value: data}
			i++
		}
		ss.Physical = nil
		if pl.ShardingState, err = json.Marshal(&ss); err != nil {
			return pl, fmt.Errorf("marshal class %q sharding state: %w", pl.Name, err)
		}
	}
	return pl, nil
}
