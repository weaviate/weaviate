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

func (m *Handler) GetClass(ctx context.Context, principal *models.Principal,
	name string,
) (*models.Class, error) {
	err := m.Authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return nil, err
	}
	return m.metaReader.ReadOnlyClass(name), nil
}

// AddClass to the schema
func (m *Handler) AddClass(ctx context.Context, principal *models.Principal,
	cls *models.Class,
) error {
	err := m.Authorizer.Authorize(principal, "create", "schema/objects")
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

	m.setClassDefaults(cls)

	if err := m.validateCanAddClass(ctx, cls, false); err != nil {
		return err
	}
	// migrate only after validation in completed
	m.migrateClassSettings(cls)
	if err := m.parser.ParseClass(cls); err != nil {
		return err
	}

	err = m.invertedConfigValidator(cls.InvertedIndexConfig)
	if err != nil {
		return err
	}

	shardState, err := sharding.InitState(cls.Class,
		cls.ShardingConfig.(sharding.Config),
		m.clusterState, cls.ReplicationConfig.Factor,
		schema.MultiTenancyEnabled(cls))
	if err != nil {
		return fmt.Errorf("init sharding state: %w", err)
	}

	return m.metaWriter.AddClass(cls, shardState)
}

func (m *Handler) RestoreClass(ctx context.Context, d *backup.ClassDescriptor) error {
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

	m.setClassDefaults(class)
	err = m.validateCanAddClass(ctx, class, true)
	if err != nil {
		return err
	}
	// migrate only after validation in completed
	m.migrateClassSettings(class)

	if err := m.parser.ParseClass(class); err != nil {
		return err
	}

	err = m.invertedConfigValidator(class.InvertedIndexConfig)
	if err != nil {
		return err
	}

	shardingState.MigrateFromOldFormat()
	/// TODO-RAFT START
	/// Implement RAFT based restore
	/// TODO-RAFT END
	return nil // return m.metaWriter.RestoreClass(class, &shardingState)
}

// DeleteClass from the schema
func (m *Handler) DeleteClass(ctx context.Context, principal *models.Principal, class string) error {
	err := m.Authorizer.Authorize(principal, "delete", "schema/objects")
	if err != nil {
		return err
	}

	return m.metaWriter.DeleteClass(class)
}

func (m *Handler) UpdateClass(ctx context.Context, principal *models.Principal,
	className string, updated *models.Class,
) error {
	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	initial := m.metaReader.ReadOnlyClass(className)
	if initial == nil {
		return ErrNotFound
	}
	mtEnabled, err := validateUpdatingMT(initial, updated)
	if err != nil {
		return err
	}

	// make sure unset optionals on 'updated' don't lead to an error, as all
	// optionals would have been set with defaults on the initial already
	m.setClassDefaults(updated)

	if err := m.validateImmutableFields(initial, updated); err != nil {
		return err
	}

	// run target vectors validation first, as it will reject classes
	// where legacy vector was changed to target vectors and vice versa
	if err := validateVectorConfigsParityAndImmutables(initial, updated); err != nil {
		return err
	}

	if err := validateVectorIndexConfigImmutableFields(initial, updated); err != nil {
		return err
	}

	if err := m.validateVectorSettings(updated); err != nil {
		return err
	}

	if err := m.parser.ParseClass(updated); err != nil {
		return err
	}

	if hasTargetVectors(updated) {
		if err := m.migrator.ValidateVectorIndexConfigsUpdate(ctx,
			asVectorIndexConfigs(initial), asVectorIndexConfigs(updated),
		); err != nil {
			return err
		}
	} else {
		if err := m.migrator.ValidateVectorIndexConfigUpdate(ctx,
			initial.VectorIndexConfig.(schema.VectorIndexConfig),
			updated.VectorIndexConfig.(schema.VectorIndexConfig)); err != nil {
			return errors.Wrap(err, "vector index config")
		}
	}

	if err := m.migrator.ValidateVectorIndexConfigUpdate(ctx,
		initial.VectorIndexConfig.(schema.VectorIndexConfig),
		updated.VectorIndexConfig.(schema.VectorIndexConfig)); err != nil {
		return errors.Wrap(err, "vector index config")
	}

	if err := m.migrator.ValidateInvertedIndexConfigUpdate(ctx,
		initial.InvertedIndexConfig, updated.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}
	if err := validateShardingConfig(initial, updated, mtEnabled, m.clusterState); err != nil {
		return fmt.Errorf("validate sharding config: %w", err)
	}

	if err := replica.ValidateConfigUpdate(initial, updated, m.clusterState); err != nil {
		return fmt.Errorf("replication config: %w", err)
	}

	initialRF := initial.ReplicationConfig.Factor
	updatedRF := updated.ReplicationConfig.Factor
	// TODO-RAFT: implement raft based scaling
	if initialRF != updatedRF {
		return fmt.Errorf("scaling is not implemented")
	}
	return m.metaWriter.UpdateClass(updated, nil)
}

func (m *Manager) setClassDefaults(class *models.Class) {
	// set only when no target vectors configured
	if !hasTargetVectors(class) {
		if class.Vectorizer == "" {
			class.Vectorizer = m.config.DefaultVectorizerModule
		}

		if class.VectorIndexType == "" {
			class.VectorIndexType = "hnsw"
		}

		if m.config.DefaultVectorDistanceMetric != "" {
			if class.VectorIndexConfig == nil {
				class.VectorIndexConfig = map[string]interface{}{"distance": m.config.DefaultVectorDistanceMetric}
			} else if class.VectorIndexConfig.(map[string]interface{})["distance"] == nil {
				class.VectorIndexConfig.(map[string]interface{})["distance"] = m.config.DefaultVectorDistanceMetric
			}
		}
	}

	setInvertedConfigDefaults(class)
	for _, prop := range class.Properties {
		setPropertyDefaults(prop)
	}

	m.moduleConfig.SetClassDefaults(class)
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

func (m *Handler) migrateClassSettings(class *models.Class) {
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

func (m *Handler) validateProperty(
	property *models.Property, class *models.Class,
	existingPropertyNames map[string]bool, relaxCrossRefValidation bool,
) error {
	if _, err := schema.ValidatePropertyName(property.Name); err != nil {
		return err
	}

	if err := schema.ValidateReservedPropertyName(property.Name); err != nil {
		return err
	}

	if existingPropertyNames[strings.ToLower(property.Name)] {
		return fmt.Errorf("class %q: conflict for property %q: already in use or provided multiple times",
			class.Class, property.Name)
	}

	// Validate data type of property.
	sch := m.getSchema()

	propertyDataType, err := (&sch).FindPropertyDataTypeWithRefs(property.DataType,
		relaxCrossRefValidation, schema.ClassName(class.Class))
	if err != nil {
		return fmt.Errorf("property '%s': invalid dataType: %v", property.Name, err)
	}

	if propertyDataType.IsNested() {
		if err := validateNestedProperties(property.NestedProperties, property.Name); err != nil {
			return err
		}
	} else {
		if len(property.NestedProperties) > 0 {
			return fmt.Errorf("property '%s': nestedProperties not allowed for data types other than object/object[]",
				property.Name)
		}
	}

	if err := m.validatePropertyTokenization(property.Tokenization, propertyDataType); err != nil {
		return err
	}

	if err := m.validatePropertyIndexing(property); err != nil {
		return err
	}

	if err := m.validatePropModuleConfig(class, property); err != nil {
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

func (m *Handler) validateCanAddClass(
	ctx context.Context, class *models.Class,
	relaxCrossRefValidation bool,
) error {
	if err := m.validateClassNameUniqueness(class.Class); err != nil {
		return err
	}

	if err := m.validateClassName(ctx, class.Class); err != nil {
		return err
	}

	existingPropertyNames := map[string]bool{}
	for _, property := range class.Properties {
		if err := m.validateProperty(property, class, existingPropertyNames, relaxCrossRefValidation); err != nil {
			return err
		}
		existingPropertyNames[strings.ToLower(property.Name)] = true
	}

	if err := m.validateVectorSettings(class); err != nil {
		return err
	}

	if err := m.moduleConfig.ValidateClass(ctx, class); err != nil {
		return err
	}

	if err := replica.ValidateConfig(class, m.config.Replication); err != nil {
		return err
	}

	// all is fine!
	return nil
}

func (m *Handler) validateClassName(name string) error {
	if _, err := schema.ValidateClassName(name); err != nil {
		return err
	}
	existingName := m.metaReader.ClassEqual(name)
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

func (m *Handler) validatePropertyTokenization(tokenization string, propertyDataType schema.PropertyDataType) error {
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

func (m *Handler) validatePropertyIndexing(prop *models.Property) error {
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

func (m *Handler) validateVectorSettings(class *models.Class) error {
	if !hasTargetVectors(class) {
		if err := m.validateVectorizer(class.Vectorizer); err != nil {
			return err
		}
		if err := m.validateVectorIndexType(class.VectorIndexType); err != nil {
			return err
		}
		return nil
	}

	if class.Vectorizer != "" {
		return fmt.Errorf("class.vectorizer %q can not be set if class.vectorConfig is configured", class.Vectorizer)
	}
	if class.VectorIndexType != "" {
		return fmt.Errorf("class.vectorIndexType %q can not be set if class.vectorConfig is configured", class.VectorIndexType)
	}

	for name, cfg := range class.VectorConfig {
		// check only if vectorizer correctly configured (map with single key being vectorizer name)
		// other cases are handled in module config validation
		if vm, ok := cfg.Vectorizer.(map[string]interface{}); ok && len(vm) == 1 {
			for vectorizer := range vm {
				if err := m.validateVectorizer(vectorizer); err != nil {
					return fmt.Errorf("target vector %q: %w", name, err)
				}
			}
		}
		if err := m.validateVectorIndexType(cfg.VectorIndexType); err != nil {
			return fmt.Errorf("target vector %q: %w", name, err)
		}
	}
	return nil
}

func (m *Handler) validateVectorizer(vectorizer string) error {
	if vectorizer == config.VectorizerModuleNone {
		return nil
	}

	if err := m.vectorizerValidator.ValidateVectorizer(vectorizer); err != nil {
		return errors.Wrap(err, "vectorizer")
	}

	return nil
}

func (m *Handler) validateVectorIndexType(vectorIndexType string) error {
	switch vectorIndexType {
	case "hnsw", "flat":
		return nil
	default:
		return errors.Errorf("unrecognized or unsupported vectorIndexType %q",
			vectorIndexType)
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

func (m *Handler) validateImmutableFields(initial, updated *models.Class) error {
	immutableFields := []immutableText{
		{
			name:     "class name",
			accessor: func(c *models.Class) string { return c.Class },
		},
	}

	if err := validateImmutableTextFields(initial, updated, immutableFields...); err != nil {
		return err
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

func validateVectorConfigsParityAndImmutables(initial, updated *models.Class) error {
	initialVecCount := len(initial.VectorConfig)
	updatedVecCount := len(updated.VectorConfig)

	// no cfgs for target vectors
	if initialVecCount == 0 && updatedVecCount == 0 {
		return nil
	}
	// no cfgs for target vectors in initial
	if initialVecCount == 0 && updatedVecCount > 0 {
		return fmt.Errorf("additional configs for vectors")
	}
	// no cfgs for target vectors in updated
	if initialVecCount > 0 && updatedVecCount == 0 {
		return fmt.Errorf("missing configs for vectors")
	}

	// matching cfgs on both sides
	for vecName := range initial.VectorConfig {
		if _, ok := updated.VectorConfig[vecName]; !ok {
			return fmt.Errorf("missing config for vector %q", vecName)
		}
	}

	if initialVecCount != updatedVecCount {
		for vecName := range updated.VectorConfig {
			if _, ok := initial.VectorConfig[vecName]; !ok {
				return fmt.Errorf("additional config for vector %q", vecName)
			}
		}
		// fallback, error should be returned in loop
		return fmt.Errorf("number of configs for vectors does not match")
	}

	// compare matching cfgs
	for vecName, initialCfg := range initial.VectorConfig {
		updatedCfg := updated.VectorConfig[vecName]

		// immutable vector type
		if initialCfg.VectorIndexType != updatedCfg.VectorIndexType {
			return fmt.Errorf("vector index type of vector %q is immutable: attempted change from %q to %q",
				vecName, initialCfg.VectorIndexType, updatedCfg.VectorIndexType)
		}

		// immutable vectorizer
		if imap, ok := initialCfg.Vectorizer.(map[string]interface{}); ok && len(imap) == 1 {
			umap, ok := updatedCfg.Vectorizer.(map[string]interface{})
			if !ok || len(umap) != 1 {
				return fmt.Errorf("invalid vectorizer config for vector %q", vecName)
			}

			ivectorizer := ""
			for k := range imap {
				ivectorizer = k
			}
			uvectorizer := ""
			for k := range umap {
				uvectorizer = k
			}

			if ivectorizer != uvectorizer {
				return fmt.Errorf("vectorizer of vector %q is immutable: attempted change from %q to %q",
					vecName, ivectorizer, uvectorizer)
			}
		}
	}
	return nil
}

func validateVectorIndexConfigImmutableFields(initial, updated *models.Class) error {
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

func asVectorIndexConfigs(c *models.Class) map[string]schema.VectorIndexConfig {
	if c.VectorConfig == nil {
		return nil
	}

	cfgs := map[string]schema.VectorIndexConfig{}
	for vecName := range c.VectorConfig {
		cfgs[vecName] = c.VectorConfig[vecName].VectorIndexConfig.(schema.VectorIndexConfig)
	}
	return cfgs
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
