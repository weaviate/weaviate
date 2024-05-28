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

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingcfg "github.com/weaviate/weaviate/usecases/sharding/config"
)

func (h *Handler) GetClass(ctx context.Context, principal *models.Principal, name string) (*models.Class, error) {
	if err := h.Authorizer.Authorize(principal, "list", "schema/*"); err != nil {
		return nil, err
	}
	cl := h.metaReader.ReadOnlyClass(name)
	return cl, nil
}

func (h *Handler) GetConsistentClass(ctx context.Context, principal *models.Principal,
	name string, consistency bool,
) (*models.Class, uint64, error) {
	if err := h.Authorizer.Authorize(principal, "list", "schema/*"); err != nil {
		return nil, 0, err
	}
	if consistency {
		vclasses, err := h.metaWriter.QueryReadOnlyClasses(name)
		return vclasses[name].Class, vclasses[name].Version, err
	}
	class, _ := h.metaReader.ReadOnlyClassWithVersion(ctx, name, 0)
	return class, 0, nil
}

func (h *Handler) GetCachedClass(ctxWithClassCache context.Context,
	principal *models.Principal, names ...string,
) (map[string]versioned.Class, error) {
	if err := h.Authorizer.Authorize(principal, "list", "schema/*"); err != nil {
		return nil, err
	}

	return classcache.ClassesFromContext(ctxWithClassCache, func(names ...string) (map[string]versioned.Class, error) {
		vclasses, err := h.metaWriter.QueryReadOnlyClasses(names...)
		if err != nil {
			return nil, err
		}

		if len(vclasses) == 0 {
			return nil, nil
		}

		for _, vclass := range vclasses {
			if err := h.parser.ParseClass(vclass.Class); err != nil {
				// remove invalid classes
				h.logger.WithFields(logrus.Fields{
					"Class": vclass.Class.Class,
					"Error": err,
				}).Warn("parsing class error")
				delete(vclasses, vclass.Class.Class)
				continue
			}
		}

		return vclasses, nil
	}, names...)
}

// AddClass to the schema
func (h *Handler) AddClass(ctx context.Context, principal *models.Principal,
	cls *models.Class,
) (*models.Class, uint64, error) {
	err := h.Authorizer.Authorize(principal, "create", "schema/objects")
	if err != nil {
		return nil, 0, err
	}

	cls.Class = schema.UppercaseClassName(cls.Class)
	cls.Properties = schema.LowercaseAllPropertyNames(cls.Properties)
	if cls.ShardingConfig != nil && schema.MultiTenancyEnabled(cls) {
		return nil, 0, fmt.Errorf("cannot have both shardingConfig and multiTenancyConfig")
	} else if cls.MultiTenancyConfig == nil {
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{}
	} else if cls.MultiTenancyConfig.Enabled {
		cls.ShardingConfig = shardingcfg.Config{DesiredCount: 0} // tenant shards will be created dynamically
	}

	h.setClassDefaults(cls)

	if err := h.validateCanAddClass(ctx, cls, false); err != nil {
		return nil, 0, err
	}
	// migrate only after validation in completed
	h.migrateClassSettings(cls)
	if err := h.parser.ParseClass(cls); err != nil {
		return nil, 0, err
	}

	err = h.invertedConfigValidator(cls.InvertedIndexConfig)
	if err != nil {
		return nil, 0, err
	}

	shardState, err := sharding.InitState(cls.Class,
		cls.ShardingConfig.(shardingcfg.Config),
		h.clusterState, cls.ReplicationConfig.Factor,
		schema.MultiTenancyEnabled(cls))
	if err != nil {
		return nil, 0, fmt.Errorf("init sharding state: %w", err)
	}
	version, err := h.metaWriter.AddClass(cls, shardState)
	if err != nil {
		return nil, 0, err
	}
	return cls, version, err
}

func (h *Handler) RestoreClass(ctx context.Context, d *backup.ClassDescriptor, m map[string]string) error {
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
	shardingState.ApplyNodeMapping(m)
	_, err = h.metaWriter.RestoreClass(class, &shardingState)
	return err
}

// DeleteClass from the schema
func (h *Handler) DeleteClass(ctx context.Context, principal *models.Principal, class string) error {
	err := h.Authorizer.Authorize(principal, "delete", "schema/objects")
	if err != nil {
		return err
	}

	_, err = h.metaWriter.DeleteClass(class)
	return err
}

func (h *Handler) UpdateClass(ctx context.Context, principal *models.Principal,
	className string, updated *models.Class,
) error {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil || updated == nil {
		return err
	}

	// make sure unset optionals on 'updated' don't lead to an error, as all
	// optionals would have been set with defaults on the initial already
	h.setClassDefaults(updated)
	if err := h.validateVectorSettings(updated); err != nil {
		return err
	}

	initial := h.metaReader.ReadOnlyClass(className)
	var shardingState *sharding.State

	// first layer of defense is basic validation if class already exists
	if initial != nil {
		_, err := validateUpdatingMT(initial, updated)
		if err != nil {
			return err
		}

		initialRF := initial.ReplicationConfig.Factor
		updatedRF := updated.ReplicationConfig.Factor

		if initialRF != updatedRF {
			ss, _, err := h.metaWriter.QueryShardingState(className)
			if err != nil {
				return fmt.Errorf("query sharding state for %q: %w", className, err)
			}
			shardingState, err = h.scaleOut.Scale(ctx, className, ss.Config, initialRF, updatedRF)
			if err != nil {
				return fmt.Errorf(
					"scale %q from %d replicas to %d: %w",
					className, initialRF, updatedRF, err)
			}
		}

		if err := validateImmutableFields(initial, updated); err != nil {
			return err
		}
	}

	_, err = h.metaWriter.UpdateClass(updated, shardingState)
	return err
}

func (h *Handler) setClassDefaults(class *models.Class) {
	// set only when no target vectors configured
	if !hasTargetVectors(class) {
		if class.Vectorizer == "" {
			class.Vectorizer = h.config.DefaultVectorizerModule
		}

		if class.VectorIndexType == "" {
			class.VectorIndexType = vectorindex.DefaultVectorIndexType
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
		class.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	}

	h.moduleConfig.SetClassDefaults(class)
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
			prop.IndexSearchable == nil {
			continue
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

		if isPrimitive && primitiveDataType == schema.DataTypeBlob {
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
	for _, prop := range props {
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
}

func (h *Handler) validateProperty(
	class *models.Class, existingPropertyNames map[string]bool,
	relaxCrossRefValidation bool, props ...*models.Property,
) error {
	for _, property := range props {
		if _, err := schema.ValidatePropertyName(property.Name); err != nil {
			return err
		}

		if err := schema.ValidateReservedPropertyName(property.Name); err != nil {
			return err
		}

		if existingPropertyNames[strings.ToLower(property.Name)] {
			return fmt.Errorf("class %q: conflict for property %q: already in use or provided multiple times", property.Name, class.Class)
		}

		// Validate data type of property.
		propertyDataType, err := schema.FindPropertyDataTypeWithRefs(h.metaReader.ReadOnlyClass, property.DataType,
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

		if err := h.validatePropertyTokenization(property.Tokenization, propertyDataType); err != nil {
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
	if _, err := schema.ValidateClassName(class.Class); err != nil {
		return err
	}

	existingPropertyNames := map[string]bool{}
	for _, property := range class.Properties {
		if err := h.validateProperty(class, existingPropertyNames, relaxCrossRefValidation, property); err != nil {
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
				models.PropertyTokenizationWhitespace, models.PropertyTokenizationLowercase, models.PropertyTokenizationTrigram, models.PropertyTokenizationGse:
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

	if propertyDataType.IsNested() {
		return fmt.Errorf("Tokenization is not allowed for object/object[] data types")
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

func (h *Handler) validateVectorIndexType(vectorIndexType string) error {
	switch vectorIndexType {
	case vectorindex.VectorIndexTypeHNSW, vectorindex.VectorIndexTypeFLAT, vectorindex.VectorIndexTypeDYNAMIC:
		return nil
	default:
		return errors.Errorf("unrecognized or unsupported vectorIndexType %q",
			vectorIndexType)
	}
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

	return
}

func validateImmutableFields(initial, updated *models.Class) error {
	immutableFields := []immutableText{
		{
			name:     "class name",
			accessor: func(c *models.Class) string { return c.Class },
		},
	}

	if err := validateImmutableTextFields(initial, updated, immutableFields...); err != nil {
		return err
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
