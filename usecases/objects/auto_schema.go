//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

type AutoSchemaManager struct {
	mutex         sync.RWMutex
	authorizer    authorization.Authorizer
	schemaManager schemaManager
	vectorRepo    VectorRepo
	config        config.AutoSchema
	logger        logrus.FieldLogger

	// Metrics without labels to avoid cardinality issues
	opsDuration  *prometheus.HistogramVec
	tenantsCount prometheus.Counter
}

func NewAutoSchemaManager(schemaManager schemaManager, vectorRepo VectorRepo,
	config *config.WeaviateConfig, authorizer authorization.Authorizer, logger logrus.FieldLogger,
	reg prometheus.Registerer,
) *AutoSchemaManager {
	r := promauto.With(reg)

	tenantsCount := r.NewCounter(
		prometheus.CounterOpts{
			Name: "weaviate_auto_tenant_total",
			Help: "Total number of tenants processed",
		},
	)

	opDuration := r.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "weaviate_auto_tenant_duration_seconds",
			Help: "Time spent in auto tenant operations",
		},
		[]string{"operation"},
	)

	return &AutoSchemaManager{
		schemaManager: schemaManager,
		vectorRepo:    vectorRepo,
		config:        config.Config.AutoSchema,
		logger:        logger,
		authorizer:    authorizer,
		tenantsCount:  tenantsCount,
		opsDuration:   opDuration,
	}
}

func (m *AutoSchemaManager) autoSchema(ctx context.Context, principal *models.Principal,
	allowCreateClass bool, classes map[string]versioned.Class, objects ...*models.Object,
) (uint64, error) {
	enabled := m.config.Enabled.Get()

	if !enabled {
		return 0, nil
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	var maxSchemaVersion uint64

	for _, object := range objects {
		if object == nil {
			return 0, ErrInvalidUserInput{validation.ErrorMissingObject}
		}

		if len(object.Class) == 0 {
			// stop performing auto schema
			return 0, ErrInvalidUserInput{validation.ErrorMissingClass}
		}

		vclass := classes[object.Class]

		schemaClass := vclass.Class
		schemaVersion := vclass.Version

		if schemaClass == nil && !allowCreateClass {
			return 0, ErrInvalidUserInput{"given class does not exist"}
		}
		properties, err := m.getProperties(object)
		if err != nil {
			return 0, err
		}

		if schemaClass == nil {
			err := m.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.CollectionsMetadata(object.Class)...)
			if err != nil {
				return 0, fmt.Errorf("auto schema can't create objects because can't create collection: %w", err)
			}

			// it returns the newly created class and version
			schemaClass, schemaVersion, err = m.createClass(ctx, principal, object.Class, properties)
			if err != nil {
				return 0, err
			}

			classes[object.Class] = versioned.Class{Class: schemaClass, Version: schemaVersion}
			classcache.RemoveClassFromContext(ctx, object.Class)
		} else {
			if newProperties := schema.DedupProperties(schemaClass.Properties, properties); len(newProperties) > 0 {
				err := m.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.CollectionsMetadata(schemaClass.Class)...)
				if err != nil {
					return 0, fmt.Errorf("auto schema can't create objects because can't update collection: %w", err)
				}
				schemaClass, schemaVersion, err = m.schemaManager.AddClassProperty(ctx,
					principal, schemaClass, schemaClass.Class, true, newProperties...)
				if err != nil {
					return 0, err
				}
				classes[object.Class] = versioned.Class{Class: schemaClass, Version: schemaVersion}
				classcache.RemoveClassFromContext(ctx, object.Class)
			}
		}

		if schemaVersion > maxSchemaVersion {
			maxSchemaVersion = schemaVersion
		}
	}
	return maxSchemaVersion, nil
}

func (m *AutoSchemaManager) createClass(ctx context.Context, principal *models.Principal,
	className string, properties []*models.Property,
) (*models.Class, uint64, error) {
	now := time.Now()
	class := &models.Class{
		Class:       className,
		Properties:  properties,
		Description: "This property was generated by Weaviate's auto-schema feature on " + now.Format(time.ANSIC),
	}
	m.logger.
		WithField("auto_schema", "createClass").
		Debugf("create class %s", className)
	newClass, schemaVersion, err := m.schemaManager.AddClass(ctx, principal, class)
	return newClass, schemaVersion, err
}

func (m *AutoSchemaManager) getProperties(object *models.Object) ([]*models.Property, error) {
	properties := []*models.Property{}
	if props, ok := object.Properties.(map[string]interface{}); ok {
		for name, value := range props {
			now := time.Now()
			dt, err := m.determineType(value, false)
			if err != nil {
				return nil, fmt.Errorf("property '%s' on class '%s': %w", name, object.Class, err)
			}

			var nestedProperties []*models.NestedProperty
			if len(dt) == 1 {
				switch dt[0] {
				case schema.DataTypeObject:
					nestedProperties, err = m.determineNestedProperties(value.(map[string]interface{}), now)
				case schema.DataTypeObjectArray:
					nestedProperties, err = m.determineNestedPropertiesOfArray(value.([]interface{}), now)
				default:
					// do nothing
				}
			}
			if err != nil {
				return nil, fmt.Errorf("property '%s' on class '%s': %w", name, object.Class, err)
			}

			property := &models.Property{
				Name:             name,
				DataType:         m.getDataTypes(dt),
				Description:      "This property was generated by Weaviate's auto-schema feature on " + now.Format(time.ANSIC),
				NestedProperties: nestedProperties,
			}
			properties = append(properties, property)
		}
	}
	return properties, nil
}

func (m *AutoSchemaManager) getDataTypes(dataTypes []schema.DataType) []string {
	dtypes := make([]string, len(dataTypes))
	for i := range dataTypes {
		dtypes[i] = string(dataTypes[i])
	}
	return dtypes
}

func (m *AutoSchemaManager) determineType(value interface{}, ofNestedProp bool) ([]schema.DataType, error) {
	fallbackDataType := []schema.DataType{schema.DataTypeText}
	fallbackArrayDataType := []schema.DataType{schema.DataTypeTextArray}

	switch typedValue := value.(type) {
	case string:
		if _, err := time.Parse(time.RFC3339, typedValue); err == nil {
			return []schema.DataType{schema.DataType(m.config.DefaultDate)}, nil
		}
		if _, err := uuid.Parse(typedValue); err == nil {
			return []schema.DataType{schema.DataTypeUUID}, nil
		}
		if m.config.DefaultString != "" {
			return []schema.DataType{schema.DataType(m.config.DefaultString)}, nil
		}
		return []schema.DataType{schema.DataTypeText}, nil
	case json.Number:
		return []schema.DataType{schema.DataType(m.config.DefaultNumber)}, nil
	case float64:
		return []schema.DataType{schema.DataTypeNumber}, nil
	case int64:
		return []schema.DataType{schema.DataTypeInt}, nil
	case bool:
		return []schema.DataType{schema.DataTypeBoolean}, nil
	case map[string]interface{}:
		// nested properties does not support phone and geo data types
		if !ofNestedProp {
			if dt, ok := m.asGeoCoordinatesType(typedValue); ok {
				return dt, nil
			}
			if dt, ok := m.asPhoneNumber(typedValue); ok {
				return dt, nil
			}
		}
		return []schema.DataType{schema.DataTypeObject}, nil
	case []interface{}:
		if len(typedValue) == 0 {
			return fallbackArrayDataType, nil
		}

		refDataTypes := []schema.DataType{}
		var isRef bool
		var determinedDataType schema.DataType

		for i := range typedValue {
			dataType, refDataType, err := m.determineArrayType(typedValue[i], ofNestedProp)
			if err != nil {
				return nil, fmt.Errorf("element [%d]: %w", i, err)
			}
			if i == 0 {
				isRef = refDataType != ""
				determinedDataType = dataType
			}
			if dataType != "" {
				// if an array contains text and UUID/Date, the type should be text
				if determinedDataType == schema.DataTypeTextArray && (dataType == schema.DataTypeUUIDArray || dataType == schema.DataTypeDateArray) {
					continue
				}
				if determinedDataType == schema.DataTypeDateArray && (dataType == schema.DataTypeUUIDArray || dataType == schema.DataTypeTextArray) {
					determinedDataType = schema.DataTypeTextArray
					continue
				}
				if determinedDataType == schema.DataTypeUUIDArray && (dataType == schema.DataTypeDateArray || dataType == schema.DataTypeTextArray) {
					determinedDataType = schema.DataTypeTextArray
					continue
				}

				if isRef {
					return nil, fmt.Errorf("element [%d]: mismatched data type - reference expected, got '%s'",
						i, asSingleDataType(dataType))
				}
				if dataType != determinedDataType {
					return nil, fmt.Errorf("element [%d]: mismatched data type - '%s' expected, got '%s'",
						i, asSingleDataType(determinedDataType), asSingleDataType(dataType))
				}
			} else {
				if !isRef {
					return nil, fmt.Errorf("element [%d]: mismatched data type - '%s' expected, got reference",
						i, asSingleDataType(determinedDataType))
				}
				refDataTypes = append(refDataTypes, refDataType)
			}
		}
		if len(refDataTypes) > 0 {
			return refDataTypes, nil
		}
		return []schema.DataType{determinedDataType}, nil
	case nil:
		return fallbackDataType, nil
	default:
		allowed := []string{
			schema.DataTypeText.String(),
			schema.DataTypeNumber.String(),
			schema.DataTypeInt.String(),
			schema.DataTypeBoolean.String(),
			schema.DataTypeDate.String(),
			schema.DataTypeUUID.String(),
			schema.DataTypeObject.String(),
		}
		if !ofNestedProp {
			allowed = append(allowed, schema.DataTypePhoneNumber.String(), schema.DataTypeGeoCoordinates.String())
		}
		return nil, fmt.Errorf("unrecognized data type of value '%v' - one of '%s' expected",
			typedValue, strings.Join(allowed, "', '"))
	}
}

func asSingleDataType(arrayDataType schema.DataType) schema.DataType {
	if dt, isArray := schema.IsArrayType(arrayDataType); isArray {
		return dt
	}
	return arrayDataType
}

func (m *AutoSchemaManager) determineArrayType(value interface{}, ofNestedProp bool,
) (schema.DataType, schema.DataType, error) {
	switch typedValue := value.(type) {
	case string:
		if _, err := time.Parse(time.RFC3339, typedValue); err == nil {
			return schema.DataTypeDateArray, "", nil
		}
		if _, err := uuid.Parse(typedValue); err == nil {
			return schema.DataTypeUUIDArray, "", nil
		}
		if schema.DataType(m.config.DefaultString) == schema.DataTypeString {
			return schema.DataTypeStringArray, "", nil
		}
		return schema.DataTypeTextArray, "", nil
	case json.Number:
		if schema.DataType(m.config.DefaultNumber) == schema.DataTypeInt {
			return schema.DataTypeIntArray, "", nil
		}
		return schema.DataTypeNumberArray, "", nil
	case float64:
		return schema.DataTypeNumberArray, "", nil
	case int64:
		return schema.DataTypeIntArray, "", nil
	case bool:
		return schema.DataTypeBooleanArray, "", nil
	case map[string]interface{}:
		if ofNestedProp {
			return schema.DataTypeObjectArray, "", nil
		}
		if refDataType, ok := m.asRef(typedValue); ok {
			return "", refDataType, nil
		}
		return schema.DataTypeObjectArray, "", nil
	default:
		allowed := []string{
			schema.DataTypeText.String(),
			schema.DataTypeNumber.String(),
			schema.DataTypeInt.String(),
			schema.DataTypeBoolean.String(),
			schema.DataTypeDate.String(),
			schema.DataTypeUUID.String(),
			schema.DataTypeObject.String(),
		}
		if !ofNestedProp {
			allowed = append(allowed, schema.DataTypeCRef.String())
		}
		return "", "", fmt.Errorf("unrecognized data type of value '%v' - one of '%s' expected",
			typedValue, strings.Join(allowed, "', '"))
	}
}

func (m *AutoSchemaManager) asGeoCoordinatesType(val map[string]interface{}) ([]schema.DataType, bool) {
	if len(val) == 2 {
		if val["latitude"] != nil && val["longitude"] != nil {
			return []schema.DataType{schema.DataTypeGeoCoordinates}, true
		}
	}
	return nil, false
}

func (m *AutoSchemaManager) asPhoneNumber(val map[string]interface{}) ([]schema.DataType, bool) {
	if val["input"] != nil {
		if len(val) == 1 {
			return []schema.DataType{schema.DataTypePhoneNumber}, true
		}
		if len(val) == 2 {
			if _, ok := val["defaultCountry"]; ok {
				return []schema.DataType{schema.DataTypePhoneNumber}, true
			}
		}
	}

	return nil, false
}

func (m *AutoSchemaManager) asRef(val map[string]interface{}) (schema.DataType, bool) {
	if v, ok := val["beacon"]; ok {
		if beacon, ok := v.(string); ok {
			ref, err := crossref.Parse(beacon)
			if err == nil {
				if ref.Class == "" {
					res, err := m.vectorRepo.ObjectByID(context.Background(), ref.TargetID, search.SelectProperties{}, additional.Properties{}, "")
					if err == nil && res != nil {
						return schema.DataType(res.ClassName), true
					}
				} else {
					return schema.DataType(ref.Class), true
				}
			}
		}
	}
	return "", false
}

func (m *AutoSchemaManager) determineNestedProperties(values map[string]interface{}, now time.Time,
) ([]*models.NestedProperty, error) {
	i := 0
	nestedProperties := make([]*models.NestedProperty, len(values))
	for name, value := range values {
		np, err := m.determineNestedProperty(name, value, now)
		if err != nil {
			return nil, fmt.Errorf("nested property '%s': %w", name, err)
		}
		nestedProperties[i] = np
		i++
	}
	return nestedProperties, nil
}

func (m *AutoSchemaManager) determineNestedProperty(name string, value interface{}, now time.Time,
) (*models.NestedProperty, error) {
	dt, err := m.determineType(value, true)
	if err != nil {
		return nil, err
	}

	var np []*models.NestedProperty
	if len(dt) == 1 {
		switch dt[0] {
		case schema.DataTypeObject:
			np, err = m.determineNestedProperties(value.(map[string]interface{}), now)
		case schema.DataTypeObjectArray:
			np, err = m.determineNestedPropertiesOfArray(value.([]interface{}), now)
		default:
			// do nothing
		}
	}
	if err != nil {
		return nil, err
	}

	return &models.NestedProperty{
		Name:     name,
		DataType: m.getDataTypes(dt),
		Description: "This nested property was generated by Weaviate's auto-schema feature on " +
			now.Format(time.ANSIC),
		NestedProperties: np,
	}, nil
}

func (m *AutoSchemaManager) determineNestedPropertiesOfArray(valArray []interface{}, now time.Time,
) ([]*models.NestedProperty, error) {
	if len(valArray) == 0 {
		return []*models.NestedProperty{}, nil
	}
	nestedProperties, err := m.determineNestedProperties(valArray[0].(map[string]interface{}), now)
	if err != nil {
		return nil, err
	}
	if len(valArray) == 1 {
		return nestedProperties, nil
	}

	nestedPropertiesIndexMap := make(map[string]int, len(nestedProperties))
	for index := range nestedProperties {
		nestedPropertiesIndexMap[nestedProperties[index].Name] = index
	}

	for i := 1; i < len(valArray); i++ {
		values := valArray[i].(map[string]interface{})
		for name, value := range values {
			index, ok := nestedPropertiesIndexMap[name]
			if !ok {
				np, err := m.determineNestedProperty(name, value, now)
				if err != nil {
					return nil, err
				}
				nestedPropertiesIndexMap[name] = len(nestedProperties)
				nestedProperties = append(nestedProperties, np)
			} else if _, isNested := schema.AsNested(nestedProperties[index].DataType); isNested {
				np, err := m.determineNestedProperty(name, value, now)
				if err != nil {
					return nil, err
				}
				if mergedNestedProperties, merged := schema.MergeRecursivelyNestedProperties(
					nestedProperties[index].NestedProperties, np.NestedProperties,
				); merged {
					nestedProperties[index].NestedProperties = mergedNestedProperties
				}
			}
		}
	}

	return nestedProperties, nil
}

func (m *AutoSchemaManager) autoTenants(ctx context.Context,
	principal *models.Principal, objects []*models.Object, fetchedClasses map[string]versioned.Class,
) (uint64, int, error) {
	start := time.Now()
	defer func() {
		m.opsDuration.With(prometheus.Labels{
			"operation": "total",
		}).Observe(time.Since(start).Seconds())
	}()

	classTenants := make(map[string]map[string]struct{})

	// group by tenants by class
	for _, obj := range objects {
		if _, ok := classTenants[obj.Class]; !ok {
			classTenants[obj.Class] = map[string]struct{}{}
		}
		classTenants[obj.Class][obj.Tenant] = struct{}{}
	}

	totalTenants := 0
	// skip invalid classes, non-MT classes, no auto tenant creation classes
	var maxSchemaVersion uint64
	for className, tenantNames := range classTenants {
		vclass, exists := fetchedClasses[className]
		if !exists || // invalid class
			vclass.Class == nil { // class is nil
			continue
		}
		totalTenants += len(tenantNames)

		if !schema.MultiTenancyEnabled(vclass.Class) || // non-MT class
			!vclass.Class.MultiTenancyConfig.AutoTenantCreation { // no auto tenant creation
			continue
		}
		names := make([]string, len(tenantNames))
		tenants := make([]*models.Tenant, len(tenantNames))
		i := 0
		for name := range tenantNames {
			names[i] = name
			tenants[i] = &models.Tenant{Name: name}
			i++
		}
		err := m.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.ShardsMetadata(className, names...)...)
		if err != nil {
			return 0, totalTenants, fmt.Errorf("add tenants because can't create collection: %w", err)
		}

		addStart := time.Now()
		if err := m.addTenants(ctx, principal, className, tenants); err != nil {
			return 0, totalTenants, fmt.Errorf("add tenants to class %q: %w", className, err)
		}
		m.tenantsCount.Add(float64(len(tenants)))
		m.opsDuration.With(prometheus.Labels{
			"operation": "add",
		}).Observe(time.Since(addStart).Seconds())

		if vclass.Version > maxSchemaVersion {
			maxSchemaVersion = vclass.Version
		}
	}

	if totalTenants == 0 {
		// if we exclusively hit non-MT classes, count them as a single tenant
		totalTenants = 1
	}

	return maxSchemaVersion, totalTenants, nil
}

func (m *AutoSchemaManager) addTenants(ctx context.Context, principal *models.Principal,
	class string, tenants []*models.Tenant,
) error {
	if len(tenants) == 0 {
		return fmt.Errorf(
			"tenants must be included for multitenant-enabled class %q", class)
	}
	version, err := m.schemaManager.AddTenants(ctx, principal, class, tenants)
	if err != nil {
		return err
	}

	err = m.schemaManager.WaitForUpdate(ctx, version)
	if err != nil {
		return fmt.Errorf("could not wait for update: %w", err)
	}

	return nil
}
