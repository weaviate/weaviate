package kinds

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/validation"
	"github.com/go-openapi/strfmt"
)

// AddActionReference Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddActionReference(ctx context.Context, id strfmt.UUID,
	propertyName string, property *models.SingleRef) error {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	classSchema := schemaLock.GetSchema()
	schemaManager := schemaLock.SchemaManager()
	dbConnector := schemaLock.Connector()

	return m.addActionReferenceToConnectorAndSchema(ctx, id, propertyName, property,
		dbConnector, classSchema, schemaManager)
}

func (m *Manager) addActionReferenceToConnectorAndSchema(ctx context.Context, id strfmt.UUID,
	propertyName string, property *models.SingleRef, repo updateRepo, classSchema schema.Schema,
	schemaManager schemaManager) error {

	// get action to see if it exists
	action, err := m.getActionFromRepo(ctx, id, repo)
	if err != nil {
		return err
	}

	err = m.validateReference(ctx, property, repo)
	if err != nil {
		return err
	}

	err = m.validateCanAddReference(kind.ACTION_KIND, action.Class, propertyName, classSchema)
	if err != nil {
		return err
	}

	extended, err := m.extendClassPropsWithReference(action.Schema, propertyName, property)
	if err != nil {
		return err
	}
	action.Schema = extended
	action.LastUpdateTimeUnix = unixNow()

	// the new ref could be a network ref
	err = m.addNetworkDataTypesForAction(ctx, schemaManager, action)
	if err != nil {
		return newErrInternal("could not update schema for network refs: %v", err)
	}

	repo.UpdateAction(ctx, action, action.ID)
	if err != nil {
		return newErrInternal("could not store action: %v", err)
	}

	return nil
}

// AddThingReference Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddThingReference(ctx context.Context, id strfmt.UUID,
	propertyName string, property *models.SingleRef) error {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	classSchema := schemaLock.GetSchema()
	schemaManager := schemaLock.SchemaManager()
	dbConnector := schemaLock.Connector()

	return m.addThingReferenceToConnectorAndSchema(ctx, id, propertyName, property,
		dbConnector, classSchema, schemaManager)
}

func (m *Manager) addThingReferenceToConnectorAndSchema(ctx context.Context, id strfmt.UUID,
	propertyName string, property *models.SingleRef, repo updateRepo, classSchema schema.Schema,
	schemaManager schemaManager) error {

	// get thing to see if it exists
	thing, err := m.getThingFromRepo(ctx, id, repo)
	if err != nil {
		return err
	}

	err = m.validateReference(ctx, property, repo)
	if err != nil {
		return err
	}

	err = m.validateCanAddReference(kind.THING_KIND, thing.Class, propertyName, classSchema)
	if err != nil {
		return err
	}

	extended, err := m.extendClassPropsWithReference(thing.Schema, propertyName, property)
	if err != nil {
		return err
	}
	thing.Schema = extended
	thing.LastUpdateTimeUnix = unixNow()

	// the new ref could be a network ref
	err = m.addNetworkDataTypesForThing(ctx, schemaManager, thing)
	if err != nil {
		return newErrInternal("could not update schema for network refs: %v", err)
	}

	repo.UpdateThing(ctx, thing, thing.ID)
	if err != nil {
		return newErrInternal("could not store thing: %v", err)
	}

	return nil
}

func (m *Manager) validateReference(ctx context.Context, reference *models.SingleRef, repo getRepo) error {
	err := validation.ValidateSingleRef(ctx, m.config, reference, repo, m.network, "reference not found")
	if err != nil {
		return newErrInvalidUserInput("invalid reference: %v", err)
	}

	return nil
}

func (m *Manager) validateCanAddReference(k kind.Kind, className string,
	propertyName string, classSchema schema.Schema) error {
	// TODO: Use checks with error handling instead of panicking
	class := schema.AssertValidClassName(className)
	propName := schema.AssertValidPropertyName(propertyName)
	err, prop := classSchema.GetProperty(k, class, propName)
	if err != nil {
		return newErrInvalidUserInput("Could not find property '%s': %v", propertyName, err)
	}

	propertyDataType, err := classSchema.FindPropertyDataType(prop.DataType)
	if err != nil {
		return newErrInternal("Could not find datatype of property '%s': %v", propertyName, err)
	}

	if propertyDataType.IsPrimitive() {
		return newErrInvalidUserInput("property '%s' is a primitive datatype, not a reference-type", propertyName)
	}

	if prop.Cardinality == nil || *prop.Cardinality != "many" {
		return newErrInvalidUserInput("Property '%s' has a cardinality of atMostOne", propertyName)
	}

	return nil
}

func (m *Manager) extendClassPropsWithReference(props interface{}, propertyName string,
	property *models.SingleRef) (interface{}, error) {

	if props == nil {
		props = map[string]interface{}{}
	}

	propsMap := props.(map[string]interface{})

	_, ok := propsMap[propertyName]
	if !ok {
		propsMap[propertyName] = []interface{}{}
	}

	existingRefs := propsMap[propertyName]
	existingRefsSlice, ok := existingRefs.([]interface{})
	if !ok {
		return nil, newErrInternal("expected list for reference props, but got %T", existingRefs)
	}

	propsMap[propertyName] = append(existingRefsSlice, property)
	return propsMap, nil
}
