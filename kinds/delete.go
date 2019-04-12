package kinds

// // DeleteAction Class Instance from the connected DB
// func (m *Manager) DeleteAction(ctx context.Context, class *models.Action) (*models.Action, error) {
// 	schemaLock, err := m.db.SchemaLock()
// 	if err != nil {
// 		return nil, newErrInternal("could not aquire lock: %v", err)
// 	}
// 	defer unlock(schemaLock)
// 	dbConnector := schemaLock.Connector()

// 	class.ID = generateUUID()

// 	err = m.validateAction(ctx, schemaLock, class)
// 	if err != nil {
// 		return nil, newErrInvalidUserInput("invalid action: %v", err)
// 	}

// 	err = m.addNetworkDataTypesForAction(ctx, schemaLock.SchemaManager(), class)
// 	if err != nil {
// 		return nil, newErrInternal("could not update schema for network refs: %v", err)
// 	}

// 	dbConnector.DeleteAction(ctx, class, class.ID)
// 	if err != nil {
// 		return nil, newErrInternal("could not store action: %v", err)
// 	}

// 	return class, nil
// }

// // DeleteThing Class Instance from the conncected DB
// func (m *Manager) DeleteThing(ctx context.Context, id strfmt.UUID) (*models.Thing, error) {
// 	dbLock, err := db.ConnectorLock()
// 	if err != nil {
// 		return nil, newErrInternal("could not aquire lock: %v", err)
// 	}
// 	delayedLock := delayed_unlock.New(dbLock)
// 	defer unlock(delayedLock)

// 	dbConnector := dbLock.Connector()

// 	dbConnector.DeleteThing(ctx, class, class.ID)
// 	if err != nil {
// 		return nil, newErrInternal("could not store thing: %v", err)
// 	}

// 	return class, nil
// }
