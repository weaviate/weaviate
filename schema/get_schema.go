package schema

import "github.com/creativesoftwarefdn/weaviate/database/schema"

// GetSchema returns a local cached version of the current schema
func (m *Manager) GetSchema() (schema.Schema, error) {
	connectorLock, err := m.db.ConnectorLock()
	if err != nil {
		return schema.Schema{}, err
	}

	defer connectorLock.Unlock()

	return connectorLock.GetSchema(), nil
}
