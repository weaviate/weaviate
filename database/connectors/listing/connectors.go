package listing

import (
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/foobar"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph"
)

// Build a new connector based on it's name. Returns nil if the connector is unknown.
func NewConnector(name string) dbconnector.DatabaseConnector {
	switch name {
	case "janusgraph":
		return janusgraph.New()
	case "foobar":
		return foobar.New()
	}

	return nil
}

// GetAllCacheConnectors contains all available cache-connectors
func GetAllCacheConnectors() []dbconnector.CacheConnector {
	// Set all existing connectors
	connectors := []dbconnector.CacheConnector{}

	return connectors
}
