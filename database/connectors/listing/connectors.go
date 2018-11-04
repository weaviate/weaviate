package listing

import (
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/foobar"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph"
)

// Build a new connector based on it's name. Returns nil if the connector is unknown.
func NewConnector(name string, config interface{}) (err error, conn dbconnector.DatabaseConnector) {
	switch name {
	case "janusgraph":
		err, conn = janusgraph.New(config)
	case "foobar":
		err, conn = foobar.New(config)
	default:
		err := fmt.Errorf("No connector with the name '%s' exists!", name)
	}
}

// GetAllCacheConnectors contains all available cache-connectors
func GetAllCacheConnectors() []dbconnector.CacheConnector {
	// Set all existing connectors
	connectors := []dbconnector.CacheConnector{}

	return connectors
}
