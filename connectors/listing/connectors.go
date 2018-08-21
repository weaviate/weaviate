package listing

import (
	dbconnector "github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/cassandra"
	"github.com/creativesoftwarefdn/weaviate/connectors/dataloader"
	"github.com/creativesoftwarefdn/weaviate/connectors/foobar"
	"github.com/creativesoftwarefdn/weaviate/connectors/janusgraph"
	"github.com/creativesoftwarefdn/weaviate/connectors/kvcache"
)

// GetAllConnectors contains all available connectors
func GetAllConnectors() []dbconnector.DatabaseConnector {
	// Set all existing connectors
	connectors := []dbconnector.DatabaseConnector{
		&foobar.Foobar{},
		&cassandra.Cassandra{},
		&janusgraph.Janusgraph{},
	}

	return connectors
}

// GetAllCacheConnectors contains all available cache-connectors
func GetAllCacheConnectors() []dbconnector.CacheConnector {
	// Set all existing connectors
	connectors := []dbconnector.CacheConnector{
		&kvcache.KVCache{},
		&dataloader.DataLoader{},
	}

	return connectors
}
