package state

import (
	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/network"
)

// State is the only source of appliaction-wide state
// NOTE: This is not true yet, se gh-xxx
type State struct {
	Database     database.Database
	Network      network.Network
	Messaging    *messages.Messaging
	ServerConfig *config.WeaviateConfig
}
