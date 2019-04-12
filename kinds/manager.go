package kinds

import (
	"fmt"
	"log"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/network"
	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
)

// Manager Manages schema changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	db      db
	network network.Network
	config  *config.WeaviateConfig
}

type db interface {
	// TODO: Remove dependency to database package, this is a violation of clean
	// arch principles
	SchemaLock() (database.SchemaLock, error)
	ConnectorLock() (database.ConnectorLock, error)
}

// NewManager creates a new manager
func NewManager(db db, network network.Network, config *config.WeaviateConfig) *Manager {
	return &Manager{
		db:      db,
		network: network,
		config:  config,
	}
}

type unlocker interface {
	Unlock() error
}

func unlock(l unlocker) {
	err := l.Unlock()
	if err != nil {
		log.Fatal(err)
	}
}

func generateUUID() strfmt.UUID {
	uuid, err := uuid.NewV4()
	if err != nil {
		panic("PANIC: Can't create UUID")
	}

	return strfmt.UUID(fmt.Sprintf("%v", uuid))
}
