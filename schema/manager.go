package schema

import (
	"log"

	"github.com/creativesoftwarefdn/weaviate/database"
)

// Manager Manages schema changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	db db
}

type db interface {
	// TODO: Remove dependency to database package, this is a violation of clean
	// arch principles
	SchemaLock() (database.SchemaLock, error)
	ConnectorLock() (database.ConnectorLock, error)
}

// NewManager creates a new manager
func NewManager(db db) *Manager {
	return &Manager{
		db: db,
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
