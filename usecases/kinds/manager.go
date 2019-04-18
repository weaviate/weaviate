/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package kinds

import (
	"fmt"
	"log"

	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	"github.com/creativesoftwarefdn/weaviate/usecases/network"
	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
)

// Manager Manages schema changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	network       network.Network
	config        *config.WeaviateConfig
	repo          repo
	locks         locks
	schemaManager schemaManager
}

type repo interface {
	addRepo
	getRepo
	updateRepo
	deleteRepo
}

type locks interface {
	LockConnector() (func() error, error)
	LockSchema() (func() error, error)
}

// NewManager creates a new manager
func NewManager(repo repo, locks locks, schemaManager schemaManager, network network.Network, config *config.WeaviateConfig) *Manager {
	return &Manager{
		network:       network,
		config:        config,
		repo:          repo,
		locks:         locks,
		schemaManager: schemaManager,
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
