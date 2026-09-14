//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// package objects provides managers for all kind-related items, such as objects.
// Manager provides methods for "regular" interaction, such as
// add, get, delete, update, etc. Additionally BatchManager allows for
// efficient batch-adding of object instances and references.
//
// The ports the managers depend on are declared in schema_ports.go (the schema
// use case), ports.go (the object store, the modules and the clock) and
// metrics.go (the operation counters).
package objects

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// Manager manages kind changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	config            *config.WeaviateConfig
	schemaManager     schemaManager
	logger            logrus.FieldLogger
	authorizer        authorization.Authorizer
	vectorRepo        VectorRepo
	timeSource        timeSource
	modulesProvider   ModulesProvider
	autoSchemaManager *AutoSchemaManager
	metrics           objectsMetrics
	allocChecker      *memwatch.Monitor
}

// NewManager creates a new manager.
func NewManager(schemaManager schemaManager,
	config *config.WeaviateConfig, logger logrus.FieldLogger,
	authorizer authorization.Authorizer, vectorRepo VectorRepo,
	modulesProvider ModulesProvider, metrics objectsMetrics, allocChecker *memwatch.Monitor,
	autoSchemaManager *AutoSchemaManager,
) *Manager {
	if allocChecker == nil {
		allocChecker = memwatch.NewDummyMonitor()
	}

	return &Manager{
		config:            config,
		schemaManager:     schemaManager,
		logger:            logger,
		authorizer:        authorizer,
		vectorRepo:        vectorRepo,
		timeSource:        defaultTimeSource{},
		modulesProvider:   modulesProvider,
		autoSchemaManager: autoSchemaManager,
		metrics:           metrics,
		allocChecker:      allocChecker,
	}
}

// resolveNS qualifies name with the principal's namespace (if enabled)
// and resolves any alias to its underlying class.
func (m *Manager) resolveNS(principal *models.Principal, name string) (class, qualifiedAlias string, err error) {
	return namespacing.Resolve(principal, m.schemaManager, m.config.Config.Namespaces.Enabled, name)
}

func generateUUID() (strfmt.UUID, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("could not generate uuid v4: %w", err)
	}

	return strfmt.UUID(id.String()), nil
}
