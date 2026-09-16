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

package schema

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// What the schema use case is handed at construction: the parsers and validators
// that turn user input into a class, the module config it defers to, and the view
// of the cluster it needs. See NewManager and NewHandler for the wiring.

type VectorConfigParser func(in interface{}, vectorIndexType string, isMultiVector bool) (schemaConfig.VectorIndexConfig, error)

type InvertedConfigValidator func(in *models.InvertedIndexConfig) error

type VectorizerValidator interface {
	ValidateVectorizer(moduleName string) error
}

type ModuleConfig interface {
	SetClassDefaults(class *models.Class)
	SetSinglePropertyDefaults(class *models.Class, props ...*models.Property)
	ValidateClass(ctx context.Context, class *models.Class) error
	GetByName(name string) modulecapabilities.Module
	IsGenerative(string) bool
	IsReranker(string) bool
	IsMultiVector(string) bool
}

// clusterState is the node view the schema use case needs: who the members are,
// plus the two switches that let an operator opt out of schema syncing.
type clusterState interface {
	cluster.NodeSelector
	cluster.MemberLister

	SchemaSyncIgnored() bool
	SkipSchemaRepair() bool
}
