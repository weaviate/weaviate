//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modulecapabilities

import (
	"context"
	"net/http"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type ModuleType string

const (
	Offload             ModuleType = "Offload"
	Backup              ModuleType = "Backup"
	Extension           ModuleType = "Extension"
	Img2Vec             ModuleType = "Img2Vec"
	Multi2Vec           ModuleType = "Multi2Vec"
	Ref2Vec             ModuleType = "Ref2Vec"
	Text2ManyVec        ModuleType = "Text2ManyVec"
	Text2Multivec       ModuleType = "Text2Multivec"
	Text2TextGenerative ModuleType = "Text2TextGenerative"
	Text2TextSummarize  ModuleType = "Text2TextSummarize"
	Text2TextReranker   ModuleType = "Text2TextReranker"
	Text2TextNER        ModuleType = "Text2TextNER"
	Text2TextQnA        ModuleType = "Text2TextQnA"
	Text2Vec            ModuleType = "Text2Vec"
	Usage               ModuleType = "Usage"
)

type Module interface {
	Name() string
	Init(ctx context.Context, params moduletools.ModuleInitParams) error
	Type() ModuleType
}

// ModuleWithClose is an optional capability interface for modules that need to be closed
type ModuleWithClose interface {
	Module
	Close() error
}

// ModuleWithHTTPHandlers is an optional capability interface for modules that provide HTTP endpoints
type ModuleWithHTTPHandlers interface {
	Module
	RootHandler() http.Handler
}

type ModuleExtension interface {
	Module
	InitExtension(modules []Module) error
}

type ModuleDependency interface {
	Module
	InitDependency(modules []Module) error
}

type Dependency[T dto.Embedding] interface {
	ModuleName() string
	Argument() string
	GraphQLArgument() GraphQLArgument
	VectorSearch() VectorForParams[T]
}

type ModuleHasAltNames interface {
	AltNames() []string
}

// ModuleWithUsageService is an optional capability interface for modules that need a usage service
type ModuleWithUsageService interface {
	Module
	SetUsageService(usageService any) // Using interface{} to avoid circular dependency
}
