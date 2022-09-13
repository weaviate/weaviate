//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modulecapabilities

import (
	"context"
	"net/http"

	"github.com/semi-technologies/weaviate/entities/moduletools"
)

type ModuleType string

const (
	Backup        ModuleType = "Backup"
	Extension     ModuleType = "Extension"
	Img2Vec       ModuleType = "Img2Vec"
	Multi2Vec     ModuleType = "Multi2Vec"
	Ref2Vec       ModuleType = "Ref2Vec"
	Text2MultiVec ModuleType = "Text2MultiVec"
	Text2Text     ModuleType = "Text2Text"
	Text2Vec      ModuleType = "Text2Vec"
)

type Module interface {
	Name() string
	Init(ctx context.Context, params moduletools.ModuleInitParams) error
	RootHandler() http.Handler // TODO: remove from overall module, this is a capability
	Type() ModuleType
}

type ModuleExtension interface {
	Module
	InitExtension(modules []Module) error
}

type ModuleDependency interface {
	Module
	InitDependency(modules []Module) error
}

type Dependency interface {
	Argument() string
	GraphQLArgument() GraphQLArgument
	VectorSearch() VectorForParams
}

type ModuleHasAltNames interface {
	AltNames() []string
}
