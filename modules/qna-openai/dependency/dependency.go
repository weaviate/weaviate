//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package dependency

import "github.com/weaviate/weaviate/entities/modulecapabilities"

type NearTextDependecy struct {
	moduleName string
	argument   modulecapabilities.GraphQLArgument
	searcher   modulecapabilities.VectorForParams
}

func New(moduleName string, argument modulecapabilities.GraphQLArgument,
	searcher modulecapabilities.VectorForParams,
) *NearTextDependecy {
	return &NearTextDependecy{moduleName, argument, searcher}
}

func (d *NearTextDependecy) Argument() string {
	return "nearText"
}

func (d *NearTextDependecy) ModuleName() string {
	return d.moduleName
}

func (d *NearTextDependecy) GraphQLArgument() modulecapabilities.GraphQLArgument {
	return d.argument
}

func (d *NearTextDependecy) VectorSearch() modulecapabilities.VectorForParams {
	return d.searcher
}
