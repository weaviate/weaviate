//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package dependency

import "github.com/semi-technologies/weaviate/entities/modulecapabilities"

type NearTextDependecy struct {
	argument modulecapabilities.GraphQLArgument
	searcher modulecapabilities.VectorForParams
}

func New(argument modulecapabilities.GraphQLArgument,
	searcher modulecapabilities.VectorForParams,
) *NearTextDependecy {
	return &NearTextDependecy{argument, searcher}
}

func (d *NearTextDependecy) Argument() string {
	return "nearText"
}

func (d *NearTextDependecy) GraphQLArgument() modulecapabilities.GraphQLArgument {
	return d.argument
}

func (d *NearTextDependecy) VectorSearch() modulecapabilities.VectorForParams {
	return d.searcher
}
