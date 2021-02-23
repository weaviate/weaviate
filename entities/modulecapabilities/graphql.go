//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modulecapabilities

import (
	"github.com/graphql-go/graphql"
)

// ExtractFn extracts graphql params to given struct implementation
type ExtractFn = func(param map[string]interface{}) interface{}

// GraphQLArguments defines the capabilities of modules to add their
// arguments to graphql API
type GraphQLArguments interface {
	GetArguments(classname string) map[string]*graphql.ArgumentConfig
	ExploreArguments() map[string]*graphql.ArgumentConfig
	ExtractFunctions() map[string]ExtractFn
}
