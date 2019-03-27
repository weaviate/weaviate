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
 */package aggregate

import (
	"fmt"

	"github.com/graphql-go/graphql"
)

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Network.Get queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}

func resolveClass(params graphql.ResolveParams) (interface{}, error) {
	// do logging here after resolvers for different aggregators are implemented (RequestsLog.Register() in a go func)
	return nil, fmt.Errorf("resolve network aggregate class field not yet supported")
}
