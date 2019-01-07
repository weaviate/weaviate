/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package get_meta provides the local get meta graphql endpoint for Weaviate
package get_meta

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
)

type Resolver interface {
	LocalGetMeta(info *LocalGetMetaParams) (interface{}, error)
}

type LocalGetMetaParams struct {
	Kind             kind.Kind
	Filters          *common_filters.LocalFilter
	ClassName        string
	Properties       []MetaProperty
	includeMetaCount bool
}

type MetaProperty struct {
	Name string
	Type string
}
