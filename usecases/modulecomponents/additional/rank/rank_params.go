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

package rank

import "github.com/weaviate/weaviate/usecases/modulecomponents/ent"

type Params struct {
	Property *string
	Query    *string
	// Decision is set when a decision model reranks, see Decision.
	Decision *Decision
	// Provider is the decisions module the request names; nil is the
	// class's. Set with Decision only.
	Provider *ent.DecisionsProvider
}

func (n Params) GetQuery() string {
	if n.Query != nil {
		return *n.Query
	}
	return ""
}

func (n Params) GetProperty() string {
	if n.Property != nil {
		return *n.Property
	}
	return ""
}

func (n Params) GetPropertiesToExtract() []string {
	property := n.GetProperty()
	if property != "" {
		return []string{property}
	}
	return []string{}
}
