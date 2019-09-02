//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"fmt"
	"regexp"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

func (t *Traverser) GetClass(ctx context.Context, principal *models.Principal,
	params *GetParams) (interface{}, error) {
	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
	}
	defer unlock()

	if params.Explore != nil || t.config.Config.EsvectorOnly {
		// if Explore is set this request can no longer be served by the connector
		// alone, instead it must be served by a (vector) explorer which can in
		// turn make use of the connector
		return t.explorer.GetClass(ctx, params)
	}

	return t.repo.GetClass(ctx, params)
}

type GetParams struct {
	Kind       kind.Kind
	Filters    *filters.LocalFilter
	ClassName  string
	Pagination *filters.Pagination
	Properties []SelectProperty
	Explore    *ExploreParams
}

type SelectProperty struct {
	Name string

	IsPrimitive bool

	// Include the __typename in all the Refs below.
	IncludeTypeName bool

	// Not a primitive type? Then select these properties.
	Refs []SelectClass
}

type SelectClass struct {
	ClassName     string
	RefProperties []SelectProperty
}

// FindSelectClass by specifying the exact class name
func (sp SelectProperty) FindSelectClass(className schema.ClassName) *SelectClass {
	for _, selectClass := range sp.Refs {
		if selectClass.ClassName == string(className) {
			return &selectClass
		}
	}

	return nil
}

// HasPeer returns true if any of the referenced classes are from the specified
// peer
func (sp SelectProperty) HasPeer(peerName string) bool {
	r := regexp.MustCompile(fmt.Sprintf("^%s__", peerName))
	for _, selectClass := range sp.Refs {
		if r.MatchString(selectClass.ClassName) {
			return true
		}
	}

	return false
}
