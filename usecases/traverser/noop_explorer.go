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

	"github.com/semi-technologies/weaviate/entities/search"
)

// NoOpExplorer errors if an explore operation is attempted
type NoOpExplorer struct {
	err error
}

// GetClass errors
func (n *NoOpExplorer) GetClass(ctx context.Context,
	params *GetParams) ([]interface{}, error) {
	return nil, n.err
}

// Concepts errors
func (n *NoOpExplorer) Concepts(ctx context.Context,
	params ExploreParams) ([]search.Result, error) {
	return nil, n.err
}

// NewNoOpExplorer with variable error
func NewNoOpExplorer(err error) *NoOpExplorer {
	return &NoOpExplorer{err: err}
}
