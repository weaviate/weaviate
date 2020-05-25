//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/classification"
)

func (db *DB) GetUnclassified(ctx context.Context, kind kind.Kind, class string, properties []string, filter *libfilters.LocalFilter) ([]search.Result, error) {
	panic("not implemented") // TODO: Implement
}

func (db *DB) AggregateNeighbors(ctx context.Context, vector []float32, kind kind.Kind, class string, properties []string, k int, filter *libfilters.LocalFilter) ([]classification.NeighborRef, error) {
	panic("not implemented") // TODO: Implement
}
