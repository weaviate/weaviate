//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package janusgraph

import (
	"context"
	"fmt"
	"runtime/debug"

	jget "github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/get"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type resolveResult struct {
	results []interface{}
	err     error
}

// LocalGetClass Implements the Local->Get->KIND->CLASS lookup.
func (j *Janusgraph) LocalGetClass(ctx context.Context, params *traverser.LocalGetParams) (interface{}, error) {
	ch := make(chan resolveResult, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// send error over the channel
				ch <- resolveResult{err: fmt.Errorf("Janusgraph.LocalGetClass paniced: %#v\n%s", r, string(debug.Stack()))}
			}
			close(ch)
		}()

		results, err := j.doLocalGetClass(ctx, params)

		if err != nil {
			ch <- resolveResult{err: fmt.Errorf("Janusgraph.LocalGetClass: %#v", err)}
		} else {
			ch <- resolveResult{results: results}
		}
	}()

	result := <-ch
	if result.err != nil {
		return nil, result.err
	}
	return result.results, nil
}

func (j *Janusgraph) doLocalGetClass(ctx context.Context, params *traverser.LocalGetParams) ([]interface{}, error) {
	q, err := jget.NewQuery(*params, &j.state, &j.schema, j.appConfig.QueryDefaults).String()
	if err != nil {
		return nil, fmt.Errorf("could not build query: %s", err)
	}

	return jget.NewProcessor(j.client, &j.state, schema.ClassName(params.ClassName)).
		Process(ctx, gremlin.New().Raw(q))
}
