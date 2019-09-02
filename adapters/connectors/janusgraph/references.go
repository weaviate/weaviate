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

package janusgraph

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

// AddBatchReferences in bulk.
func (j *Janusgraph) AddBatchReferences(ctx context.Context, refs kinds.BatchReferences) error {
	return j.addBatchReferences(ctx, refs)
}

func (j *Janusgraph) addBatchReferences(ctx context.Context, references kinds.BatchReferences) error {
	chunkSize := MaximumBatchItemsPerQuery
	chunks := len(references) / chunkSize
	if len(references) < chunkSize {
		chunks = 1
	}
	chunked := make([][]kinds.BatchReference, chunks)
	chunk := 0

	for i := 0; i < len(references); i++ {
		if i%chunkSize == 0 {
			if i != 0 {
				chunk++
			}

			currentChunkSize := chunkSize
			if len(references)-i < chunkSize {
				currentChunkSize = len(references) - i
			}
			chunked[chunk] = make([]kinds.BatchReference, currentChunkSize)
		}
		chunked[chunk][i%chunkSize] = references[i]
	}

	for _, chunk := range chunked {
		err := j.addBatchReferencesChunk(ctx, chunk)
		if err != nil {
			return err
		}

	}

	return nil
}

func (j *Janusgraph) addBatchReferencesChunk(ctx context.Context, references kinds.BatchReferences) error {
	q := gremlin.New().Raw("g")

	for _, ref := range references {
		if ref.Err != nil {
			// an error that happened prior to this point int time could have been a
			// validation error. We simply skip over it right now, as it has
			// already errored. The reason it is still included in this list is so
			// that the result list matches the incoming list exactly in order and
			// length, so the user can easily deduce which individual class could
			// be imported and which failed.
			continue
		}
		q = q.Raw("\n")
		updatedQ, err := j.addOneBatchedRef(q, ref)
		if err != nil {
			return err
		}
		q = updatedQ
	}

	if q.String() == "g" {
		// it seems we didn't get a single valid item, our query is still the same
		// as before. Let's return. The API package is aware of all prior errors
		// and can send them to the user correctly.
		return nil
	}

	_, err := j.client.Execute(ctx, q)
	if err != nil {
		return err
	}

	return nil
}

func (j *Janusgraph) addOneBatchedRef(q *gremlin.Query, ref kinds.BatchReference) (*gremlin.Query, error) {
	className := schema.AssertValidClassName(string(ref.From.Class))
	propertyName := schema.AssertValidPropertyName(string(ref.From.Property))
	mappedPropertyName, err := j.state.GetMappedPropertyName(className, propertyName)
	if err != nil {
		return nil, err
	}

	if !ref.From.Local {
		return nil, fmt.Errorf("source in ref is not local to this peer, but to %s", ref.From.PeerName)
	}

	if !ref.To.Local {
		return nil, fmt.Errorf("network refs can't be batch imported")
	}

	return q.AddE(string(mappedPropertyName)).
		FromQuery(gremlin.G.V().HasString(PROP_UUID, string(ref.From.TargetID))).
		ToQuery(gremlin.G.V().HasString(PROP_UUID, string(ref.To.TargetID))).
		StringProperty(PROP_REF_ID, string(mappedPropertyName)).
		StringProperty(PROP_REF_EDGE_CREF, string(ref.To.TargetID)).
		StringProperty(PROP_REF_EDGE_TYPE, ref.To.Kind.Name()).
		StringProperty(PROP_REF_EDGE_LOCATION, ref.To.PeerName), nil
}
