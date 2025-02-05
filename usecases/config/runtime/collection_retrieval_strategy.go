//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package runtime

type CollectionRetrievalStrategy string

const (
	LeaderOnly       CollectionRetrievalStrategy = "LeaderOnly"
	LocalOnly        CollectionRetrievalStrategy = "LocalOnly"
	LeaderOnMismatch CollectionRetrievalStrategy = "LeaderOnMismatch"

	CollectionRetrievalStrategyEnvVariable = "COLLECTION_RETRIEVAL_STRATEGY"
	CollectionRetrievalStrategyLDKey       = "collection-retrieval-strategy"
)
