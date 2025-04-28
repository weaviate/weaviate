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

// The Flat index rescores against a dedicated vector store when PQ is used.
// This leads to a 2x write amplification of all vectors because they are
// already stored in the object store.
//
// Our hypothesis is that there is no benefit from this dedicated store and we
// could save a lot of space if we were to rescore against the object store.
// This might even have benefits due to (page) cache locality.
//
// The point of this temporary flag is to prove our hypothesis right or wrong.
type FlatIndexRescoreAgainstObjectStore bool

const (
	// This is a new experimental flag. IF proven valid in practice it is meant
	// to become the new default
	RescoreAgainstObjectStore FlatIndexRescoreAgainstObjectStore = true

	// This is the default setting as of April '25. As part of this experiment we are considering changing this.
	RescoreAgainstDedicatedIndex FlatIndexRescoreAgainstObjectStore = false

	FlatIndexRescoreAgainstObjectStoreEnvVariable = "FLAT_INDEX_RESCORE_AGAINST_OBJECT_STORE"
	FlatIndexRescoreAgainstObjectStoreLDKey       = "flat-index-rescore-against-object-store"
)
