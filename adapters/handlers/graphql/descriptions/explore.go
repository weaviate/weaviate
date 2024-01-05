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

// Package descriptions provides the descriptions as used by the graphql endpoint for Weaviate
package descriptions

const (
	LocalExplore         = "Explore Concepts on a local weaviate with vector-aided search"
	LocalExploreConcepts = "Explore Concepts on a local weaviate with vector-aided serach through keyword-based search terms"
	VectorMovement       = "Move your search term closer to or further away from another vector described by keywords"
	Keywords             = "Keywords are a list of search terms. Array type, e.g. [\"keyword 1\", \"keyword 2\"]"
	Network              = "Set to true, if the exploration should include remote peers"
	Limit                = "Limit the results set (usually fewer results mean faster queries)"
	Offset               = "Offset of the results set (usually fewer results mean faster queries)"
	Certainty            = "Normalized Distance between the result item and the search vector. Normalized to be between 0 (identical vectors) and 1 (perfect opposite)."
	Distance             = "The required degree of similarity between an object's characteristics and the provided filter values"
	Vector               = "Target vector to be used in kNN search"
	Force                = "The force to apply for a particular movements. Must be between 0 and 1 where 0 is equivalent to no movement and 1 is equivalent to largest movement possible"
	ClassName            = "Name of the Class"
	ID                   = "Concept identifier in the uuid format"
	Beacon               = "Concept identifier in the beacon format, such as weaviate://<hostname>/<kind>/id"
)
