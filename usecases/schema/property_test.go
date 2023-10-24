//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

/// TODO-RAFT START
// Fix Unit Tests
// With property-related transactions refactored into property.go, we need to re-implement the following test cases:
// - Adding new property (previously in add_property_test.go)
// - Updating existing property
// - Validation (previously in remove_duplicate_props_test.go)
// Please consult the previous implementation's test files for reference.
/// TODO-RAFT END
