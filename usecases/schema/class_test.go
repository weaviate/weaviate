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
// With class-related transactions refactored into class.go, we need to re-implement the following test cases:
// - Get schema (previously located in get_test.go)
// - Validation (previously located in validation_test.go)
// - Adding new classes (previously in add_test.go)
// - Updating existing classes (previously in update_test.go)
// - Restore classes once Restore is implemented (previously in restore_test.go)
// Please consult the previous implementation's test files for reference.
/// TODO-RAFT END
