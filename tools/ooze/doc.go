//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package oozemutation hosts the reusable ooze (https://github.com/gtramontina/ooze)
// mutation-testing harness for this repository.
//
// The harness itself lives in mutation_test.go behind the `mutation` build tag,
// so it never affects normal builds, `go test ./...`, or linting. This file
// exists only so the package compiles when the `mutation` tag is absent.
//
// Drive it through the wrapper script, passing the package directory to mutate:
//
//	tools/ooze_mutation.sh usecases/byteops
//
// See mutation_test.go for the full set of supported environment variables.
package oozemutation
