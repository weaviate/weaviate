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

// Package modelsext provides extension methods to the structures in the models package. As this package is
// generated, we cannot put any functionality or helper methods straight into it.
//
// As the models package is used very widely throughout the repository, extensions package
// should not import anything outside the models package as well to avoid any circular dependencies.
//
// To keep things tidy, use these conventions:
// 1. Helpers for the struct in models/X.go should be in modelsext/X.go .
// 2. Functions should be named <struct name><aspirational method name>.
package modelsext
