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

package versioned

import "github.com/weaviate/weaviate/entities/models"

// Class is a wrapper on top of class created by OpenAPI to be able
// to inject version to class
type Class struct {
	*models.Class
	Version uint64
}

type Classes map[string]Class
