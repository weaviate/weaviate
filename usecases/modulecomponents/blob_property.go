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

package modulecomponents

import (
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

// IsBlobProperty says whether the property is a blob, whose content a
// decisions module sends as an image. An unknown property is text.
func IsBlobProperty(cfg moduletools.ClassConfig, property string) bool {
	if cfg == nil || property == "" {
		return false
	}
	return cfg.PropertiesDataTypes()[property] == schema.DataTypeBlob
}
