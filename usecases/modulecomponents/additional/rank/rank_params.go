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

package rank

type Params struct {
	Property *string
	Query    *string
}

func (n Params) GetQuery() string {
	if n.Query != nil {
		return *n.Query
	}
	return ""
}

func (n Params) GetProperty() string {
	if n.Property != nil {
		return *n.Property
	}
	return ""
}
