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

package rbac

type Level string

func CheckLevel(sp *string) bool {
	if sp == nil {
		return false
	}
	return *sp == "database" || *sp == "collection"
}

type Policy struct {
	Name     string
	Resource string
	Verb     string
	Level    string
}
