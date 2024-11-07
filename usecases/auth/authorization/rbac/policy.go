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

func CheckDomain(sp *string) bool {
	if sp == nil {
		return false
	}
	return *sp == "cluster" || *sp == "collections" || *sp == "objects" || *sp == "roles" || *sp == "tenants"
}

type Policy struct {
	Name     string
	Resource string
	Verb     string
	Domain   string
}
