//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package helper

func NilThunk() func() interface{} {
	return func() interface{} {
		return nil
	}
}

func IdentityThunk(x interface{}) func() interface{} {
	return func() interface{} {
		return x
	}
}

func EmptyListThunk() func() interface{} {
	return func() interface{} {
		list := []interface{}{}
		return interface{}(list)
	}
}

func EmptyList() interface{} {
	return []interface{}{}
}

func SingletonThunk(x interface{}) func() interface{} {
	return func() interface{} {
		return interface{}(x)
	}
}
