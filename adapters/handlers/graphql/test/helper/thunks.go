//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
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
