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

package scaler

// difference returns elements in xs which doesn't exists in ys
func difference(xs, ys []string) []string {
	m := make(map[string]struct{}, len(ys))
	for _, y := range ys {
		m[y] = struct{}{}
	}
	rs := make([]string, 0, len(ys))
	for _, x := range xs {
		if _, ok := m[x]; !ok {
			rs = append(rs, x)
		}
	}
	return rs
}
