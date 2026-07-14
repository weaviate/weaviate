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

package search

import (
	"fmt"
	"testing"
)

func makeBenchProps(n int) SelectProperties {
	props := make(SelectProperties, n)
	for i := range props {
		props[i] = SelectProperty{
			Name:        fmt.Sprintf("prop_%d", i),
			IsPrimitive: true,
		}
	}
	return props
}

func BenchmarkFindProperty(b *testing.B) {
	for _, size := range []int{5, 20, 50} {
		props := makeBenchProps(size)
		first := props[0].Name
		last := props[size-1].Name

		b.Run(fmt.Sprintf("size_%d/hit_first", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if props.FindProperty(first) == nil {
					b.Fatal("expected hit")
				}
			}
		})

		b.Run(fmt.Sprintf("size_%d/hit_last", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if props.FindProperty(last) == nil {
					b.Fatal("expected hit")
				}
			}
		})

		b.Run(fmt.Sprintf("size_%d/miss", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if props.FindProperty("does_not_exist") != nil {
					b.Fatal("expected miss")
				}
			}
		})
	}
}

// BenchmarkResolvePattern simulates the refcache read path: for every result
// object, every schema key is looked up in the same SelectProperties
// instance.
func BenchmarkResolvePattern(b *testing.B) {
	const objects = 100
	for _, size := range []int{5, 20, 50} {
		props := makeBenchProps(size)
		keys := make([]string, size)
		for i := range keys {
			keys[i] = props[i].Name
		}

		b.Run(fmt.Sprintf("size_%d/find_property", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for o := 0; o < objects; o++ {
					for _, key := range keys {
						if props.FindProperty(key) == nil {
							b.Fatal("expected hit")
						}
					}
				}
			}
		})

		b.Run(fmt.Sprintf("size_%d/indexed", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				idx := props.Indexed()
				for o := 0; o < objects; o++ {
					for _, key := range keys {
						if idx.Find(key) == nil {
							b.Fatal("expected hit")
						}
					}
				}
			}
		})
	}
}
