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

//go:build !linux && !darwin

package file

// NameMax returns 255, the limit NTFS and most other filesystems set. NTFS
// counts UTF-16 units, and every staged name is ASCII.
func NameMax(string) (int, error) {
	return 255, nil
}
