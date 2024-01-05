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

package helper

import (
	"math/rand"
	"time"
)

// GetRandomString returns a string comprised of random
// samplings of charset, of length specified by caller
func GetRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	s := make([]byte, length)
	for n := range s {
		s[n] = charset[seededRand.Intn(len(charset))]
	}

	return string(s)
}
