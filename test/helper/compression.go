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
	"testing"
	"time"
)

func EnablePQ(t *testing.T, className string, pq map[string]interface{}) {
	class := GetClass(t, className)
	cfg := class.VectorIndexConfig.(map[string]interface{})
	cfg["pq"] = pq
	class.VectorIndexConfig = cfg
	UpdateClass(t, class)
	// Time for compression to complete
	time.Sleep(2 * time.Second)
}
