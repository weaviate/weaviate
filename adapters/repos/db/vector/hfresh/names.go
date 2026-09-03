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

package hfresh

import "fmt"

// HFreshPostingsBucketName is the LSM bucket holding hfresh's posting lists.
func HFreshPostingsBucketName(indexID string) string {
	return fmt.Sprintf("hfresh_postings_%s", indexID)
}

// HFreshSharedBucketName is the LSM bucket holding hfresh's shared metadata.
func HFreshSharedBucketName(indexID string) string {
	return fmt.Sprintf("hfresh_shared_%s", indexID)
}
