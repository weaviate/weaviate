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

package helpers

import "fmt"

var (
	ObjectsBucket []byte = []byte("objects")
	IndexIDBucket []byte = []byte("index_ids")
)

func BucketFromPropName(propName string) []byte {
	return []byte(fmt.Sprintf("property_%s", propName))
}
