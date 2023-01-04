//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package helpers

import "github.com/dgraph-io/sroar"

// AllowList groups a list of possible indexIDs to be passed to a secondary
// index. The secondary index must make sure that it only returns result
// present on the AllowList
// type AllowList map[uint64]struct{}

type RoaringAllowList struct {
	*sroar.Bitmap
}

func NewRoaringAllowList() *RoaringAllowList {
	return &RoaringAllowList{sroar.NewBitmap()}
}

// // Inserting and reading is not thread-safe. However, if inserting has
// // completed, and the list can be considered read-only, it is safe to read from
// // it concurrently
func (al RoaringAllowList) Insert(id uint64) {
	// no need to check if it's already present, simply overwrite
	al.Set(id)
}

func (al RoaringAllowList) InsertAll(ids []uint64) {
	// no need to check if it's already present, simply overwrite
	al.SetMany(ids)
}

// // Contains is not thread-safe if the list is still being filled. However, if
// // you can guarantee that the list is no longer being inserted into and it
// // effectively becomes read-only, you can safely read concurrently
func (al RoaringAllowList) Contains(id uint64) bool {
	return al.Contains(id)
}

func (al RoaringAllowList) DeepCopy() RoaringAllowList {
	return RoaringAllowList{al.Clone()}
}

func (al RoaringAllowList) Slice() []uint64 {
	return al.Slice()
}

func (al RoaringAllowList) Size() int {
	return al.GetCardinality()
}
