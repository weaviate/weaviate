package roaringset

import "github.com/dgraph-io/sroar"

func NewBitmap(values ...uint64) *sroar.Bitmap {
	bm := sroar.NewBitmap()
	bm.SetMany(values)
	return bm
}
