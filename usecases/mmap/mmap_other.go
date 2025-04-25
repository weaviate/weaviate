//go:build !(darwin || linux)

package mmap

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

type MMap = mmap.MMap

func MapRegion(f *os.File, length int, prot, flags int, offset int64) (MMap, error) {
	return mmap.MapRegion(f, length, prot, flags, offset)
}
