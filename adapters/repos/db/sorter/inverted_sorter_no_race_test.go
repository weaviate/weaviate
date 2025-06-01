//go:build !race
// +build !race

package sorter

import "testing"

func TestInvertedSorter(t *testing.T) {
	race := false
	testInvertedSorter(t, race)
}

func TestInvertedSorterMultiOrder(t *testing.T) {
	race := false
	testInvertedSorterMultiOrder(t, race)
}
