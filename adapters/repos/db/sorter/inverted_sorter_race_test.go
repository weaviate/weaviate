//go:build race
// +build race

package sorter

import "testing"

func TestInvertedSorter(t *testing.T) {
	race := true
	testInvertedSorter(t, race)
}

func TestInvertedSorterMultiOrder(t *testing.T) {
	race := true
	testInvertedSorterMultiOrder(t, race)
}
