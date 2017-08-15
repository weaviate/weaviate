package internal_test

import (
	"reflect"
	"testing"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/internal"
)

func TestBasicStackFunctionality(t *testing.T) {
	data := []float64{1, 1, 1, 0, 0, 0, 2, 2, 2, 6, 6, 6}
	stack := internal.NewCoordStack(geom.XYM)

	size := stack.Size()
	if size != 0 {
		t.Fatalf("Expected 0 elements in the stack but was %v", size)
	}
	verifyPush(t, stack, data, 0)

	size = stack.Size()
	if size != 1 {
		t.Fatalf("Expected 1 elements in the stack but was %v", size)
	}

	verifyPush(t, stack, data, 0)
	verifyPush(t, stack, data, 3)
	verifyPush(t, stack, data, 9)
	verifyPush(t, stack, data, 6)

	size = stack.Size()
	if size != 5 {
		t.Fatalf("Expected 5 elements in the stack but was %v", size)
	}
	verifyPeek(t, stack, []float64{2, 2, 2})
	verifyPop(t, stack, 4, []float64{2, 2, 2})
	verifyPeek(t, stack, []float64{6, 6, 6})
	verifyPop(t, stack, 3, []float64{6, 6, 6})
	verifyPeek(t, stack, []float64{0, 0, 0})
	verifyPop(t, stack, 2, []float64{0, 0, 0})
	verifyPeek(t, stack, []float64{1, 1, 1})
	verifyPop(t, stack, 1, []float64{1, 1, 1})
	verifyPeek(t, stack, []float64{1, 1, 1})
	verifyPop(t, stack, 0, []float64{1, 1, 1})
}

func verifyPush(t *testing.T, stack *internal.CoordStack, toPush []float64, i int) {
	c := stack.Push(toPush, i)

	if !reflect.DeepEqual(c, toPush[i:i+3]) {
		t.Fatalf("stack.Peek() failed, expected %v but was %v", toPush[i:i+3], c)
	}
}
func verifyPeek(t *testing.T, stack *internal.CoordStack, expectedCoord []float64) {
	c := stack.Peek()

	if !reflect.DeepEqual(c, expectedCoord) {
		t.Fatalf("stack.Peek() failed, expected %v but was %v", expectedCoord, c)
	}
}
func verifyPop(t *testing.T, stack *internal.CoordStack, expectedSize int, expectedCoord []float64) {

	c, size := stack.Pop()

	if size != expectedSize {
		t.Fatalf("Expected %v elements in the stack but was %v", expectedSize, size)
	}

	if !reflect.DeepEqual(c, expectedCoord) {
		t.Fatalf("stack.Pop() failed, expected %v but was %v", expectedCoord, c)
	}
}
