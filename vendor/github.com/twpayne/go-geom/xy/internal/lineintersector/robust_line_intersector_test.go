package lineintersector

import (
	"reflect"
	"runtime/debug"
	"testing"
)

func TestRobustLineIntersectionPointOnLine(t *testing.T) {
	exectuteLineIntersectionPointOnLineTest(t, RobustLineIntersector{})
}

func TestRobustLineIntersectionLines(t *testing.T) {
	executeLineIntersectionLinesTest(t, RobustLineIntersector{})

	// extra tests that have different values for robust intersector than non-robust

	for i, tc := range robustLineOnLineIntersectionData {
		doLineIntersectsLineTest(t, RobustLineIntersector{}, i, tc)
	}
}

func exectuteLineIntersectionPointOnLineTest(t *testing.T, intersectionStrategy Strategy) {
	for i, tc := range pointOnLineIntersectionTestData {
		calculatedResult := PointIntersectsLine(intersectionStrategy, tc.P, tc.LineEnd1, tc.LineEnd2)
		if !reflect.DeepEqual(tc.Result, calculatedResult) {
			t.Errorf("Test '%v' failed: expected \n%v was \n%v", i+1, tc.Result, calculatedResult)
		}
	}
}

func executeLineIntersectionLinesTest(t *testing.T, intersectionStrategy Strategy) {
	for i, tc := range lineOnLineIntersectionTestData {
		doLineIntersectsLineTest(t, intersectionStrategy, i, tc)
	}
}

func doLineIntersectsLineTest(t *testing.T, intersectionStrategy Strategy, i int, tc lineIntersectsLinesTestData) {
	defer func() {
		if err := recover(); err != nil {
			t.Errorf("%T - An error occurred during Test '%v' (%v): %v\n%s", intersectionStrategy, i+1, tc.Desc, err, debug.Stack())
		}
	}()

	calculatedResult := LineIntersectsLine(intersectionStrategy, tc.P1, tc.P2, tc.P3, tc.P4)

	if !reflect.DeepEqual(calculatedResult, tc.Result) {
		t.Errorf("%T - Test '%v' (%v) failed: expected \n%v but was \n%v", intersectionStrategy, i+1, tc.Desc, tc.Result, calculatedResult)
	}
}
