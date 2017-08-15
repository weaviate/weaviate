package lineintersector

import (
	"testing"
)

func TestNonRobustLineIntersectionPointOnLine(t *testing.T) {
	exectuteLineIntersectionPointOnLineTest(t, NonRobustLineIntersector{})
}

func TestNonRobustLineIntersectionLines(t *testing.T) {
	executeLineIntersectionLinesTest(t, NonRobustLineIntersector{})
}
