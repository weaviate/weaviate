package gremlin

import "fmt"

// EqInt returns a testing predicate such as eq(<int>), e.g. eq(1000)
func EqInt(value int) *Query {
	return &Query{query: fmt.Sprintf(`eq(%d)`, value)}
}

// NeqInt returns a testing predicate such as neq(<int>), e.g. neq(1000)
func NeqInt(value int) *Query {
	return &Query{query: fmt.Sprintf(`neq(%d)`, value)}
}

// LtInt returns a testing predicate such as lt(<int>), e.g. lt(1000)
func LtInt(value int) *Query {
	return &Query{query: fmt.Sprintf(`lt(%d)`, value)}
}

// LteInt returns a testing predicate such as lte(<int>), e.g. lte(1000)
func LteInt(value int) *Query {
	return &Query{query: fmt.Sprintf(`lte(%d)`, value)}
}

// GtInt returns a testing predicate such as gt(<int>), e.g. gt(1000)
func GtInt(value int) *Query {
	return &Query{query: fmt.Sprintf(`gt(%d)`, value)}
}

// GteInt returns a testing predicate such as gte(<int>), e.g. gte(1000)
func GteInt(value int) *Query {
	return &Query{query: fmt.Sprintf(`gte(%d)`, value)}
}
