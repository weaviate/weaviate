package gremlin

import (
	"fmt"
	"strconv"
)

// Int

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

// Float

// EqFloat returns a testing predicate such as eq(<float64>), e.g. eq(1000)
func EqFloat(value float64) *Query {
	return &Query{query: fmt.Sprintf(`eq(%f)`, value)}
}

// NeqFloat returns a testing predicate such as neq(<float64>), e.g. neq(1000)
func NeqFloat(value float64) *Query {
	return &Query{query: fmt.Sprintf(`neq(%f)`, value)}
}

// LtFloat returns a testing predicate such as lt(<float64>), e.g. lt(1000)
func LtFloat(value float64) *Query {
	return &Query{query: fmt.Sprintf(`lt(%f)`, value)}
}

// LteFloat returns a testing predicate such as lte(<float64>), e.g. lte(1000)
func LteFloat(value float64) *Query {
	return &Query{query: fmt.Sprintf(`lte(%f)`, value)}
}

// GtFloat returns a testing predicate such as gt(<float64>), e.g. gt(1000)
func GtFloat(value float64) *Query {
	return &Query{query: fmt.Sprintf(`gt(%f)`, value)}
}

// GteFloat returns a testing predicate such as gte(<float64>), e.g. gte(1000)
func GteFloat(value float64) *Query {
	return &Query{query: fmt.Sprintf(`gte(%f)`, value)}
}

// String

// EqString returns a testing predicate such as eq(<string>), e.g. eq(1000)
func EqString(value string) *Query {
	return &Query{query: fmt.Sprintf(`eq("%s")`, value)}
}

// NeqString returns a testing predicate such as neq(<string>), e.g. neq(1000)
func NeqString(value string) *Query {
	return &Query{query: fmt.Sprintf(`neq("%s")`, value)}
}

// Bool

// EqBool returns a testing predicate such as eq(<bool>), e.g. eq(1000)
func EqBool(value bool) *Query {
	return &Query{query: fmt.Sprintf(`eq(%s)`, strconv.FormatBool(value))}
}

// NeqBool returns a testing predicate such as neq(<bool>), e.g. neq(1000)
func NeqBool(value bool) *Query {
	return &Query{query: fmt.Sprintf(`neq(%s)`, strconv.FormatBool(value))}
}
