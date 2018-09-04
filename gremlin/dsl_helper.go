package gremlin

import (
	"fmt"
	"strings"
)

// Escape a string so that it can be used without risk of SQL-injection like escapes.
// TODO figure out other ways of doing string interpolation in Groovy and escape them.
func escapeString(str string) string {
	s := strings.Replace(str, `"`, `\"`, -1)
	s = strings.Replace(s, `$`, `\$`, -1)
	return s
}

func extend_query(query *Query, format string, vals ...interface{}) *Query {
	r := Query{query: query.query + fmt.Sprintf(format, vals...)}
	return &r
}
