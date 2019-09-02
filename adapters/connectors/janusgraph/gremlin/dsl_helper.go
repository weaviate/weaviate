//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package gremlin

import (
	"fmt"
	"strings"
)

// Escape a string so that it can be used without risk of SQL-injection like escapes.
// TODO gh-614: figure out other ways of doing string interpolation in Groovy and escape them.
func EscapeString(str string) string {
	s := strings.Replace(str, `"`, `\"`, -1)
	s = strings.Replace(s, `$`, `\$`, -1)
	return s
}

func extend_query(query *Query, format string, vals ...interface{}) *Query {
	r := Query{query: query.query + fmt.Sprintf(format, vals...)}
	return &r
}

// leads with dot if q is an existing query, does not lead with dot if q is a
// new query
func smartExtendQuery(query *Query, format string, vals ...interface{}) *Query {
	var r Query
	if query.query == "" {
		r = Query{query: query.query + fmt.Sprintf(format, vals...)}
	} else {
		r = Query{query: query.query + "." + fmt.Sprintf(format, vals...)}
	}
	return &r

}
