package rank

import (
	"github.com/tailor-inc/graphql/language/ast"
)

func (p *CrossRankerProvider) parseCrossRankerArguments(args []*ast.Argument) *Params {
	out := &Params{}

	for _, arg := range args {
		switch arg.Name.Value {
		case "query":
			out.Query = &arg.Value.(*ast.StringValue).Value
		case "property":
			out.Property = &arg.Value.(*ast.StringValue).Value
		}
	}

	return out
}
