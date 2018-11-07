package graphqlapi

import (
	"github.com/graphql-go/graphql"
)

type IR interface {
}

type ir struct {
}

type IRHook func(generatedIR IR)

func (g *graphQL) convertGetToIR(graphql.ResolveParams) (IR, error) {
	panic("IR")
	return "whoot", nil
}
