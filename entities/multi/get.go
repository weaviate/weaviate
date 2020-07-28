package multi

import "github.com/semi-technologies/weaviate/entities/schema/kind"

type Identifier struct {
	Id        string
	Kind      kind.Kind
	ClassName string
}
